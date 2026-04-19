#!/usr/bin/env python3
"""
Scraper de Imóveis do Rio de Janeiro — ZAP Imóveis
===================================================
Arquitetura Produtor-Consumidor com:
- Deduplicação thread-safe
- Retry com backoff exponencial
- Encerramento gracioso via Event
- Geo-enriquecimento vetorizado (NumPy)
- Análise financeira adaptativa (MCMV vs Mercado)
- NLP de descrições para tags qualitativas
- Output otimizado para Power BI

Uso:
    python main.py
    python main.py --headless          # Sem janela (menos RAM)
    python main.py --workers 6         # Customizar nº de consumidores
    python main.py --paginas 100       # Limitar páginas por zona
    python main.py --pipeline-only     # Só rodar pós-processamento
"""
import argparse
import glob
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Garante que o diretório do script esteja no PATH de importação,
# independente de onde o usuário executar (ex: python scraper_rio/main.py)
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

from config import (
    logger,
    ZONAS_CONFIG,
    MAX_LISTING_WORKERS,
    MAX_DETAILS_WORKERS,
    NUM_PAGINAS,
)
from scraper import (
    LINK_QUEUE,
    PRODUCERS_DONE,
    produtor_listagem,
    consumidor_detalhes,
    get_stats,
)
from pipeline import unificar_e_tratar


def limpar_temps():
    """Remove CSVs temporários de execuções anteriores."""
    for f in glob.glob("temp_worker_*.csv"):
        try:
            os.remove(f)
        except Exception:
            pass


def build_tarefas(num_paginas: int) -> list:
    """Monta a lista de tarefas para os produtores (com split par/ímpar)."""
    tarefas = []
    for zona in ZONAS_CONFIG:
        if zona["split"]:
            tarefas.append({
                "zona": zona,
                "paginas": range(1, num_paginas + 1, 2),
                "worker_id": "impar",
            })
            tarefas.append({
                "zona": zona,
                "paginas": range(2, num_paginas + 1, 2),
                "worker_id": "par",
            })
        else:
            tarefas.append({
                "zona": zona,
                "paginas": range(1, num_paginas + 1),
                "worker_id": "unico",
            })
    return tarefas


def run_scraper(num_workers: int, num_paginas: int, headless: bool):
    """Executa o scraping completo: produtores → consumidores → pipeline."""
    limpar_temps()

    tarefas = build_tarefas(num_paginas)

    pag_display = "Até acabar os anúncios" if num_paginas >= 9999 else f"Máximo {num_paginas}"

    logger.info("=" * 60)
    logger.info("  SCRAPER DE IMÓVEIS — RIO DE JANEIRO")
    logger.info("=" * 60)
    logger.info(f"  Produtores:  {len(tarefas)} tarefas ({MAX_LISTING_WORKERS} workers)")
    logger.info(f"  Consumidores: {num_workers} workers")
    logger.info(f"  Páginas/zona: {pag_display}")
    logger.info(f"  Headless:     {headless}")
    logger.info("=" * 60)

    start_time = time.time()

    # --- FASE 1: Inicia consumidores ---
    executor_consumidores = ThreadPoolExecutor(
        max_workers=num_workers,
        thread_name_prefix="Consumer",
    )
    futures_consumidores = [
        executor_consumidores.submit(consumidor_detalhes, i, headless)
        for i in range(num_workers)
    ]

    # --- FASE 2: Inicia produtores (bloqueante até todos terminarem) ---
    with ThreadPoolExecutor(
        max_workers=MAX_LISTING_WORKERS,
        thread_name_prefix="Producer",
    ) as executor_produtores:
        futures_produtores = [
            executor_produtores.submit(produtor_listagem, t, headless)
            for t in tarefas
        ]
        for f in as_completed(futures_produtores):
            try:
                f.result()
            except Exception as e:
                logger.error(f"Produtor falhou: {e}")

    # --- FASE 3: Sinaliza que produtores acabaram ---
    logger.info("✅ Todos os produtores finalizaram. Sinalizando consumidores...")
    PRODUCERS_DONE.set()

    # Sentinelas adicionais (fallback para consumidores que estejam bloqueados no get)
    for _ in range(num_workers):
        LINK_QUEUE.put(None)

    # --- FASE 4: Espera consumidores terminarem ---
    for f in as_completed(futures_consumidores):
        try:
            f.result()
        except Exception as e:
            logger.error(f"Consumidor falhou: {e}")

    executor_consumidores.shutdown(wait=True)

    elapsed = time.time() - start_time
    stats = get_stats()
    logger.info("=" * 60)
    logger.info(f"  SCRAPING FINALIZADO em {elapsed/60:.1f} minutos")
    logger.info(f"  Enfileirados: {stats['enqueued']}")
    logger.info(f"  Processados:  {stats['processed']}")
    logger.info(f"  Erros:        {stats['errors']}")
    logger.info(f"  Duplicatas:   {stats['skipped_dup']}")
    logger.info("=" * 60)

    # --- FASE 5: Pipeline de pós-processamento ---
    unificar_e_tratar()


def main():
    parser = argparse.ArgumentParser(
        description="Scraper de Imóveis — ZAP Imóveis RJ",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--headless", action="store_true",
        help="Executar Chrome sem janela (recomendado)",
    )
    parser.add_argument(
        "--workers", type=int, default=MAX_DETAILS_WORKERS,
        help=f"Nº de consumidores (default: {MAX_DETAILS_WORKERS})",
    )
    parser.add_argument(
        "--paginas", type=int, default=0,
        help=(
            "Limite de páginas por zona.\n"
            "  0 = sem limite, para quando acabar os anúncios (padrão)\n"
            "  N = máximo de N páginas por zona\n"
            "Exemplos:\n"
            "  --paginas 0    → varre tudo até o fim\n"
            "  --paginas 5    → apenas 5 páginas por zona (teste rápido)\n"
            "  --paginas 100  → no máximo 100 páginas por zona"
        ),
    )
    parser.add_argument(
        "--pipeline-only", action="store_true",
        help="Só rodar pós-processamento nos CSVs já coletados",
    )

    args = parser.parse_args()

    if args.pipeline_only:
        logger.info("Modo pipeline-only: processando CSVs existentes...")
        unificar_e_tratar()
    else:
        # paginas=0 significa sem limite (vai até acabar os anúncios)
        num_paginas = args.paginas if args.paginas > 0 else 9999

        run_scraper(
            num_workers=args.workers,
            num_paginas=num_paginas,
            headless=args.headless,
        )


if __name__ == "__main__":
    main()