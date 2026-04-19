"""
Pipeline de pós-processamento: unifica CSVs temporários, aplica todos os
enriquecimentos e gera o dataset final otimizado para Power BI.
"""
import os as _os, sys as _sys
_DIR = _os.path.dirname(_os.path.abspath(__file__))
if _DIR not in _sys.path:
    _sys.path.insert(0, _DIR)

import glob
import os

import pandas as pd

from config import logger
from enrichment.quality import (
    aplicar_regras_qualidade,
    calcular_media_inteligente,
    analisar_saturacao_rua,
)
from enrichment.geo import enriquecer_geo_contexto
from enrichment.finance import analise_financeira
from enrichment.nlp import (
    extrair_tags,
    detectar_urgencia,
    calcular_dias_mercado,
    segmentar_mercado,
    calcular_conforto,
)
from enrichment.profiles import gerar_perfis
from enrichment.ml_pricing import enriquecer_com_escrituras


def unificar_e_tratar(arquivo_final: str = "DATASET_RIO_BI_READY.csv"):
    """
    Pipeline completo de pós-processamento:
    1. Unifica CSVs temporários dos workers
    2. Limpeza e deduplicação
    3. Inteligência de preço
    4. Geo-contexto vetorizado
    5. Análise financeira
    6. NLP / Tags / Urgência
    7. Segmentação e scoring
    8. Otimização para Power BI
    """
    logger.info("=" * 60)
    logger.info("  PIPELINE DE PÓS-PROCESSAMENTO")
    logger.info("=" * 60)

    # --- FASE 1: UNIFICAÇÃO ---
    all_files = sorted(glob.glob("temp_worker_*.csv"))
    if not all_files:
        logger.error("Nenhum arquivo temporário encontrado!")
        return

    logger.info(f"📂 Unificando {len(all_files)} arquivos temporários...")
    dfs = []
    for f in all_files:
        try:
            df_temp = pd.read_csv(
                f, sep=";", dtype=str, on_bad_lines="skip",
                encoding="utf-8-sig",  # Mesmo encoding usado na escrita
            )
            dfs.append(df_temp)
            logger.info(f"   ✓ {f}: {len(df_temp)} linhas")
        except Exception as e:
            logger.warning(f"   ✗ Erro ao ler {f}: {e}")

    if not dfs:
        logger.error("Nenhum dado válido encontrado nos temporários!")
        return

    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"📊 Total bruto: {len(df)} registros")

    try:
        # --- FASE 2: QUALIDADE ---
        df = aplicar_regras_qualidade(df)

        if len(df) == 0:
            logger.error(
                "❌ Pipeline abortado: 0 registros após filtro de qualidade. "
                "Os CSVs temporários foram preservados para diagnóstico."
            )
            return

        # --- FASE 3: INTELIGÊNCIA DE PREÇO ---
        df = calcular_media_inteligente(df, min_amostras_rua=3)

        # --- FASE 4: GEO-CONTEXTO ---
        df = enriquecer_geo_contexto(df)

        # --- FASE 5: FINANCEIRO ---
        df = analise_financeira(df)

        # --- FASE 6: SATURAÇÃO DE OFERTA ---
        df = analisar_saturacao_rua(df)

        # --- FASE 7: SEGMENTAÇÃO ---
        df = segmentar_mercado(df)

        # --- FASE 8: DIAS NO MERCADO ---
        df = calcular_dias_mercado(df)

        # --- FASE 9: NLP TAGS ---
        df = extrair_tags(df)

        # --- FASE 10: URGÊNCIA ---
        df = detectar_urgencia(df)

        # --- FASE 11: CONFORTO ---
        df = calcular_conforto(df)

        # --- FASE 12: PERFIS (zona, faixas, família/investidor/1º imóvel) ---
        df = gerar_perfis(df)

        # --- FASE 13: ANÁLISE DE FEATURES + SELEÇÃO DE MODELO ---
        if len(df) >= 50:
            try:
                _executar_analise_ia(df)
            except Exception as e:
                logger.warning(f"   ⚠️ Análise IA falhou: {e}. Continuando...")

        # --- FASE 14: ML + ESCRITURAS ---
        df = enriquecer_com_escrituras(df)

        # --- FASE 15: MODELOS AVANÇADOS (Clustering + Anomalia + MLP) ---
        if len(df) >= 100:
            try:
                from ia.modelos_avancados import executar_modelos_avancados
                df = executar_modelos_avancados(df)
            except Exception as e:
                logger.warning(f"   ⚠️ Modelos avançados falhou: {e}. Continuando...")

        # --- FASE 16: OTIMIZAÇÃO POWER BI ---
        _otimizar_para_powerbi(df)

        # --- SALVAR ---
        df.to_csv(
            arquivo_final,
            sep=";",
            decimal=",",
            index=False,
            encoding="utf-8-sig",
        )

        logger.info("=" * 60)
        logger.info(f"🏆 SUCESSO! Dataset gerado: {arquivo_final}")
        logger.info(f"📊 Total Imóveis: {len(df)}")
        logger.info(f"📋 Total Colunas: {len(df.columns)}")
        logger.info("=" * 60)

        # Limpa temporários
        for f in all_files:
            try:
                os.remove(f)
            except Exception:
                pass

    except Exception as e:
        logger.error(f"Erro no pipeline de processamento: {e}", exc_info=True)


def _executar_analise_ia(df: pd.DataFrame):
    """Executa análise de features e salva config para o ML usar."""
    logger.info("🧪 Análise de Features e Seleção de Modelo...")

    # Salva CSV temporário
    _tmp = "__temp_analise.csv"
    df.to_csv(_tmp, sep=";", decimal=",", index=False, encoding="utf-8-sig")

    try:
        import subprocess, sys
        result = subprocess.run(
            [sys.executable, "ia/analise_features.py", "--dataset", _tmp],
            capture_output=True, text=True, timeout=300,
        )
        if result.returncode != 0:
            logger.warning(f"   ⚠️ Análise retornou erro: {result.stderr[:200]}")
        else:
            # Mostra resumo
            for line in result.stdout.split("\n"):
                if "CONCLUÍDA" in line or "Melhor" in line or "Features:" in line:
                    logger.info(f"   {line.strip()}")
    finally:
        if os.path.exists(_tmp):
            os.remove(_tmp)


def _otimizar_para_powerbi(df: pd.DataFrame):
    """Ajustes de tipo e formato para compatibilidade com Power BI."""
    logger.info("📊 Otimizando tipos de dados para Power BI...")

    # A. Nulos em colunas financeiras → 0
    cols_fin = [
        "condominio_R$", "iptu_R$", "valor_R$", "area_m2", "custo_fixo_mensal",
        "entrada_minima", "valor_financiado", "primeira_parcela_estimada",
        "renda_minima_familiar",
    ]
    for col in cols_fin:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # B. Booleanos → int (True/False → 1/0)
    bool_prefixes = ("tem_", "aceita_", "tag_", "flag_", "bool_", "alerta_", "gatilho_")
    bool_map = {"True": 1, "False": 0, "true": 1, "false": 0, True: 1, False: 0}
    for col in df.columns:
        if df[col].dtype == "bool" or col.startswith(bool_prefixes):
            try:
                mapped = df[col].map(bool_map)
                df[col] = pd.to_numeric(mapped.fillna(df[col]), errors="coerce").fillna(0).astype(int)
            except Exception:
                pass

    # C. Data ISO (Time Intelligence do Power BI)
    if "anuncio_criado" in df.columns:
        df["anuncio_criado"] = (
            pd.to_datetime(df["anuncio_criado"], errors="coerce")
            .dt.strftime("%Y-%m-%d")
        )

    # D. Fusão de colunas redundantes (OR lógico)
    logger.info("🔗 Fundindo colunas redundantes...")

    _fuse_or(df, ["tem_vista_pro_mar", "tag_vista_mar"], "final_tem_vista_mar")
    _fuse_or(df, ["tem_varanda", "tem_varanda_gourmet"], "final_tem_varanda")
    _fuse_or(df, ["tem_piscina", "tem_churrasqueira", "tem_academia"], "final_tem_lazer")


def _fuse_or(df: pd.DataFrame, source_cols: list, target_col: str):
    """Cria coluna booleana (OR) a partir de múltiplas colunas fonte."""
    existing = [c for c in source_cols if c in df.columns]
    if existing:
        df[target_col] = df[existing].fillna(0).astype(int).max(axis=1)