# Scraper de Imóveis do Rio de Janeiro v2.0

Scraper produtor-consumidor para extração, enriquecimento e análise de imóveis do **ZAP Imóveis** na cidade do Rio de Janeiro.

## Arquitetura

```
scraper_rio/
├── __init__.py              # Package info
├── config.py                # Constantes, POIs, parâmetros financeiros, seletores CSS
├── driver.py                # Chrome driver (criação, stealth, scroll)
├── utils.py                 # Retry, parsing, haversine, geocoding
├── scraper.py               # Produtor/consumidor (extração de dados)
├── pipeline.py              # Orquestração do pós-processamento
├── enrichment/
│   ├── __init__.py
│   ├── geo.py               # Contexto geográfico (VETORIZADO com NumPy)
│   ├── finance.py           # Análise financeira (parâmetros Caixa)
│   ├── nlp.py               # Mineração de texto (tags + urgência)
│   └── quality.py           # Limpeza, dedup, segmentação, conforto
└── main.py                  # Entry point com CLI
```

## Uso

```bash
# Scraping completo + pipeline
python -m scraper_rio.main

# Modo headless (sem janela — economia de RAM)
python -m scraper_rio.main --headless

# Só pipeline (reprocessa CSVs já existentes)
python -m scraper_rio.main --pipeline-only

# Output customizado
python -m scraper_rio.main --output meu_dataset.csv
```

## Dependências

```
pip install pandas numpy requests undetected-chromedriver selenium
```

## Melhorias v2.0 vs v1.0

### Performance
| Aspecto | v1.0 | v2.0 |
|---|---|---|
| Geo-contexto | `df.apply(axis=1)` + haversine escalar | **NumPy vetorizado** (~30x mais rápido em 17k registros) |
| Consumidores Chrome | 18 instâncias simultâneas | **8** (com `--headless` + restart periódico) |
| Análise financeira | `df.apply(axis=1)` por linha | **Totalmente vetorizado** com `np.where` |

### Robustez
| Aspecto | v1.0 | v2.0 |
|---|---|---|
| Deduplicação na fila | Nenhuma (dups possíveis) | **Set thread-safe** com lock |
| Encerramento | `qsize()` (race-prone) | **`threading.Event`** + sentinelas |
| Retry | Nenhum (except pass) | **Decorator com backoff** (3 tentativas) |
| Nominatim rate limit | Nenhum (risco de ban) | **Semaphore global** (1 req/s) |
| CSV buffering | Padrão (risco de perda) | **Line-buffered** (flush imediato) |

### Qualidade dos dados
| Aspecto | v1.0 | v2.0 |
|---|---|---|
| "arborizada" = "silencioso" | Falso positivo | **Separados** em tags distintas |
| Vocação Airbnb | Só Zona Sul (hardcoded) | **16+ bairros turísticos** + distância a pontos turísticos |
| Financiamento | Juros fixos 10.5% | **Diferenciado MCMV vs mercado** (7.5% / 10.5%) |
| Tags NLP | 15 padrões | **19 padrões** (+ home_office, terraço, energia solar, duplex) |
| Score lifestyle | Não existia | **Score 0-10** baseado em acesso a lazer/cultura/praia |

### Arquitetura
| Aspecto | v1.0 | v2.0 |
|---|---|---|
| Estrutura | Arquivo único 1531 linhas | **9 módulos** organizados por responsabilidade |
| CLI | Nenhum | **argparse** com `--headless`, `--pipeline-only`, `--output` |
| Configuração | Constantes espalhadas | **`config.py`** centralizado + `@dataclass` |
| Logging | Básico | **Thread-name prefix** para rastreio |

## Parâmetros Financeiros

Editável em `config.py > ParametrosFinanceiros`:

```python
@dataclass
class ParametrosFinanceiros:
    taxa_itbi: float = 0.03          # 3%
    taxa_cartorio: float = 0.015     # 1.5%
    juros_aa_mercado: float = 0.105  # 10.5% a.a.
    juros_aa_mcmv_f3: float = 0.075  # 7.5% a.a. (MCMV Faixa 3)
    teto_mcmv: float = 350_000.0     # Teto MCMV
    yield_aluguel: float = 0.0045    # 0.45% mensal
```

## Pipeline de Enriquecimento (Ordem)

```
CSV Bruto → Qualidade → Médias Preço → Geo 360° → Financeiro → Saturação → Segmentação → DOM → NLP Tags → Urgência → Conforto → Power BI
```
