"""
Módulos de enriquecimento pós-scraping:
- geo: contexto geográfico vetorizado
- finance: análise financeira com parâmetros da Caixa
- nlp: mineração de texto (tags e urgência)
- quality: limpeza, dedup, segmentação
"""
import os as _os, sys as _sys
_PARENT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
if _PARENT not in _sys.path:
    _sys.path.insert(0, _PARENT)

from enrichment.geo import enriquecer_geo_contexto
from enrichment.finance import analise_financeira
from enrichment.nlp import (
    extrair_tags,
    detectar_urgencia,
    calcular_dias_mercado,
    segmentar_mercado,
    calcular_conforto,
)
from enrichment.quality import (
    aplicar_regras_qualidade,
    calcular_media_inteligente,
    analisar_saturacao_rua,
)
from enrichment.profiles import gerar_perfis
from enrichment.ml_pricing import enriquecer_com_escrituras

__all__ = [
    "enriquecer_geo_contexto",
    "analise_financeira",
    "extrair_tags",
    "detectar_urgencia",
    "calcular_dias_mercado",
    "segmentar_mercado",
    "calcular_conforto",
    "aplicar_regras_qualidade",
    "calcular_media_inteligente",
    "analisar_saturacao_rua",
    "gerar_perfis",
    "enriquecer_com_escrituras",
]