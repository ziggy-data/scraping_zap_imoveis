"""
Enriquecimento geográfico vetorizado: distâncias a POIs, scores, vocação Airbnb.
Usa NumPy em vez de df.apply() row-by-row → ordens de magnitude mais rápido.
"""
import os as _os, sys as _sys
_PARENT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
if _PARENT not in _sys.path:
    _sys.path.insert(0, _PARENT)

import numpy as np
import pandas as pd

from config import logger, POIS_RIO, BAIRROS_TURISTICOS
from utils import haversine_vectorized


def _parse_coordenadas(df: pd.DataFrame) -> tuple:
    """Extrai arrays NumPy de lat/lon a partir da coluna 'coordenadas'."""
    n = len(df)
    if n == 0:
        return np.array([]), np.array([])

    if "coordenadas" not in df.columns:
        return np.zeros(n), np.zeros(n)

    coords_split = df["coordenadas"].astype(str).str.split(",", expand=True)

    # Se o split não gerou colunas (ex: todos NaN), retorna zeros
    if coords_split.shape[1] == 0:
        return np.zeros(n), np.zeros(n)

    lats = pd.to_numeric(coords_split[0], errors="coerce").fillna(0).values

    if coords_split.shape[1] > 1:
        lons = pd.to_numeric(coords_split[1], errors="coerce").fillna(0).values
    else:
        lons = np.zeros(n)

    return lats, lons


def _find_nearest_vectorized(
    lats: np.ndarray, lons: np.ndarray, categoria: str
) -> tuple:
    """
    Para cada imóvel, encontra o POI mais próximo em uma categoria.
    Retorna (distancias, nomes) como arrays.
    """
    pois = POIS_RIO.get(categoria, {})
    n = len(lats)

    if not pois:
        return np.full(n, 99.9), np.full(n, "N/A", dtype=object)

    min_dists = np.full(n, 999.0)
    min_names = np.full(n, "N/A", dtype=object)

    # Máscara de coordenadas válidas
    valid = (lats != 0) & (lons != 0)

    for nome, (plat, plon) in pois.items():
        dists = np.full(n, 999.0)
        dists[valid] = haversine_vectorized(lats[valid], lons[valid], plat, plon)

        closer = dists < min_dists
        min_dists[closer] = dists[closer]
        min_names[closer] = nome

    return np.round(min_dists, 2), min_names


def enriquecer_geo_contexto(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula distâncias vetorizadas para todas as categorias de POI.
    Gera scores de mobilidade, segurança, lifestyle e vocação Airbnb.
    """
    logger.info("🌍 Calculando Geo-Contexto 360° (vetorizado)...")
    lats, lons = _parse_coordenadas(df)

    # --- Distâncias por categoria ---
    categorias_simples = {
        "praias": ("dist_praia_km", "praia_prox"),
        "lazer_verde": ("dist_lazer_km", "lazer_prox"),
        "shoppings_premium": ("dist_shopping_km", "shopping_prox"),
        "cultura_esporte": ("dist_cultura_km", "cultura_prox"),
        "mercados_essenciais": ("dist_mercado_km", "mercado_prox"),
        "feiras_alimentacao": ("dist_feira_km", "feira_prox"),
        "saude_educacao": ("dist_saude_educ_km", "saude_educ_prox"),
        "areas_sensiveis": ("dist_risco_km", "risco_prox"),
        "seguranca_publica": ("dist_policia_km", "policia_prox"),
        "pontos_turisticos": ("dist_turismo_km", "turismo_prox"),
    }

    for cat, (col_dist, col_nome) in categorias_simples.items():
        dists, nomes = _find_nearest_vectorized(lats, lons, cat)
        df[col_dist] = dists
        df[col_nome] = nomes

    # --- Transporte: merge metrô/trem + BRT, pega o mais perto ---
    dist_metro, nome_metro = _find_nearest_vectorized(lats, lons, "transporte_hub")
    dist_brt, nome_brt = _find_nearest_vectorized(lats, lons, "brt_stations")

    brt_closer = dist_brt < dist_metro
    df["dist_transporte_km"] = np.where(brt_closer, dist_brt, dist_metro)
    df["transporte_prox"] = np.where(brt_closer, nome_brt, nome_metro)

    # --- Score Mobilidade (0-10) ---
    dist_t = df["dist_transporte_km"].values
    score_mob = np.full(len(df), 0.0)
    score_mob = np.where(dist_t < 0.6, 10.0, score_mob)
    score_mob = np.where((dist_t >= 0.6) & (dist_t < 1.5), 8.0, score_mob)
    score_mob = np.where((dist_t >= 1.5) & (dist_t < 3.0), 6.0, score_mob)
    score_mob = np.where((dist_t >= 3.0) & (dist_t < 5.0), 4.0, score_mob)
    score_mob = np.where(dist_t >= 5.0, np.maximum(0, 10 - dist_t * 1.5), score_mob)
    df["score_mobilidade"] = np.round(score_mob, 1)

    # --- Score Segurança (0-10) — saldo risco vs. presença policial ---
    score_seg = np.full(len(df), 5.0)
    dist_risco = df["dist_risco_km"].values.astype(float)
    dist_pol = df["dist_policia_km"].values.astype(float)

    # Penalidade por proximidade de área sensível
    score_seg = np.where(dist_risco < 0.5, score_seg - 4, score_seg)
    score_seg = np.where((dist_risco >= 0.5) & (dist_risco < 1.0), score_seg - 2, score_seg)
    score_seg = np.where((dist_risco >= 1.0) & (dist_risco < 2.0), score_seg - 0.5, score_seg)

    # Bônus por proximidade de BPM/DP
    score_seg = np.where(dist_pol < 0.8, score_seg + 2.5, score_seg)
    score_seg = np.where((dist_pol >= 0.8) & (dist_pol < 2.0), score_seg + 1, score_seg)
    df["score_seguranca"] = np.clip(np.round(score_seg, 1), 0, 10)

    # --- Score Lifestyle (0-10) ---
    # Quantos serviços "walkable" (< 1.5km) o imóvel tem?
    walkable_cols = [
        "dist_transporte_km", "dist_mercado_km", "dist_feira_km",
        "dist_lazer_km", "dist_praia_km", "dist_shopping_km",
    ]
    walkable_count = sum(
        (df[col].values < 1.5).astype(int) for col in walkable_cols if col in df.columns
    )
    df["score_lifestyle"] = np.clip(np.round(walkable_count * 10 / 6, 1), 0, 10)

    # --- Vocação Airbnb (expandida para além da Zona Sul) ---
    bairro_series = df["bairro"].astype(str).str.strip().str.title()
    is_turistico = bairro_series.isin(BAIRROS_TURISTICOS)
    dist_praia = df["dist_praia_km"].values
    dist_turismo = df["dist_turismo_km"].values if "dist_turismo_km" in df.columns else np.full(len(df), 99.0)

    vocacao = np.full(len(df), "Baixa", dtype=object)
    vocacao = np.where(
        is_turistico & ((dist_praia < 0.8) | (dist_turismo < 1.0)),
        "Altíssima", vocacao
    )
    vocacao = np.where(
        is_turistico & (vocacao != "Altíssima") & ((dist_praia < 2.0) | (dist_turismo < 2.0)),
        "Alta", vocacao
    )
    vocacao = np.where(
        (~is_turistico.values) & (dist_praia < 1.0),
        "Média", vocacao
    )
    df["vocacao_airbnb"] = vocacao

    return df