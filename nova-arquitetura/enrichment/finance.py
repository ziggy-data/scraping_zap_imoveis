"""
finance.py — Módulo de Análise Financeira Imobiliária (2026)
==============================================================

Simula financiamento para cada imóvel em 3 cenários:
  1. MCMV (Minha Casa Minha Vida) — 4 faixas com taxas e subsídios
  2. Mercado SFH (Sistema Financeiro de Habitação) — até R$ 2,25M
  3. Mercado SFI (Sistema de Financiamento Imobiliário) — acima de R$ 2,25M

Para cada cenário calcula SAC e Price.

Fontes:
  - Conselho Curador do FGTS (Março/2026)
  - Caixa Econômica Federal — Simulador Habitacional
"""
import os as _os, sys as _sys
_PARENT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
if _PARENT not in _sys.path:
    _sys.path.insert(0, _PARENT)

import numpy as np
import pandas as pd
from config import logger

# ==============================================================================
#  PARÂMETROS MCMV 2026 — 4 FAIXAS (Sudeste / RJ)
# ==============================================================================

MCMV_FAIXAS = {
    1: {"renda_max": 2_850, "teto_imovel": 264_000, "subsidio_max": 55_000,
        "juros_aa": 0.0500, "cota": 0.95, "prazo": 420, "tem_subsidio": True},
    2: {"renda_max": 4_700, "teto_imovel": 264_000, "subsidio_max": 55_000,
        "juros_aa": 0.0700, "cota": 0.80, "prazo": 420, "tem_subsidio": True},
    3: {"renda_max": 8_600, "teto_imovel": 350_000, "subsidio_max": 0,
        "juros_aa": 0.0816, "cota": 0.80, "prazo": 420, "tem_subsidio": False},
    4: {"renda_max": 12_000, "teto_imovel": 500_000, "subsidio_max": 0,
        "juros_aa": 0.1050, "cota": 0.80, "prazo": 420, "tem_subsidio": False},
}

SFH = {"teto": 2_250_000, "juros_aa": 0.1099, "cota": 0.80, "prazo": 420}
SFI = {"teto": 999_999_999, "juros_aa": 0.1250, "cota": 0.70, "prazo": 360}

TAXA_ITBI_RJ = 0.03
TAXA_CARTORIO = 0.015
YIELD_ALUGUEL = 0.0045


# ==============================================================================
#  FUNÇÕES DE CÁLCULO
# ==============================================================================

def _mensal(aa): return (1 + aa) ** (1/12) - 1

def _sac(fin, jm, n):
    if fin <= 0 or n <= 0: return 0, 0, 0
    a = fin / n
    p1 = a + fin * jm
    pu = a + a * jm
    tj = jm * fin * (n + 1) / 2
    return round(p1, 2), round(pu, 2), round(tj, 2)

def _price(fin, jm, n):
    if fin <= 0 or n <= 0 or jm <= 0: return 0, 0
    f = (1 + jm) ** n
    p = fin * (jm * f) / (f - 1)
    tj = p * n - fin
    return round(p, 2), round(tj, 2)

def _enquadrar_mcmv(v):
    for num, f in sorted(MCMV_FAIXAS.items()):
        if v <= f["teto_imovel"]:
            return num, f
    return 0, None


# ==============================================================================
#  PIPELINE
# ==============================================================================

def analise_financeira(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("💰 Análise Financeira 2026 (MCMV 4 faixas + SAC/Price + Mercado)...")
    n = len(df)

    valor = pd.to_numeric(df["valor_R$"], errors="coerce").fillna(0).values
    condo = pd.to_numeric(df["condominio_R$"], errors="coerce").fillna(0).values
    iptu = pd.to_numeric(df["iptu_R$"], errors="coerce").fillna(0).values

    # --- Aquisição ---
    itbi = np.round(valor * TAXA_ITBI_RJ, 2)
    cart = np.round(valor * TAXA_CARTORIO, 2)
    df["itbi_estimado"] = itbi
    df["cartorio_estimado"] = cart
    df["custo_aquisicao_total"] = np.round(valor + itbi + cart, 2)

    # --- Arrays ---
    cols = {}
    for prefix in ["mcmv_", "merc_"]:
        for k in ["faixa", "elegivel", "teto", "subsidio", "juros_aa", "entrada",
                   "financiado", "sac_1a", "sac_ult", "sac_juros_total",
                   "price_parcela", "price_juros_total", "renda_min_sac",
                   "renda_min_price", "economia"]:
            cols[prefix + k] = np.zeros(n)

    merc_modal = [""] * n
    faixa_labels = {0: "Não elegível", 1: "Faixa 1", 2: "Faixa 2", 3: "Faixa 3", 4: "Faixa 4"}

    for i in range(n):
        v = valor[i]
        if v <= 0:
            merc_modal[i] = "N/A"
            continue

        # === MCMV ===
        fnum, finfo = _enquadrar_mcmv(v)
        if finfo:
            cols["mcmv_elegivel"][i] = 1
            cols["mcmv_faixa"][i] = fnum
            cols["mcmv_teto"][i] = finfo["teto_imovel"]

            sub = 0
            if finfo["tem_subsidio"]:
                fator = 0.50 if fnum == 1 else 0.30
                sub = min(finfo["subsidio_max"] * fator, v * 0.90)
            cols["mcmv_subsidio"][i] = round(sub, 2)

            fin = v * finfo["cota"]
            ent = max(v - fin - sub, 0)
            cols["mcmv_entrada"][i] = round(ent, 2)
            cols["mcmv_financiado"][i] = round(fin, 2)
            cols["mcmv_juros_aa"][i] = round(finfo["juros_aa"] * 100, 2)

            jm = _mensal(finfo["juros_aa"])
            pr = finfo["prazo"]

            s1, su, stj = _sac(fin, jm, pr)
            cols["mcmv_sac_1a"][i] = s1
            cols["mcmv_sac_ult"][i] = su
            cols["mcmv_sac_juros_total"][i] = stj

            pp, ptj = _price(fin, jm, pr)
            cols["mcmv_price_parcela"][i] = pp
            cols["mcmv_price_juros_total"][i] = ptj

            cols["mcmv_renda_min_sac"][i] = round(s1 / 0.30, 2)
            cols["mcmv_renda_min_price"][i] = round(pp / 0.30, 2)

        # === Mercado ===
        if v <= SFH["teto"]:
            mi = SFH; merc_modal[i] = "SFH"
        else:
            mi = SFI; merc_modal[i] = "SFI"

        fin_m = v * mi["cota"]
        ent_m = v - fin_m
        cols["merc_entrada"][i] = round(ent_m, 2)
        cols["merc_financiado"][i] = round(fin_m, 2)
        cols["merc_juros_aa"][i] = round(mi["juros_aa"] * 100, 2)

        jmm = _mensal(mi["juros_aa"])
        prm = mi["prazo"]

        ms1, msu, mstj = _sac(fin_m, jmm, prm)
        cols["merc_sac_1a"][i] = ms1
        cols["merc_sac_ult"][i] = msu
        cols["merc_sac_juros_total"][i] = mstj

        mpp, mptj = _price(fin_m, jmm, prm)
        cols["merc_price_parcela"][i] = mpp
        cols["merc_price_juros_total"][i] = mptj

        cols["merc_renda_min_sac"][i] = round(ms1 / 0.30, 2)
        cols["merc_renda_min_price"][i] = round(mpp / 0.30, 2)

        # Economia MCMV vs Mercado
        if finfo:
            cols["mcmv_economia"][i] = round(mptj - cols["mcmv_price_juros_total"][i], 2)

    # Atribui colunas
    for k, arr in cols.items():
        df[k] = arr
    df["merc_modalidade"] = merc_modal
    df["mcmv_faixa_label"] = df["mcmv_faixa"].astype(int).map(faixa_labels)

    logger.info(f"   MCMV elegíveis: {int(cols['mcmv_elegivel'].sum())}/{n}")

    # --- Investimento ---
    custo_fixo = condo + (iptu / 12)
    df["custo_fixo_mensal"] = np.round(custo_fixo, 2)

    aluguel = valor * YIELD_ALUGUEL
    df["aluguel_estimado"] = np.round(aluguel, 2)

    fluxo = aluguel - custo_fixo
    df["fluxo_caixa_mensal"] = np.round(fluxo, 2)
    df["yield_bruto_anual"] = np.where(valor > 0, np.round((aluguel * 12 / valor) * 100, 2), 0)

    custo_total = df["custo_aquisicao_total"].values
    fluxo_anual = fluxo * 12
    payback = np.where(fluxo_anual > 0, custo_total / fluxo_anual, 999)
    df["anos_payback"] = np.round(payback, 1)

    df["payback_classificacao"] = np.select(
        [payback <= 15, payback <= 20, payback <= 30, payback <= 50, payback > 50],
        ["Excelente (<=15 anos)", "Bom (15-20 anos)", "Regular (20-30 anos)",
         "Longo (30-50 anos)", "Inviavel (>50 anos)"],
        default="N/A"
    )

    # --- Score Investimento ---
    score = np.full(n, 5.0)
    diff = pd.to_numeric(df.get("diferenca_percentual", 0), errors="coerce").fillna(0).values
    dist_t = pd.to_numeric(df.get("dist_transporte_km", 99), errors="coerce").fillna(99).values

    score = np.where(diff < -20, score + 2, score)
    score = np.where((diff >= -20) & (diff < -10), score + 1, score)
    score = np.where(diff > 20, score - 2, score)
    score = np.where((diff > 10) & (diff <= 20), score - 1, score)
    score = np.where(dist_t < 0.5, score + 1.5, score)
    score = np.where((dist_t >= 0.5) & (dist_t < 1.0), score + 0.5, score)
    score = np.where(custo_fixo < 500, score + 1, score)
    score = np.where(custo_fixo > 2500, score - 1, score)
    score = np.where(payback < 15, score + 1, score)
    score = np.where(payback > 40, score - 1, score)
    score = np.where(cols["mcmv_elegivel"] == 1, score + 0.5, score)

    df["score_investimento"] = np.clip(np.round(score).astype(int), 0, 10)

    # --- Resumo ---
    resumos = []
    for i in range(n):
        p = []
        if cols["mcmv_elegivel"][i]:
            p.append(f"MCMV {faixa_labels[int(cols['mcmv_faixa'][i])]}: SAC R${cols['mcmv_sac_1a'][i]:,.0f}")
            if cols["mcmv_subsidio"][i] > 0:
                p.append(f"Subsidio ~R${cols['mcmv_subsidio'][i]:,.0f}")
            if cols["mcmv_economia"][i] > 0:
                p.append(f"Economia R${cols['mcmv_economia'][i]:,.0f}")
        else:
            p.append(f"{merc_modal[i]}: SAC R${cols['merc_sac_1a'][i]:,.0f}")
        if fluxo[i] > 0:
            p.append(f"Payback {payback[i]:.0f}a")
        resumos.append(" | ".join(p))
    df["resumo_financeiro"] = resumos

    return df