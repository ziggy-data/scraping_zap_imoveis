"""
Limpeza, deduplicação, e inteligência de preço (média rua/bairro).
"""
import os as _os, sys as _sys
_PARENT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
if _PARENT not in _sys.path:
    _sys.path.insert(0, _PARENT)

import re
import pandas as pd
import numpy as np

from config import logger


def aplicar_regras_qualidade(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicação, conversão numérica, filtro de sanidade, outliers."""
    logger.info(f"🧼 Tratamento de qualidade. Dados brutos: {len(df)}")

    # 0. Limpeza de nomes de coluna (BOM, espaços, etc.)
    df.columns = df.columns.str.strip().str.replace("\ufeff", "")
    logger.info(f"   Colunas detectadas: {list(df.columns[:10])}...")

    # 1. ID canônico via URL
    def _extract_id(x):
        m = re.search(r"id-(\d+)", str(x))
        return m.group(1) if m else str(x)

    df["id_imovel"] = df["url"].apply(_extract_id)
    antes_dedup = len(df)
    df.drop_duplicates(subset=["id_imovel"], keep="first", inplace=True)
    logger.info(f"   Após dedup: {len(df)} (removidos: {antes_dedup - len(df)})")

    # 2. Conversão numérica (com diagnóstico)
    cols_num = ["valor_R$", "area_m2", "condominio_R$", "iptu_R$",
                "quartos", "vagas", "banheiros", "suites"]
    for col in cols_num:
        if col in df.columns:
            antes = df[col].head(3).tolist()
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            depois = df[col].head(3).tolist()
            # Log de diagnóstico para os campos críticos
            if col in ("valor_R$", "area_m2"):
                nao_zero = (df[col] > 0).sum()
                logger.info(f"   {col}: {antes} → {depois} ({nao_zero}/{len(df)} não-zero)")
        else:
            logger.warning(f"   ⚠️ Coluna '{col}' NÃO encontrada no CSV!")

    # 3. Flag de vaga
    df["bool_vaga"] = (df["vagas"] > 0).astype(int)

    # 4. Filtro de sanidade
    mask_valor = df["valor_R$"] > 10_000
    mask_area = df["area_m2"] >= 10
    logger.info(
        f"   Filtro: valor>10k: {mask_valor.sum()}, area>=10: {mask_area.sum()}, "
        f"ambos: {(mask_valor & mask_area).sum()}"
    )
    df = df[mask_valor & mask_area].copy()

    if len(df) == 0:
        logger.error(
            "   ❌ ZERO registros após filtro de sanidade! "
            "Verifique se os seletores CSS estão extraindo preço e área corretamente."
        )
        return df

    # 5. Preço/m²
    df["preco_m2"] = (df["valor_R$"] / df["area_m2"]).round(2)

    # 6. Remoção de outliers (1º e 99º percentil)
    if len(df) > 20:
        q_lo = df["preco_m2"].quantile(0.01)
        q_hi = df["preco_m2"].quantile(0.99)
        antes_outlier = len(df)
        df = df[(df["preco_m2"] > q_lo) & (df["preco_m2"] < q_hi)].copy()
        logger.info(f"   Outliers removidos: {antes_outlier - len(df)}")

    # 7. Padronização de texto
    for col in ["bairro", "tipo", "cidade", "descricao", "rua", "corretora"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()

    # 8. Limpeza do campo "tipo" (card traz "Apartamento Para Comprar Com 225 M²...")
    if "tipo" in df.columns:
        # Extrai só a parte antes de "Para" → "Apartamento", "Casa", "Sala/Conjunto"
        df["tipo"] = df["tipo"].str.extract(r"^(.+?)\s+Para\b", expand=False).fillna(df["tipo"])
        df["tipo"] = df["tipo"].str.strip().str.title()

    # 9. tipo_imovel: usa o dado da detail page; se vazio, usa tipo limpo do card
    if "tipo_imovel" in df.columns:
        df["tipo_imovel"] = df["tipo_imovel"].astype(str).str.strip().str.title()
        mask_vazio = df["tipo_imovel"].isin(["", "Nan", "Não Informado", "None"])
        if "tipo" in df.columns:
            df.loc[mask_vazio, "tipo_imovel"] = df.loc[mask_vazio, "tipo"]

    logger.info(f"✨ Dados qualificados: {len(df)} registros.")
    return df


def calcular_media_inteligente(df: pd.DataFrame, min_amostras_rua: int = 3) -> pd.DataFrame:
    """
    Preço de referência hierárquico: Rua (se >= min_amostras) → Bairro.
    Calcula diferença percentual para detecção de oportunidades.
    """
    logger.info("🧠 Calculando médias inteligentes (Rua > Bairro)...")

    df["preco_m2_real"] = np.where(
        df["area_m2"] > 0, df["valor_R$"] / df["area_m2"], 0
    )
    df_calc = df[df["preco_m2_real"] > 100].copy()

    # Médias por bairro
    stats_bairro = (
        df_calc.groupby("bairro")["preco_m2_real"]
        .mean()
        .reset_index()
        .rename(columns={"preco_m2_real": "media_bairro_m2"})
    )

    # Médias por rua (com contagem de amostras)
    stats_rua = (
        df_calc.groupby(["bairro", "rua"])["preco_m2_real"]
        .agg(["mean", "count"])
        .reset_index()
        .rename(columns={"mean": "media_rua_m2", "count": "qtd_amostras_rua"})
    )

    df = pd.merge(df, stats_bairro, on="bairro", how="left")
    df = pd.merge(df, stats_rua, on=["bairro", "rua"], how="left")

    # Decisão hierárquica: rua com amostra suficiente → rua; senão → bairro
    ruas_invalidas = {"Rua Não Informada", "Endereço Não Disponível", "Nan", ""}
    tem_rua_valida = (~df["rua"].isin(ruas_invalidas)) & (df["qtd_amostras_rua"] >= min_amostras_rua)

    df["preco_m2_referencia"] = np.where(
        tem_rua_valida, df["media_rua_m2"], df["media_bairro_m2"]
    ).round(2)

    df["origem_referencia"] = np.where(
        tem_rua_valida,
        "Rua",
        np.where(
            df["rua"].isin(ruas_invalidas),
            "Bairro (Rua desconhecida)",
            "Bairro (Amostra insuficiente na rua)"
        )
    )

    df["diferenca_percentual"] = (
        ((df["preco_m2_real"] - df["preco_m2_referencia"]) / df["preco_m2_referencia"]) * 100
    ).round(1)

    return df


def analisar_saturacao_rua(df: pd.DataFrame) -> pd.DataFrame:
    """Indicador de risco: ruas com concentração de ofertas muito acima da média."""
    logger.info("🚨 Analisando concentração de ofertas (Risco de Fuga)...")

    oferta_rua = (
        df.groupby(["bairro", "rua"])["url"]
        .count()
        .reset_index()
        .rename(columns={"url": "qtd_na_rua"})
    )

    media_bairro = (
        oferta_rua.groupby("bairro")["qtd_na_rua"]
        .mean()
        .reset_index()
        .rename(columns={"qtd_na_rua": "media_ofertas_por_rua_no_bairro"})
    )

    df = pd.merge(df, oferta_rua, on=["bairro", "rua"], how="left")
    df = pd.merge(df, media_bairro, on="bairro", how="left")

    # Classificação vetorizada (sem apply)
    qtd = df["qtd_na_rua"].fillna(0)
    media = df["media_ofertas_por_rua_no_bairro"].fillna(1)

    conditions = [
        qtd < 3,
        qtd > (media * 3),
        qtd > (media * 1.5),
    ]
    choices = [
        "Normal",
        "ALERTA: Fuga Possível (Oferta Muito Alta)",
        "Oferta Elevada",
    ]
    df["alerta_oferta_rua"] = np.select(conditions, choices, default="Normal/Baixa")

    return df