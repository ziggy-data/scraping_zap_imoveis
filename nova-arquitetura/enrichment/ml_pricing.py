"""
ml_pricing.py — Estimativa de Preço Justo via Dados de Escritura.
=================================================================

Cruza dados do ZAP com transações reais da Prefeitura do RJ para:
  1. Calcular preço/m² real por bairro (dados de escritura vs anúncio)
  2. Estimar "preço justo" baseado em regressão (features do imóvel)
  3. Detectar oportunidades (preço pedido muito abaixo do estimado)
  4. Calcular yield real por bairro (baseado em dados históricos)

Uso standalone:
    python -m enrichment.ml_pricing --escrituras escrituras.csv --dataset DATASET_RIO_BI_READY.csv

Uso no pipeline (automático se o CSV de escrituras existir):
    Coloca 'escrituras_prefeitura.csv' na pasta do projeto.
"""
import os as _os, sys as _sys
_PARENT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
if _PARENT not in _sys.path:
    _sys.path.insert(0, _PARENT)

import numpy as np
import pandas as pd
from pathlib import Path
from config import logger

ESCRITURAS_DEFAULT = "escrituras_prefeitura.csv"


# ==============================================================================
#  1. CARREGAR E PREPARAR DADOS DE ESCRITURA
# ==============================================================================

def _carregar_escrituras(caminho: str) -> pd.DataFrame:
    """Carrega CSV da prefeitura e normaliza."""
    if not Path(caminho).exists():
        return pd.DataFrame()

    df = pd.read_csv(caminho, encoding="utf-8")

    # Normaliza nomes: strip + lowercase + remove acentos
    import unicodedata
    def _strip_acc(s):
        return ''.join(
            c for c in unicodedata.normalize('NFD', s)
            if unicodedata.category(c) != 'Mn'
        )

    df.columns = [_strip_acc(c.strip().lower()) for c in df.columns]

    # Renomeia para nomes padronizados
    renames = {}
    for col in df.columns:
        if col == "bairro":
            continue
        elif "valor_transa" in col or "media_valor_t" in col:
            renames[col] = "media_valor_transacao"
        elif "ano_transa" in col:
            renames[col] = "ano_transacao"
        elif "area_terreno" in col or "media_area" in col:
            renames[col] = "media_area_terreno"
        elif "valor_imovel" in col or "media_valor_i" in col:
            renames[col] = "media_valor_imovel"
        elif col == "logradouro":
            pass
        elif "total_transa" in col:
            renames[col] = "total_transacoes"
    df.rename(columns=renames, inplace=True)

    logger.info(f"   Colunas escritura: {list(df.columns)}")

    # Limpeza
    if "bairro" not in df.columns:
        logger.warning("   ⚠️ Coluna 'bairro' não encontrada nas escrituras. Pulando.")
        return pd.DataFrame()

    df["bairro"] = df["bairro"].astype(str).str.strip().str.title()

    for col in ["media_valor_transacao", "media_valor_imovel", "media_area_terreno",
                "ano_transacao", "total_transacoes"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


# ==============================================================================
#  2. ESTATÍSTICAS POR BAIRRO (escrituras)
# ==============================================================================

def _calcular_stats_bairro(df_esc: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega dados de escritura por bairro → preço/m² real, tendência,
    volume de transações.
    """
    if df_esc.empty:
        return pd.DataFrame()

    # Filtra transações com valor válido
    mask = (df_esc["media_valor_transacao"] > 10_000)
    if "media_area_terreno" in df_esc.columns:
        mask = mask & (df_esc["media_area_terreno"] > 0)
    df_valid = df_esc[mask].copy()

    if df_valid.empty:
        return pd.DataFrame()

    # Preço/m² real por escritura
    if "media_area_terreno" in df_valid.columns:
        df_valid["preco_m2_escritura"] = (
            df_valid["media_valor_transacao"] / df_valid["media_area_terreno"]
        )
    else:
        df_valid["preco_m2_escritura"] = 0

    # Agrega por bairro
    stats = df_valid.groupby("bairro").agg(
        escritura_valor_medio=("media_valor_transacao", "mean"),
        escritura_valor_mediano=("media_valor_transacao", "median"),
        escritura_preco_m2_medio=("preco_m2_escritura", lambda x: x[x > 0].mean() if (x > 0).any() else 0),
        escritura_total_transacoes=("total_transacoes" if "total_transacoes" in df_valid.columns else "media_valor_transacao", "count"),
        escritura_ano_mais_recente=("ano_transacao", "max"),
        escritura_ano_mais_antigo=("ano_transacao", "min"),
    ).reset_index()

    # Tendência: compara últimos 3 anos vs anteriores
    if "ano_transacao" in df_valid.columns:
        ano_max = df_valid["ano_transacao"].max()
        recente = df_valid[df_valid["ano_transacao"] >= ano_max - 3]
        antigo = df_valid[df_valid["ano_transacao"] < ano_max - 3]

        if not recente.empty and not antigo.empty:
            media_recente = recente.groupby("bairro")["media_valor_transacao"].mean()
            media_antigo = antigo.groupby("bairro")["media_valor_transacao"].mean()
            tendencia = ((media_recente - media_antigo) / media_antigo * 100).round(1)
            tendencia_df = tendencia.reset_index()
            tendencia_df.columns = ["bairro", "escritura_valorizacao_pct"]
            stats = pd.merge(stats, tendencia_df, on="bairro", how="left")

    stats = stats.round(2)
    return stats


# ==============================================================================
#  3. MODELO DE PREÇO JUSTO (Regressão)
# ==============================================================================

def _treinar_modelo_preco(df: pd.DataFrame) -> dict:
    """
    Treina modelo para estimar preço justo.
    Se ia/config_modelo.json existir, usa features e modelo recomendados pela análise.
    Senão, auto-descobre features.
    """
    try:
        from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
        from sklearn.model_selection import cross_val_score
        from sklearn.preprocessing import LabelEncoder
    except ImportError:
        logger.warning("   ⚠️ scikit-learn não instalado. pip install scikit-learn")
        return None

    # --- Tenta carregar config da análise de features ---
    config_path = Path("ia/config_modelo.json")
    usar_config = False
    config = {}

    if config_path.exists():
        try:
            import json
            config = json.load(open(config_path, encoding="utf-8"))
            if config.get("features_recomendadas"):
                usar_config = True
                logger.info(f"   📋 Usando config da análise: {config['melhor_modelo']} com {config['n_features']} features")
        except Exception:
            pass

    if usar_config:
        feature_cols = [c for c in config["features_recomendadas"]
                        if c in df.columns and not c.endswith("_encoded")]
    else:
        # Auto-descoberta de features
        EXCLUIR = {
            "valor_R$", "preco_m2", "url", "descricao", "imagem_url", "corretora",
            "endereco_completo", "coordenadas", "publicacao_texto", "resumo_financeiro",
            "nota_media", "total_avaliacoes", "anuncio_criado", "origem_geo",
            "tipo_negocio", "rua", "numero", "cidade", "uf",
            "preco_estimado_ml", "delta_preco_ml", "delta_preco_ml_pct",
            "oportunidade_ml", "ml_r2_score",
            "preco_referencia", "diferenca_percentual", "diferenca_absoluta",
            "preco_m2_real", "media_rua_m2", "media_bairro_m2", "preco_m2_referencia",
            "segmento_preco", "segmento_area",
            "custo_aquisicao_total", "itbi_estimado", "cartorio_estimado",
            "aluguel_estimado", "fluxo_caixa_mensal", "yield_bruto_anual",
            "anos_payback", "custo_fixo_mensal",
            "mcmv_entrada", "mcmv_financiado", "mcmv_sac_1a", "mcmv_sac_ult",
            "mcmv_sac_juros_total", "mcmv_price_parcela", "mcmv_price_juros_total",
            "mcmv_renda_min_sac", "mcmv_renda_min_price", "mcmv_economia",
            "merc_entrada", "merc_financiado", "merc_sac_1a", "merc_sac_ult",
            "merc_sac_juros_total", "merc_price_parcela", "merc_price_juros_total",
            "merc_renda_min_sac", "merc_renda_min_price",
            "merc_juros_aa", "merc_teto", "merc_elegivel", "merc_faixa",
            "mcmv_faixa", "mcmv_elegivel", "mcmv_teto", "mcmv_juros_aa", "mcmv_subsidio",
            "escritura_valor_medio", "escritura_valor_mediano",
            "escritura_preco_m2_medio", "delta_escritura_pct",
            "perfil_familia", "perfil_investidor", "perfil_primeiro_imovel",
            "score_investimento",
            # Modelos avançados (gerados depois)
            "preco_mlp", "delta_mlp_pct", "cluster_id", "anomalia_preco", "anomalia_score",
        }
        feature_cols = []
        for col in df.columns:
            if col in EXCLUIR:
                continue
            if col == "valor_R$":
                continue
            if df[col].dtype in ("float64", "int64", "int32", "float32"):
                if df[col].nunique() > 1:
                    feature_cols.append(col)

    if len(feature_cols) < 5:
        logger.warning("   ⚠️ Features insuficientes para treinar modelo.")
        return None

    # Prepara dados
    target = pd.to_numeric(df["valor_R$"], errors="coerce")
    X = df[feature_cols].copy()
    for col in X.columns:
        X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0)

    # Adiciona bairro encoded
    le = None
    if "bairro" in df.columns:
        le = LabelEncoder()
        X["bairro_encoded"] = le.fit_transform(df["bairro"].astype(str))
        feature_cols.append("bairro_encoded")

    # Adiciona zona encoded
    if "zona" in df.columns:
        le_zona = LabelEncoder()
        X["zona_encoded"] = le_zona.fit_transform(df["zona"].astype(str))
        feature_cols.append("zona_encoded")

    # Remove nulos do target
    mask = target > 0
    X = X[mask]
    y = target[mask]

    if len(X) < 20:
        logger.warning(f"   ⚠️ Apenas {len(X)} amostras — modelo pode ser impreciso.")

    logger.info(f"   Features selecionadas: {len(feature_cols)} "
                f"(tem_*: {sum(1 for c in feature_cols if c.startswith('tem_'))}, "
                f"tag_*: {sum(1 for c in feature_cols if c.startswith('tag_'))}, "
                f"dist_*: {sum(1 for c in feature_cols if c.startswith('dist_'))}, "
                f"score_*: {sum(1 for c in feature_cols if c.startswith('score_'))})")

    # Treina modelo (usa config se disponível)
    if usar_config and config.get("melhor_modelo") == "RandomForest":
        params = config.get("hiperparametros", {})
        model = RandomForestRegressor(
            n_estimators=int(params.get("n_estimators", 200)),
            max_depth=None if params.get("max_depth") == "None" else int(params.get("max_depth", 6)),
            min_samples_leaf=int(params.get("min_samples_leaf", 2)),
            max_features=None if params.get("max_features") == "None" else params.get("max_features", "sqrt"),
            random_state=42,
        )
    else:
        model = GradientBoostingRegressor(
            n_estimators=200, max_depth=4, learning_rate=0.05,
            min_samples_leaf=3, subsample=0.8, random_state=42,
        )
    model.fit(X, y)

    # Cross-validation
    if len(X) >= 30:
        scores = cross_val_score(model, X, y, cv=5, scoring="r2")
        r2 = scores.mean()
    else:
        r2 = model.score(X, y)

    logger.info(f"   Modelo treinado: R²={r2:.3f}, features={len(feature_cols)}, amostras={len(X)}")

    # Predições
    pred = model.predict(X)

    return {
        "model": model,
        "features": feature_cols,
        "r2": r2,
        "predictions": pred,
        "mask": mask,
        "le": le if "bairro_encoded" in feature_cols else None,
    }


# ==============================================================================
#  4. PIPELINE PRINCIPAL
# ==============================================================================

def enriquecer_com_escrituras(df: pd.DataFrame,
                              caminho_escrituras: str = ESCRITURAS_DEFAULT) -> pd.DataFrame:
    """
    Enriquece dataset com dados de escritura da prefeitura e modelo de preço.
    
    Colunas geradas:
        Escrituras:
            escritura_valor_medio, escritura_preco_m2_medio,
            escritura_valorizacao_pct, escritura_total_transacoes
        
        Modelo ML:
            preco_estimado_ml, delta_preco_ml, delta_preco_ml_pct,
            oportunidade_ml (flag: preço pedido < estimado em >15%)
        
        Yield real:
            yield_estimado_bairro (baseado em dados de escritura)
    """
    logger.info("🤖 Enriquecimento com dados de escritura e ML...")

    # --- 1. ESCRITURAS ---
    df_esc = _carregar_escrituras(caminho_escrituras)
    if not df_esc.empty:
        logger.info(f"   Escrituras carregadas: {len(df_esc)} registros")
        stats = _calcular_stats_bairro(df_esc)

        if not stats.empty:
            # Remove colunas de escritura anteriores (se re-executando)
            cols_esc = [c for c in df.columns if c.startswith("escritura_") or c == "delta_escritura_pct"]
            if cols_esc:
                df.drop(columns=cols_esc, inplace=True, errors="ignore")

            # Merge por bairro
            df = pd.merge(df, stats, on="bairro", how="left")

            # Delta: preço anúncio vs escritura
            if "escritura_valor_medio" in df.columns:
                esc_val = pd.to_numeric(df["escritura_valor_medio"], errors="coerce").fillna(0)
                valor = pd.to_numeric(df["valor_R$"], errors="coerce").fillna(0)
                df["delta_escritura_pct"] = np.where(
                    esc_val > 0,
                    np.round(((valor - esc_val) / esc_val) * 100, 1),
                    0
                )
                logger.info(f"   Bairros com dados de escritura: {(esc_val > 0).sum()}/{len(df)}")
            else:
                logger.warning("   ⚠️ Coluna escritura_valor_medio não gerada pelo merge.")
    else:
        logger.info(f"   Arquivo '{caminho_escrituras}' não encontrado — pulando escrituras.")
        logger.info(f"   Dica: coloque o CSV da prefeitura com nome '{caminho_escrituras}' na pasta.")

    # --- 2. MODELO DE PREÇO ---
    # Remove colunas ML de execuções anteriores
    cols_ml = [c for c in df.columns if c.startswith(("preco_estimado_ml", "delta_preco_ml", "oportunidade_ml", "ml_r2"))]
    if cols_ml:
        df.drop(columns=cols_ml, inplace=True, errors="ignore")

    resultado_ml = _treinar_modelo_preco(df)
    if resultado_ml:
        model = resultado_ml["model"]
        features = resultado_ml["features"]
        mask = resultado_ml["mask"]
        le = resultado_ml["le"]

        # Prepara X para predição em TODOS os registros
        # Separa features reais (do df) de encoded (geradas)
        encoded_cols = [c for c in features if c.endswith("_encoded")]
        df_cols = [c for c in features if c not in encoded_cols]

        X_all = df[df_cols].copy()
        for col in X_all.columns:
            X_all[col] = pd.to_numeric(X_all[col], errors="coerce").fillna(0)

        # Adiciona colunas encoded
        if "bairro_encoded" in encoded_cols and le:
            bairros = df["bairro"].astype(str)
            encoded = []
            for b in bairros:
                try:
                    encoded.append(le.transform([b])[0])
                except ValueError:
                    encoded.append(-1)
            X_all["bairro_encoded"] = encoded

        if "zona_encoded" in encoded_cols and "zona" in df.columns:
            from sklearn.preprocessing import LabelEncoder
            le_z = LabelEncoder()
            le_z.fit(df["zona"].astype(str))
            X_all["zona_encoded"] = le_z.transform(df["zona"].astype(str))

        pred_all = model.predict(X_all)
        valor = pd.to_numeric(df["valor_R$"], errors="coerce").fillna(0).values

        df["preco_estimado_ml"] = np.round(pred_all, 0)
        df["delta_preco_ml"] = np.round(valor - pred_all, 0)
        df["delta_preco_ml_pct"] = np.where(
            pred_all > 0,
            np.round(((valor - pred_all) / pred_all) * 100, 1),
            0
        )

        # Flag de oportunidade: preço pedido < estimado em mais de 15%
        df["oportunidade_ml"] = np.where(df["delta_preco_ml_pct"] < -15, 1, 0)
        df["ml_r2_score"] = round(resultado_ml["r2"], 3)

        n_oportunidades = df["oportunidade_ml"].sum()
        logger.info(f"   Oportunidades detectadas (>15% abaixo estimado): {n_oportunidades}/{len(df)}")

        # Feature importance
        if hasattr(model, "feature_importances_"):
            importances = sorted(
                zip(features, model.feature_importances_),
                key=lambda x: x[1], reverse=True,
            )
            top_features = ", ".join(f"{f}({v:.2f})" for f, v in importances[:5])
            logger.info(f"   Top features: {top_features}")

        # Salva modelo e relatório
        _salvar_artefatos(model, features, resultado_ml, df)

    else:
        df["preco_estimado_ml"] = 0
        df["delta_preco_ml"] = 0
        df["delta_preco_ml_pct"] = 0
        df["oportunidade_ml"] = 0
        df["ml_r2_score"] = 0

    return df


# ==============================================================================
#  5. SALVAR ARTEFATOS (modelo + relatório)
# ==============================================================================

def _salvar_artefatos(model, features, resultado_ml, df):
    """Salva modelo treinado, relatório JSON e relatório de oportunidades."""
    import json
    from datetime import datetime

    agora = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Salva modelo com joblib
    try:
        import joblib
        joblib.dump(model, "modelo_preco_imovel.joblib")
        logger.info(f"   Modelo salvo: modelo_preco_imovel.joblib")
    except ImportError:
        logger.warning("   ⚠️ joblib não instalado — modelo não salvo. pip install joblib")

    # Relatório JSON
    r2 = resultado_ml["r2"]
    importances = []
    if hasattr(model, "feature_importances_"):
        importances = [
            {"feature": f, "importance": round(float(v), 4)}
            for f, v in sorted(zip(features, model.feature_importances_),
                               key=lambda x: x[1], reverse=True)
        ]

    n_oportunidades = int(df["oportunidade_ml"].sum()) if "oportunidade_ml" in df.columns else 0

    relatorio = {
        "data_treino": agora,
        "modelo": "GradientBoostingRegressor",
        "amostras": int(resultado_ml["mask"].sum()),
        "features": len(features),
        "r2_score": round(float(r2), 4),
        "oportunidades_detectadas": n_oportunidades,
        "feature_importances": importances[:15],
        "parametros": {
            "n_estimators": 200,
            "max_depth": 4,
            "learning_rate": 0.05,
            "min_samples_leaf": 3,
            "subsample": 0.8,
        },
    }

    with open("ML_REPORT.json", "w", encoding="utf-8") as f:
        json.dump(relatorio, f, indent=2, ensure_ascii=False)
    logger.info(f"   Relatório salvo: ML_REPORT.json")

    # --- RELATÓRIO DE OPORTUNIDADES (Markdown) ---
    if "oportunidade_ml" not in df.columns or n_oportunidades == 0:
        return

    oport = df[df["oportunidade_ml"] == 1].sort_values("delta_preco_ml_pct")
    md = []
    md.append(f"# 🔥 Relatório de Oportunidades — Imóveis Abaixo do Preço Justo\n")
    md.append(f"**Data:** {agora}  ")
    md.append(f"**Modelo:** GradientBoostingRegressor (R²={r2:.3f}, {len(features)} features)  ")
    md.append(f"**Dataset:** {len(df)} imóveis analisados  ")
    md.append(f"**Oportunidades:** {n_oportunidades} imóveis com preço >15% abaixo do estimado\n")

    if r2 < 0:
        md.append(f"> ⚠️ **Atenção:** R² negativo ({r2:.2f}) indica que o modelo ainda não é confiável ")
        md.append(f"> com {len(df)} amostras. Use como indicativo, não como certeza. ")
        md.append(f"> Com 1000+ amostras a precisão melhora significativamente.\n")

    md.append(f"## 📊 Resumo por Zona\n")
    if "zona" in oport.columns:
        for zona, grupo in oport.groupby("zona"):
            md.append(f"- **{zona}**: {len(grupo)} oportunidade(s)")
    md.append("")

    md.append(f"## 🏠 Imóveis Detectados\n")

    for rank, (_, row) in enumerate(oport.iterrows(), 1):
        valor = row["valor_R$"]
        estimado = row["preco_estimado_ml"]
        delta = row["delta_preco_ml_pct"]
        bairro = row.get("bairro", "?")
        zona = row.get("zona", "?")
        area = row.get("area_m2", 0)
        quartos = int(row.get("quartos", 0))
        vagas = int(row.get("vagas", 0))
        tipo = row.get("tipo_imovel", "?")
        andar = row.get("andar_classificacao", "?")
        idade = row.get("idade_estimada", "?")
        faixa = row.get("faixa_preco", "?")
        mcmv = row.get("mcmv_faixa_label", "?")
        url = row.get("url", "")
        condo = row.get("condominio_R$", 0)
        iptu = row.get("iptu_R$", 0)
        conforto = row.get("score_conforto", 0)
        pf = row.get("perfil_familia", 0)
        pi = row.get("perfil_investidor", 0)
        pp = row.get("perfil_primeiro_imovel", 0)
        payback = row.get("anos_payback", 999)
        desc = str(row.get("descricao", ""))[:200]

        # Classificação da oportunidade
        if delta < -40:
            nivel = "🔥🔥🔥 EXCEPCIONAL"
        elif delta < -25:
            nivel = "🔥🔥 FORTE"
        else:
            nivel = "🔥 MODERADA"

        md.append(f"### #{rank} — {bairro} ({zona}) | {nivel}\n")
        md.append(f"| Dado | Valor |")
        md.append(f"|------|-------|")
        md.append(f"| **Preço Pedido** | R$ {valor:,.0f} |")
        md.append(f"| **Preço Estimado (ML)** | R$ {estimado:,.0f} |")
        md.append(f"| **Diferença** | **{delta:+.0f}%** |")
        md.append(f"| Tipo | {tipo} |")
        md.append(f"| Área | {area} m² ({quartos}q, {vagas}v) |")
        md.append(f"| Andar / Idade | {andar} / {idade} |")
        md.append(f"| Faixa | {faixa} |")
        md.append(f"| MCMV | {mcmv} |")
        if condo > 0:
            md.append(f"| Condomínio | R$ {condo:,.0f}/mês |")
        if iptu > 0:
            md.append(f"| IPTU | R$ {iptu:,.0f} |")
        if payback < 900:
            md.append(f"| Payback | {payback:.0f} anos |")
        md.append(f"| Conforto | {conforto}/100 |")
        md.append(f"| Perfil Família | {pf}/100 |")
        md.append(f"| Perfil Investidor | {pi}/100 |")
        md.append(f"| Perfil 1º Imóvel | {pp}/100 |")
        if url:
            md.append(f"| Link | [Ver anúncio]({url}) |")
        md.append(f"\n> {desc}...\n")

    # Top features
    md.append(f"## 🧠 Features Mais Importantes do Modelo\n")
    md.append(f"| Rank | Feature | Importância |")
    md.append(f"|:----:|---------|:-----------:|")
    for i, item in enumerate(importances[:15], 1):
        bar = "█" * int(item["importance"] * 50)
        md.append(f"| {i} | `{item['feature']}` | {item['importance']:.1%} {bar} |")
    md.append("")

    with open("OPORTUNIDADES_REPORT.md", "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    logger.info(f"   📋 Relatório de oportunidades salvo: OPORTUNIDADES_REPORT.md")


# ==============================================================================
#  6. STANDALONE CLI
# ==============================================================================

def main():
    """Execução standalone do módulo de ML."""
    import argparse

    parser = argparse.ArgumentParser(
        description="ML Pricing — Estimativa de Preço Justo via Escrituras + GBM",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--dataset", type=str, default="DATASET_RIO_BI_READY.csv",
        help="CSV do dataset de imóveis (default: DATASET_RIO_BI_READY.csv)",
    )
    parser.add_argument(
        "--escrituras", type=str, default=ESCRITURAS_DEFAULT,
        help=f"CSV de escrituras da prefeitura (default: {ESCRITURAS_DEFAULT})",
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Nome do CSV de saída (default: sobrescreve o dataset)",
    )
    args = parser.parse_args()

    # Lê dataset
    print(f"📄 Lendo {args.dataset}...")
    df = pd.read_csv(args.dataset, sep=";", encoding="utf-8-sig", decimal=",")
    print(f"   ✓ {len(df)} registros, {len(df.columns)} colunas")

    # Enriquece
    df = enriquecer_com_escrituras(df, caminho_escrituras=args.escrituras)

    # Salva
    output = args.output or args.dataset
    df.to_csv(output, sep=";", decimal=",", index=False, encoding="utf-8-sig")
    print(f"\n✓ Dataset salvo: {output} ({len(df.columns)} colunas)")

    # Resumo
    if "oportunidade_ml" in df.columns:
        n = df["oportunidade_ml"].sum()
        print(f"   Oportunidades: {n}/{len(df)} imóveis abaixo do preço justo estimado")
    if "ml_r2_score" in df.columns:
        print(f"   R² do modelo: {df['ml_r2_score'].iloc[0]}")


if __name__ == "__main__":
    main()