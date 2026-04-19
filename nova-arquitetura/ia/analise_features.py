#!/usr/bin/env python3
"""
analise_features.py — Análise completa de features e seleção de modelo.
========================================================================

Executa pipeline de Data Science completo:
  1. Limpeza e preparação (remove data leakage, separa target)
  2. Análise de correlação (matriz + remoção de colineares)
  3. PCA — variância explicada e componentes
  4. Mutual Information — relevância de cada feature ao target
  5. Comparação de modelos (GBM, RandomForest, Ridge, SVR, KNN)
  6. Tuning de hiperparâmetros (GridSearchCV no melhor modelo)
  7. Feature importance final (permutation + built-in)
  8. Exporta modelo treinado + relatório + config reutilizável

Uso:
    python ia/analise_features.py
    python ia/analise_features.py --dataset DATASET_RIO_BI_READY.csv
    python ia/analise_features.py --target valor_R$

Artefatos gerados na pasta ia/:
    ANALISE_FEATURES_REPORT.md   — relatório completo
    melhor_modelo.joblib         — modelo treinado
    config_modelo.json           — features, params, métricas (consumido pelo pipeline)
"""
import os
import sys
import json
import argparse
import warnings
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# Diretório deste script
_DIR = Path(__file__).resolve().parent
_PROJECT = _DIR.parent

# ==============================================================================
#  CONFIGURAÇÃO
# ==============================================================================

# Colunas que NUNCA devem ser features (metadados, URLs, textos, derivadas do preço)
EXCLUIR_SEMPRE = {
    # Metadados
    "url", "descricao", "imagem_url", "corretora", "endereco_completo",
    "coordenadas", "publicacao_texto", "resumo_financeiro", "rua", "numero",
    "cidade", "uf", "nota_media", "total_avaliacoes", "anuncio_criado",
    "origem_geo", "tipo_negocio", "objectid",
    # Target e derivados diretos
    "valor_R$", "preco_m2",
    # Derivados do preço (data leakage)
    "preco_estimado_ml", "delta_preco_ml", "delta_preco_ml_pct",
    "oportunidade_ml", "ml_r2_score",
    "preco_referencia", "diferenca_percentual", "diferenca_absoluta",
    "preco_m2_real", "media_rua_m2", "media_bairro_m2", "preco_m2_referencia",
    "segmento_preco", "segmento_area", "origem_referencia",
    # Financeiras derivadas do preço
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
    # Escrituras (referência de preço)
    "escritura_valor_medio", "escritura_valor_mediano",
    "escritura_preco_m2_medio", "delta_escritura_pct",
    # Perfis compostos (usam scores que já são features)
    "perfil_familia", "perfil_investidor", "perfil_primeiro_imovel",
    "score_investimento",
    # Labels textuais
    "payback_classificacao", "mcmv_faixa_label", "merc_modalidade",
    "perfil_familia_label", "perfil_investidor_label", "perfil_primeiro_imovel_label",
    "faixa_preco", "andar_classificacao", "idade_estimada",
    "vocacao_airbnb", "dom_categoria",
}


def _carregar_dataset(path: str, target: str) -> tuple:
    """Carrega CSV e separa features numéricas do target."""
    df = pd.read_csv(path, sep=";", encoding="utf-8-sig", decimal=",")
    print(f"📄 Dataset: {len(df)} registros, {len(df.columns)} colunas")

    # Target
    y = pd.to_numeric(df[target], errors="coerce")
    mask = y > 0
    y = y[mask]

    # Features: tudo que é numérico, tem variação e não está na lista de exclusão
    feature_cols = []
    for col in df.columns:
        if col in EXCLUIR_SEMPRE:
            continue
        if col in (target,):
            continue
        serie = pd.to_numeric(df[col], errors="coerce")
        if serie.notna().sum() > len(df) * 0.3 and serie.nunique() > 1:
            feature_cols.append(col)

    X = df.loc[mask, feature_cols].copy()
    for col in X.columns:
        X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0)

    # Encode categorias textuais úteis
    from sklearn.preprocessing import LabelEncoder
    encoders = {}
    for col_name, df_col in [("bairro", "bairro"), ("zona", "zona"), ("tipo_imovel", "tipo_imovel")]:
        if df_col in df.columns:
            le = LabelEncoder()
            encoded_name = f"{col_name}_encoded"
            X[encoded_name] = le.fit_transform(df.loc[mask, df_col].astype(str))
            feature_cols.append(encoded_name)
            encoders[col_name] = le

    print(f"   Features candidatas: {len(X.columns)}")
    print(f"   Amostras válidas: {len(X)}")

    return X, y, df.loc[mask], encoders


# ==============================================================================
#  ANÁLISES
# ==============================================================================

def analise_correlacao(X: pd.DataFrame, threshold: float = 0.95) -> dict:
    """Identifica features altamente correlacionadas entre si."""
    print("\n🔗 Análise de Correlação...")
    corr = X.corr().abs()

    # Pares com correlação > threshold
    upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
    pares_altos = []
    to_drop = set()
    for col in upper.columns:
        for idx in upper.index:
            val = upper.loc[idx, col]
            if pd.notna(val) and val > threshold:
                pares_altos.append((idx, col, round(val, 3)))
                to_drop.add(col)  # Remove a segunda do par

    print(f"   Pares com correlação > {threshold}: {len(pares_altos)}")
    if pares_altos:
        for a, b, v in pares_altos[:10]:
            print(f"     {a} ↔ {b}: {v}")
    print(f"   Features a remover por colinearidade: {len(to_drop)}")

    return {"pares": pares_altos, "remover": list(to_drop)}


def analise_pca(X: pd.DataFrame, n_components: int = None) -> dict:
    """PCA — análise de variância explicada e componentes."""
    print("\n📊 Análise PCA...")
    from sklearn.decomposition import PCA
    from sklearn.preprocessing import StandardScaler

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X.fillna(0))

    if n_components is None:
        n_components = min(X.shape[0], X.shape[1])

    pca = PCA(n_components=n_components)
    pca.fit(X_scaled)

    cumvar = np.cumsum(pca.explained_variance_ratio_)

    # Quantos componentes para 80%, 90%, 95%
    for pct in [0.80, 0.90, 0.95]:
        n = np.argmax(cumvar >= pct) + 1
        print(f"   {pct:.0%} variância explicada com {n} componentes (de {X.shape[1]})")

    # Top features por loading no PC1
    loadings_pc1 = pd.Series(np.abs(pca.components_[0]), index=X.columns)
    top_pc1 = loadings_pc1.nlargest(10)

    return {
        "variancia_explicada": pca.explained_variance_ratio_.tolist(),
        "variancia_acumulada": cumvar.tolist(),
        "n_80pct": int(np.argmax(cumvar >= 0.80) + 1),
        "n_90pct": int(np.argmax(cumvar >= 0.90) + 1),
        "n_95pct": int(np.argmax(cumvar >= 0.95) + 1),
        "top_loadings_pc1": {k: round(v, 4) for k, v in top_pc1.items()},
    }


def analise_mutual_information(X: pd.DataFrame, y: pd.Series) -> dict:
    """Mutual Information — relevância não-linear de cada feature."""
    print("\n🧠 Mutual Information (relevância feature → target)...")
    from sklearn.feature_selection import mutual_info_regression

    mi = mutual_info_regression(X.fillna(0), y, random_state=42, n_neighbors=5)
    mi_series = pd.Series(mi, index=X.columns).sort_values(ascending=False)

    top = mi_series.head(20)
    print(f"   Top 10 features por Mutual Information:")
    for feat, score in top.head(10).items():
        print(f"     {feat:35s} MI={score:.4f}")

    zero_mi = (mi_series == 0).sum()
    print(f"   Features com MI=0 (irrelevantes): {zero_mi}/{len(mi_series)}")

    return {
        "scores": {k: round(float(v), 4) for k, v in mi_series.items()},
        "top_20": list(top.index),
        "irrelevantes": list(mi_series[mi_series == 0].index),
    }


def comparar_modelos(X: pd.DataFrame, y: pd.Series) -> dict:
    """Compara múltiplos algoritmos via cross-validation."""
    print("\n🏆 Comparação de Modelos (5-fold CV)...")
    from sklearn.model_selection import cross_val_score
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import make_pipeline
    from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
    from sklearn.linear_model import Ridge, Lasso
    from sklearn.neighbors import KNeighborsRegressor

    modelos = {
        "GradientBoosting": GradientBoostingRegressor(
            n_estimators=200, max_depth=4, learning_rate=0.05,
            min_samples_leaf=3, subsample=0.8, random_state=42,
        ),
        "RandomForest": RandomForestRegressor(
            n_estimators=200, max_depth=6, min_samples_leaf=3, random_state=42,
        ),
        "Ridge": make_pipeline(StandardScaler(), Ridge(alpha=1.0)),
        "Lasso": make_pipeline(StandardScaler(), Lasso(alpha=1000, max_iter=5000)),
        "KNN": make_pipeline(StandardScaler(), KNeighborsRegressor(n_neighbors=5)),
    }

    resultados = {}
    melhor_r2 = -999
    melhor_nome = ""

    for nome, modelo in modelos.items():
        cv = min(5, len(X))
        scores = cross_val_score(modelo, X.fillna(0), y, cv=cv, scoring="r2")
        r2_mean = scores.mean()
        r2_std = scores.std()
        mae_scores = cross_val_score(modelo, X.fillna(0), y, cv=cv, scoring="neg_mean_absolute_error")
        mae_mean = -mae_scores.mean()

        resultados[nome] = {
            "r2_mean": round(float(r2_mean), 4),
            "r2_std": round(float(r2_std), 4),
            "mae_mean": round(float(mae_mean), 0),
        }

        status = "👑" if r2_mean > melhor_r2 else "  "
        print(f"   {status} {nome:25s} R²={r2_mean:+.4f} ±{r2_std:.4f}  MAE=R${mae_mean:>12,.0f}")

        if r2_mean > melhor_r2:
            melhor_r2 = r2_mean
            melhor_nome = nome

    print(f"\n   🏆 Melhor modelo: {melhor_nome} (R²={melhor_r2:.4f})")

    return {"resultados": resultados, "melhor": melhor_nome, "modelos": modelos}


def tuning_melhor_modelo(X: pd.DataFrame, y: pd.Series, nome: str, modelos: dict) -> dict:
    """GridSearchCV no melhor modelo para otimizar hiperparâmetros."""
    print(f"\n⚙️ Tuning de hiperparâmetros ({nome})...")
    from sklearn.model_selection import GridSearchCV

    grids = {
        "GradientBoosting": {
            "n_estimators": [100, 200, 300],
            "max_depth": [3, 4, 5],
            "learning_rate": [0.01, 0.05, 0.1],
            "min_samples_leaf": [2, 3, 5],
            "subsample": [0.8, 1.0],
        },
        "RandomForest": {
            "n_estimators": [100, 200, 300],
            "max_depth": [4, 6, 8, None],
            "min_samples_leaf": [2, 3, 5],
            "max_features": ["sqrt", "log2", None],
        },
        "KNN": {
            "kneighborsregressor__n_neighbors": [3, 5, 7, 9, 11, 15],
            "kneighborsregressor__weights": ["uniform", "distance"],
            "kneighborsregressor__metric": ["euclidean", "manhattan"],
        },
    }

    if nome not in grids:
        print(f"   Tuning não disponível para {nome}. Usando modelo padrão.")
        modelo_final = modelos[nome]
        modelo_final.fit(X.fillna(0), y)
        return {"modelo": modelo_final, "params": {}, "best_score": 0}

    grid = GridSearchCV(
        modelos[nome], grids[nome], cv=min(5, len(X)),
        scoring="r2", n_jobs=-1, verbose=0,
    )
    grid.fit(X.fillna(0), y)

    print(f"   Melhor R² (CV): {grid.best_score_:.4f}")
    print(f"   Parâmetros: {grid.best_params_}")

    return {
        "modelo": grid.best_estimator_,
        "params": grid.best_params_,
        "best_score": round(float(grid.best_score_), 4),
    }


def analise_feature_importance(modelo, X: pd.DataFrame, y: pd.Series) -> dict:
    """Feature importance via permutation + built-in."""
    print("\n📈 Feature Importance Final...")
    from sklearn.inspection import permutation_importance

    # Built-in (se disponível)
    builtin = {}
    if hasattr(modelo, "feature_importances_"):
        imp = pd.Series(modelo.feature_importances_, index=X.columns).sort_values(ascending=False)
        builtin = {k: round(float(v), 4) for k, v in imp.items()}
        print(f"   Built-in (top 10):")
        for feat, score in list(imp.items())[:10]:
            bar = "█" * int(score * 80)
            print(f"     {feat:35s} {score:.3f} {bar}")

    # Permutation importance (mais robusto)
    perm = permutation_importance(modelo, X.fillna(0), y, n_repeats=10, random_state=42)
    perm_series = pd.Series(perm.importances_mean, index=X.columns).sort_values(ascending=False)
    perm_dict = {k: round(float(v), 4) for k, v in perm_series.items()}

    positivas = (perm_series > 0).sum()
    print(f"\n   Permutation (top 10):")
    for feat, score in list(perm_series.items())[:10]:
        print(f"     {feat:35s} {score:.4f}")
    print(f"   Features com importância positiva: {positivas}/{len(perm_series)}")

    # Features recomendadas: MI > 0 AND (permutation > 0 OR builtin > 0.01)
    recomendadas = [c for c in X.columns if perm_series.get(c, 0) > 0]

    return {
        "builtin": builtin,
        "permutation": perm_dict,
        "recomendadas": recomendadas,
        "n_recomendadas": len(recomendadas),
    }


# ==============================================================================
#  RELATÓRIO MARKDOWN
# ==============================================================================

def gerar_relatorio(resultados: dict, output_dir: Path) -> str:
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
    md = []

    md.append(f"# 🧪 Análise de Features e Seleção de Modelo\n")
    md.append(f"**Data:** {agora}  ")
    md.append(f"**Dataset:** {resultados['n_amostras']} amostras, {resultados['n_features_input']} features candidatas  ")
    md.append(f"**Target:** `{resultados['target']}`\n")
    md.append(f"![Distribuição de Preços](graficos/distribuicao_precos.png)\n")

    # PCA
    pca = resultados["pca"]
    md.append(f"## 📊 PCA (Análise de Componentes Principais)\n")
    md.append(f"![PCA Variância](graficos/pca_variancia.png)\n")
    md.append(f"- 80% da variância com **{pca['n_80pct']}** componentes (de {resultados['n_features_input']})")
    md.append(f"- 90% da variância com **{pca['n_90pct']}** componentes")
    md.append(f"- 95% da variância com **{pca['n_95pct']}** componentes\n")
    md.append(f"**Top loadings PC1:**\n")
    for feat, load in pca["top_loadings_pc1"].items():
        md.append(f"- `{feat}`: {load:.4f}")
    md.append("")

    # Correlação
    corr = resultados["correlacao"]
    md.append(f"## 🔗 Correlação\n")
    md.append(f"![Correlação Heatmap](graficos/correlacao_heatmap.png)\n")
    md.append(f"- Pares com correlação > 0.95: **{len(corr['pares'])}**")
    md.append(f"- Features removidas por colinearidade: **{len(corr['remover'])}**\n")
    if corr["pares"]:
        md.append(f"| Feature A | Feature B | Correlação |")
        md.append(f"|-----------|-----------|:----------:|")
        for a, b, v in corr["pares"][:15]:
            md.append(f"| `{a}` | `{b}` | {v} |")
    md.append("")

    # Mutual Information
    mi = resultados["mutual_info"]
    md.append(f"## 🧠 Mutual Information\n")
    md.append(f"![MI Top 20](graficos/mutual_information.png)\n")
    md.append(f"| Rank | Feature | MI Score |")
    md.append(f"|:----:|---------|:--------:|")
    for i, (feat, score) in enumerate(list(mi["scores"].items())[:20], 1):
        bar = "█" * int(score * 20)
        md.append(f"| {i} | `{feat}` | {score:.4f} {bar} |")
    md.append(f"\nFeatures irrelevantes (MI=0): **{len(mi['irrelevantes'])}**\n")

    # Comparação de modelos
    comp = resultados["comparacao"]
    md.append(f"## 🏆 Comparação de Modelos\n")
    md.append(f"![Comparação](graficos/comparacao_modelos.png)\n")
    md.append(f"| Modelo | R² (CV) | ± Std | MAE |")
    md.append(f"|--------|:-------:|:-----:|----:|")
    for nome, r in comp["resultados"].items():
        crown = " 👑" if nome == comp["melhor"] else ""
        md.append(f"| **{nome}**{crown} | {r['r2_mean']:+.4f} | {r['r2_std']:.4f} | R${r['mae_mean']:>12,.0f} |")
    md.append(f"\n**Melhor modelo:** {comp['melhor']}\n")

    # Tuning
    tuning = resultados["tuning"]
    md.append(f"## ⚙️ Hiperparâmetros Otimizados\n")
    md.append(f"**R² após tuning:** {tuning['best_score']:.4f}\n")
    if tuning["params"]:
        md.append(f"| Parâmetro | Valor |")
        md.append(f"|-----------|-------|")
        for k, v in tuning["params"].items():
            md.append(f"| `{k}` | `{v}` |")
    md.append("")

    # Feature Importance Final
    fi = resultados["feature_importance"]
    md.append(f"## 📈 Feature Importance Final\n")
    md.append(f"![Feature Importance](graficos/feature_importance.png)\n")
    md.append(f"**Features recomendadas:** {fi['n_recomendadas']}/{resultados['n_features_input']}\n")

    if fi["builtin"]:
        md.append(f"### Built-in Importance\n")
        md.append(f"| Rank | Feature | Importância |")
        md.append(f"|:----:|---------|:-----------:|")
        for i, (feat, score) in enumerate(list(fi["builtin"].items())[:20], 1):
            bar = "█" * int(score * 50)
            md.append(f"| {i} | `{feat}` | {score:.3f} {bar} |")
    md.append("")

    if fi["permutation"]:
        md.append(f"### Permutation Importance (mais robusto)\n")
        md.append(f"| Rank | Feature | Importância |")
        md.append(f"|:----:|---------|:-----------:|")
        positivas = [(f, s) for f, s in fi["permutation"].items() if s > 0]
        for i, (feat, score) in enumerate(positivas[:20], 1):
            md.append(f"| {i} | `{feat}` | {score:.4f} |")
    md.append("")

    # Recomendação
    md.append(f"## ✅ Configuração Recomendada\n")
    md.append(f"- **Modelo:** {comp['melhor']}")
    md.append(f"- **Features:** {fi['n_recomendadas']} (de {resultados['n_features_input']})")
    md.append(f"- **R² esperado:** {tuning['best_score']:.4f}")
    md.append(f"- **Arquivo de config:** `ia/config_modelo.json`")
    md.append(f"- **Modelo salvo:** `ia/melhor_modelo.joblib`\n")

    if resultados["n_amostras"] < 200:
        md.append(f"> ⚠️ **Atenção:** {resultados['n_amostras']} amostras é insuficiente para ")
        md.append(f"> resultados confiáveis. Execute com 1000+ registros para validação robusta.")

    return "\n".join(md)


def _gerar_graficos(pca_result, mi_result, comp_result, fi_result, X, y, output_dir):
    """Gera gráficos PNG para o relatório."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("   ⚠️ matplotlib não instalado. pip install matplotlib")
        return

    os.makedirs(output_dir, exist_ok=True)

    # 1. PCA Variância Acumulada
    fig, ax = plt.subplots(figsize=(10, 5))
    cumvar = pca_result["variancia_acumulada"][:min(50, len(pca_result["variancia_acumulada"]))]
    ax.plot(range(1, len(cumvar)+1), cumvar, "b-o", markersize=3)
    ax.axhline(0.80, color="orange", linestyle="--", label="80%")
    ax.axhline(0.90, color="red", linestyle="--", label="90%")
    ax.axhline(0.95, color="darkred", linestyle="--", label="95%")
    ax.fill_between(range(1, len(cumvar)+1), cumvar, alpha=0.1, color="blue")
    ax.set_xlabel("Nº de Componentes"); ax.set_ylabel("Variância Acumulada")
    ax.set_title("PCA — Variância Explicada Acumulada"); ax.legend(); ax.grid(True, alpha=0.3)
    plt.tight_layout(); plt.savefig(f"{output_dir}/pca_variancia.png", dpi=150); plt.close()

    # 2. Mutual Information Top 20
    mi_top = dict(list(mi_result["scores"].items())[:20])
    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(list(reversed(mi_top.keys())), list(reversed(mi_top.values())), color="steelblue")
    ax.set_xlabel("MI Score"); ax.set_title("Mutual Information — Top 20 Features")
    for bar, val in zip(bars, reversed(list(mi_top.values()))):
        ax.text(bar.get_width() + 0.005, bar.get_y() + bar.get_height()/2, f"{val:.3f}",
                va="center", fontsize=8)
    ax.grid(True, alpha=0.3, axis="x")
    plt.tight_layout(); plt.savefig(f"{output_dir}/mutual_information.png", dpi=150); plt.close()

    # 3. Comparação de Modelos
    nomes = list(comp_result["resultados"].keys())
    r2s = [comp_result["resultados"][n]["r2_mean"] for n in nomes]
    maes = [comp_result["resultados"][n]["mae_mean"] for n in nomes]
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    colors = ["#4CAF50" if n == comp_result["melhor"] else "#2196F3" for n in nomes]
    ax1.barh(nomes, r2s, color=colors); ax1.set_xlabel("R² (CV)"); ax1.set_title("R² por Modelo")
    for i, v in enumerate(r2s):
        ax1.text(v + 0.01, i, f"{v:.3f}", va="center", fontsize=9)
    ax1.grid(True, alpha=0.3, axis="x")
    ax2.barh(nomes, maes, color=colors); ax2.set_xlabel("MAE"); ax2.set_title("MAE por Modelo")
    ax2.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"R${x/1e3:.0f}k"))
    ax2.grid(True, alpha=0.3, axis="x")
    plt.tight_layout(); plt.savefig(f"{output_dir}/comparacao_modelos.png", dpi=150); plt.close()

    # 4. Feature Importance Top 20
    if fi_result["builtin"]:
        fi_top = dict(list(fi_result["builtin"].items())[:20])
        fig, ax = plt.subplots(figsize=(10, 7))
        bars = ax.barh(list(reversed(fi_top.keys())), list(reversed(fi_top.values())), color="darkorange")
        ax.set_xlabel("Importância"); ax.set_title("Feature Importance (Built-in) — Top 20")
        ax.grid(True, alpha=0.3, axis="x")
        plt.tight_layout(); plt.savefig(f"{output_dir}/feature_importance.png", dpi=150); plt.close()

    # 5. Matriz de Correlação (top 15 features)
    if fi_result["builtin"]:
        top_feats = list(fi_result["builtin"].keys())[:15]
        existing = [c for c in top_feats if c in X.columns]
        if len(existing) >= 5:
            corr_matrix = X[existing].corr()
            fig, ax = plt.subplots(figsize=(12, 10))
            im = ax.imshow(corr_matrix.values, cmap="RdBu_r", vmin=-1, vmax=1)
            ax.set_xticks(range(len(existing))); ax.set_yticks(range(len(existing)))
            ax.set_xticklabels(existing, rotation=45, ha="right", fontsize=8)
            ax.set_yticklabels(existing, fontsize=8)
            for i in range(len(existing)):
                for j in range(len(existing)):
                    val = corr_matrix.values[i, j]
                    ax.text(j, i, f"{val:.2f}", ha="center", va="center",
                           fontsize=7, color="white" if abs(val) > 0.5 else "black")
            ax.set_title("Correlação entre Top 15 Features")
            plt.colorbar(im, ax=ax, shrink=0.8)
            plt.tight_layout(); plt.savefig(f"{output_dir}/correlacao_heatmap.png", dpi=150); plt.close()

    # 6. Distribuição do Target (preço)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    ax1.hist(y, bins=50, color="steelblue", edgecolor="white", alpha=0.8)
    ax1.set_xlabel("Preço (R$)"); ax1.set_ylabel("Frequência")
    ax1.set_title("Distribuição de Preços"); ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"R${x/1e6:.1f}M"))

    # Log scale
    ax2.hist(np.log10(y[y > 0]), bins=50, color="coral", edgecolor="white", alpha=0.8)
    ax2.set_xlabel("log₁₀(Preço)"); ax2.set_ylabel("Frequência")
    ax2.set_title("Distribuição de Preços (log)"); ax2.grid(True, alpha=0.3)
    plt.tight_layout(); plt.savefig(f"{output_dir}/distribuicao_precos.png", dpi=150); plt.close()

    print(f"   📊 6 gráficos salvos em {output_dir}/")


# ==============================================================================
#  MAIN
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Análise de Features e Seleção de Modelo",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--dataset", default=str(_PROJECT / "DATASET_RIO_BI_READY.csv"),
                        help="Caminho do dataset CSV")
    parser.add_argument("--target", default="valor_R$",
                        help="Coluna target (default: valor_R$)")
    args = parser.parse_args()

    print("=" * 60)
    print("  ANÁLISE DE FEATURES E SELEÇÃO DE MODELO")
    print("=" * 60)

    # 1. Carregar
    X, y, df_valid, encoders = _carregar_dataset(args.dataset, args.target)
    n_features_input = len(X.columns)

    # 2. Correlação
    corr_result = analise_correlacao(X)

    # Remover colineares
    X_clean = X.drop(columns=corr_result["remover"], errors="ignore")
    print(f"   Features após remoção de colineares: {len(X_clean.columns)}")

    # 3. PCA
    pca_result = analise_pca(X_clean)

    # 4. Mutual Information
    mi_result = analise_mutual_information(X_clean, y)

    # 5. Comparação de modelos
    comp_result = comparar_modelos(X_clean, y)

    # 6. Tuning
    tuning_result = tuning_melhor_modelo(
        X_clean, y, comp_result["melhor"], comp_result["modelos"]
    )

    # 7. Feature importance
    fi_result = analise_feature_importance(tuning_result["modelo"], X_clean, y)

    # 7.5 Gráficos
    _gerar_graficos(pca_result, mi_result, comp_result, fi_result, X_clean, y, str(_DIR / "graficos"))

    # 8. Gerar resultados
    resultados = {
        "target": args.target,
        "n_amostras": len(X),
        "n_features_input": n_features_input,
        "correlacao": corr_result,
        "pca": pca_result,
        "mutual_info": mi_result,
        "comparacao": comp_result,
        "tuning": tuning_result,
        "feature_importance": fi_result,
    }

    # 9. Salvar modelo
    try:
        import joblib
        model_path = _DIR / "melhor_modelo.joblib"
        joblib.dump(tuning_result["modelo"], model_path)
        print(f"\n💾 Modelo salvo: {model_path}")
    except ImportError:
        print("⚠️ joblib não instalado")

    # 10. Salvar config JSON (consumido pelo ml_pricing.py)
    config = {
        "data_analise": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "target": args.target,
        "n_amostras": len(X),
        "melhor_modelo": comp_result["melhor"],
        "melhor_r2": tuning_result["best_score"],
        "hiperparametros": {k: (v if not isinstance(v, type(None)) else "None")
                           for k, v in tuning_result["params"].items()},
        "features_recomendadas": fi_result["recomendadas"],
        "n_features": fi_result["n_recomendadas"],
        "features_removidas_colinearidade": corr_result["remover"],
        "features_irrelevantes_mi": mi_result["irrelevantes"],
        "pca_n_90pct": pca_result["n_90pct"],
        "mutual_info_top20": mi_result["top_20"],
        "feature_importances": fi_result["builtin"],
        "comparacao_modelos": {k: v for k, v in comp_result["resultados"].items()},
    }

    config_path = _DIR / "config_modelo.json"
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    print(f"📋 Config salva: {config_path}")

    # 11. Relatório Markdown
    relatorio = gerar_relatorio(resultados, _DIR)
    report_path = _DIR / "ANALISE_FEATURES_REPORT.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(relatorio)
    print(f"📄 Relatório salvo: {report_path}")

    print("\n" + "=" * 60)
    print(f"  ✅ ANÁLISE CONCLUÍDA")
    print(f"  Modelo: {comp_result['melhor']} (R²={tuning_result['best_score']:.4f})")
    print(f"  Features: {fi_result['n_recomendadas']}/{n_features_input}")
    print("=" * 60)


if __name__ == "__main__":
    main()