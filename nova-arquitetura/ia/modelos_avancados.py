"""
modelos_avancados.py — Clustering, Detecção de Anomalias e Rede Neural.
=========================================================================

Integrado ao pipeline, gera colunas novas no dataset:
  - cluster_id, cluster_label        (K-Means)
  - anomalia_preco, anomalia_score   (Isolation Forest)
  - preco_mlp, delta_mlp_pct         (Rede Neural MLP)

Uso standalone:
    python ia/modelos_avancados.py --dataset DATASET_RIO_BI_READY.csv
"""
import os as _os, sys as _sys
from pathlib import Path

_DIR = Path(__file__).resolve().parent
_PROJECT = _DIR.parent
if str(_PROJECT) not in _sys.path:
    _sys.path.insert(0, str(_PROJECT))

import numpy as np
import pandas as pd
import json
import warnings
warnings.filterwarnings("ignore")

from config import logger

# Features usadas nos modelos (sem data leakage)
FEATURES_MODELO = [
    "area_m2", "quartos", "vagas", "banheiros", "suites", "andar",
    "condominio_R$", "iptu_R$", "latitude", "longitude",
    "dist_praia_km", "dist_transporte_km", "dist_shopping_km",
    "score_mobilidade", "score_seguranca", "score_lifestyle", "score_conforto",
]
FEATURES_BOOL = [c for c in [
    "tem_portaria_24h", "tem_elevador", "tem_piscina", "tem_churrasqueira",
    "tem_academia", "tem_condominio_fechado", "tem_varanda", "tem_playground",
]]
FEATURES_TAG = [c for c in [
    "tag_reformado", "tag_mobiliado", "tag_gourmet", "tag_vista_mar",
    "tag_silencioso", "tag_coworking", "tag_exclusivo",
]]


def _preparar_X(df: pd.DataFrame) -> pd.DataFrame:
    """Prepara matrix de features normalizadas."""
    from sklearn.preprocessing import StandardScaler, LabelEncoder

    cols = []
    for c in FEATURES_MODELO + FEATURES_BOOL + FEATURES_TAG:
        if c in df.columns:
            cols.append(c)

    X = df[cols].copy()
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce").fillna(0)

    # Encode categorias
    if "bairro" in df.columns:
        le = LabelEncoder()
        X["bairro_enc"] = le.fit_transform(df["bairro"].astype(str))
    if "zona" in df.columns:
        le2 = LabelEncoder()
        X["zona_enc"] = le2.fit_transform(df["zona"].astype(str))
    if "tipo_imovel" in df.columns:
        le3 = LabelEncoder()
        X["tipo_enc"] = le3.fit_transform(df["tipo_imovel"].astype(str))

    return X


# ==============================================================================
#  1. CLUSTERING (K-Means)
# ==============================================================================

def _clustering(df: pd.DataFrame, X: pd.DataFrame) -> pd.DataFrame:
    """Segmentação automática de imóveis via K-Means."""
    logger.info("🔮 Clustering K-Means...")
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X.fillna(0))

    # Determina K ótimo via inertia (elbow simplificado)
    max_k = min(10, len(df) // 20)
    if max_k < 3:
        max_k = 3

    best_k = 5  # default
    if len(df) >= 100:
        inertias = []
        for k in range(2, max_k + 1):
            km = KMeans(n_clusters=k, random_state=42, n_init=10)
            km.fit(X_scaled)
            inertias.append(km.inertia_)

        # Elbow: maior queda relativa
        diffs = [inertias[i] - inertias[i+1] for i in range(len(inertias)-1)]
        if diffs:
            best_k = np.argmin([diffs[i+1]/diffs[i] if diffs[i] > 0 else 1
                               for i in range(len(diffs)-1)]) + 3
            best_k = min(best_k, max_k)

    km = KMeans(n_clusters=best_k, random_state=42, n_init=10)
    labels = km.fit_predict(X_scaled)
    df["cluster_id"] = labels

    # Gera labels descritivos baseados nas médias do cluster
    valor = pd.to_numeric(df["valor_R$"], errors="coerce").fillna(0)
    area = pd.to_numeric(df["area_m2"], errors="coerce").fillna(0)

    cluster_labels = {}
    for cid in range(best_k):
        mask = labels == cid
        n = mask.sum()
        avg_val = valor[mask].mean()
        avg_area = area[mask].mean()

        if avg_val > 2_000_000:
            faixa = "Alto Padrão"
        elif avg_val > 800_000:
            faixa = "Médio-Alto"
        elif avg_val > 400_000:
            faixa = "Médio"
        elif avg_val > 200_000:
            faixa = "Popular"
        else:
            faixa = "Econômico"

        if avg_area > 150:
            tamanho = "Grande"
        elif avg_area > 80:
            tamanho = "Médio"
        else:
            tamanho = "Compacto"

        cluster_labels[cid] = f"{faixa} {tamanho} (n={n})"

    df["cluster_label"] = df["cluster_id"].map(cluster_labels)
    logger.info(f"   K={best_k} clusters: {list(cluster_labels.values())}")

    return df


# ==============================================================================
#  2. DETECÇÃO DE ANOMALIAS (Isolation Forest)
# ==============================================================================

def _anomalias(df: pd.DataFrame, X: pd.DataFrame) -> pd.DataFrame:
    """Detecta imóveis com preço anômalo via Isolation Forest."""
    logger.info("🚨 Detecção de Anomalias (Isolation Forest)...")
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler

    valor = pd.to_numeric(df["valor_R$"], errors="coerce").fillna(0)
    area = pd.to_numeric(df["area_m2"], errors="coerce").fillna(0)

    # Adiciona preço e preco/m2 como features para detecção
    X_anom = X.copy()
    X_anom["valor"] = valor
    X_anom["preco_m2"] = np.where(area > 0, valor / area, 0)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_anom.fillna(0))

    iso = IsolationForest(
        contamination=0.05,  # 5% dos dados são considerados anomalias
        random_state=42,
        n_estimators=200,
    )
    pred = iso.fit_predict(X_scaled)
    scores = iso.decision_function(X_scaled)

    # -1 = anomalia, 1 = normal
    df["anomalia_preco"] = np.where(pred == -1, 1, 0)
    df["anomalia_score"] = np.round(-scores, 4)  # Maior = mais anômalo

    n_anomalias = (pred == -1).sum()
    logger.info(f"   Anomalias detectadas: {n_anomalias}/{len(df)} ({n_anomalias/len(df)*100:.1f}%)")

    # Classifica tipo de anomalia
    anomalos = df[df["anomalia_preco"] == 1]
    if not anomalos.empty and "preco_estimado_ml" in df.columns:
        estimado = pd.to_numeric(anomalos["preco_estimado_ml"], errors="coerce").fillna(0)
        valor_anom = pd.to_numeric(anomalos["valor_R$"], errors="coerce").fillna(0)
        df.loc[df["anomalia_preco"] == 1, "anomalia_tipo"] = np.where(
            valor_anom < estimado * 0.7,
            "Subprecificado (possível oportunidade)",
            np.where(
                valor_anom > estimado * 1.5,
                "Superprecificado (possível erro)",
                "Atípico (revisar manualmente)"
            )
        )
    else:
        df["anomalia_tipo"] = np.where(df["anomalia_preco"] == 1, "Atípico", "Normal")

    df.loc[df["anomalia_preco"] == 0, "anomalia_tipo"] = "Normal"

    return df


# ==============================================================================
#  3. REDE NEURAL MLP
# ==============================================================================

def _mlp_pricing(df: pd.DataFrame, X: pd.DataFrame) -> pd.DataFrame:
    """Predição de preço via Rede Neural (MLPRegressor) com log-transform."""
    logger.info("🧠 Rede Neural MLP...")
    from sklearn.neural_network import MLPRegressor
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import cross_val_score

    valor = pd.to_numeric(df["valor_R$"], errors="coerce")
    mask = valor > 0
    X_train = X[mask].fillna(0)
    y_raw = valor[mask]

    if len(X_train) < 200:
        logger.warning(f"   ⚠️ MLP precisa de ≥200 amostras ({len(X_train)} disponíveis). Pulando.")
        df["preco_mlp"] = 0
        df["delta_mlp_pct"] = 0
        return df

    # Log-transform no target (preços têm distribuição log-normal)
    y_log = np.log1p(y_raw)

    scaler_X = StandardScaler()
    X_scaled = scaler_X.fit_transform(X_train)

    # Arquitetura adaptativa ao tamanho do dataset
    n = len(X_train)
    if n < 500:
        layers = (64, 32)
    elif n < 2000:
        layers = (128, 64, 32)
    else:
        layers = (256, 128, 64, 32)

    mlp = MLPRegressor(
        hidden_layer_sizes=layers,
        activation="relu",
        solver="adam",
        max_iter=1000,
        early_stopping=True,
        validation_fraction=0.15,
        random_state=42,
        learning_rate="adaptive",
        learning_rate_init=0.001,
        alpha=0.01,
        tol=1e-5,
    )

    # Cross-validation (no espaço log)
    cv = min(5, max(3, len(X_train) // 15))
    try:
        scores = cross_val_score(mlp, X_scaled, y_log, cv=cv, scoring="r2")
        r2_cv = scores.mean()
        logger.info(f"   MLP R² (CV, log-space): {r2_cv:.4f}")
    except Exception:
        r2_cv = -999

    # Treina modelo final
    mlp.fit(X_scaled, y_log)
    r2_train = mlp.score(X_scaled, y_log)
    logger.info(f"   MLP R² (treino, log-space): {r2_train:.4f}, iter={mlp.n_iter_}")

    # Predição para todos (inversa do log)
    X_all_scaled = scaler_X.transform(X.fillna(0))
    pred_log = mlp.predict(X_all_scaled)
    pred = np.expm1(pred_log)  # inversa do log1p
    pred = np.clip(pred, 0, None)  # garante não-negativo

    valor_arr = valor.fillna(0).values

    df["preco_mlp"] = np.round(pred, 0)
    df["delta_mlp_pct"] = np.where(
        pred > 10000,
        np.round(((valor_arr - pred) / pred) * 100, 1),
        0
    )

    # Salva modelo
    try:
        import joblib
        joblib.dump({"mlp": mlp, "scaler_X": scaler_X, "features": list(X.columns),
                     "log_transform": True},
                    str(_DIR / "modelo_mlp.joblib"))
        logger.info(f"   Modelo MLP salvo: ia/modelo_mlp.joblib")
    except ImportError:
        pass

    return df


# ==============================================================================
#  4. RELATÓRIO DE ANOMALIAS
# ==============================================================================

def _gerar_relatorio_anomalias(df: pd.DataFrame):
    """Gera relatório markdown das anomalias detectadas."""
    from datetime import datetime

    anomalos = df[df["anomalia_preco"] == 1].sort_values("anomalia_score", ascending=False)
    if anomalos.empty:
        return

    md = []
    md.append(f"# 🚨 Relatório de Anomalias — Imóveis com Preço Atípico\n")
    md.append(f"**Data:** {datetime.now().strftime('%d/%m/%Y %H:%M')}  ")
    md.append(f"**Dataset:** {len(df)} imóveis | **Anomalias:** {len(anomalos)} ({len(anomalos)/len(df)*100:.1f}%)\n")

    # Resumo por tipo
    md.append(f"## Resumo por Tipo\n")
    for tipo, grupo in anomalos.groupby("anomalia_tipo"):
        md.append(f"- **{tipo}**: {len(grupo)} imóveis")
    md.append("")

    # Detalhes
    md.append(f"## Imóveis Anômalos\n")
    md.append(f"| # | Bairro | Zona | Área | Quartos | Preço Pedido | Score | Tipo | Link |")
    md.append(f"|---|--------|------|------|---------|-------------|-------|------|------|")

    for rank, (_, row) in enumerate(anomalos.iterrows(), 1):
        bairro = row.get("bairro", "?")
        zona = row.get("zona", "?")
        area = row.get("area_m2", 0)
        quartos = int(row.get("quartos", 0))
        valor = row.get("valor_R$", 0)
        score = row.get("anomalia_score", 0)
        tipo = row.get("anomalia_tipo", "?")
        url = row.get("url", "")
        link = f"[ver]({url})" if url else ""

        md.append(f"| {rank} | {bairro} | {zona} | {area}m² | {quartos}q | "
                  f"R${valor:,.0f} | {score:.3f} | {tipo} | {link} |")

        if rank >= 50:
            md.append(f"\n*...e mais {len(anomalos) - 50} anomalias.*")
            break

    md.append("")

    with open(str(_DIR / "ANOMALIAS_REPORT.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    logger.info(f"   📋 Relatório: ia/ANOMALIAS_REPORT.md")


# ==============================================================================
#  PIPELINE UNIFICADO
# ==============================================================================

def executar_modelos_avancados(df: pd.DataFrame) -> pd.DataFrame:
    """Executa clustering + anomalia + MLP e retorna df enriquecido."""
    logger.info("🤖 Modelos Avançados (Clustering + Anomalia + MLP)...")

    X = _preparar_X(df)
    logger.info(f"   Features preparadas: {len(X.columns)}")

    # 1. Clustering
    try:
        df = _clustering(df, X)
    except Exception as e:
        logger.warning(f"   ⚠️ Clustering falhou: {e}")

    # 2. Anomalias
    try:
        df = _anomalias(df, X)
        _gerar_relatorio_anomalias(df)
    except Exception as e:
        logger.warning(f"   ⚠️ Anomalia falhou: {e}")

    # 3. MLP
    try:
        df = _mlp_pricing(df, X)
    except Exception as e:
        logger.warning(f"   ⚠️ MLP falhou: {e}")

    # Salva resumo
    resumo = {
        "clusters": int(df["cluster_id"].nunique()) if "cluster_id" in df.columns else 0,
        "anomalias": int(df["anomalia_preco"].sum()) if "anomalia_preco" in df.columns else 0,
        "mlp_r2": round(float(df["preco_mlp"].corr(pd.to_numeric(df["valor_R$"], errors="coerce"))**2), 4) if "preco_mlp" in df.columns else 0,
    }
    with open(str(_DIR / "modelos_avancados_resumo.json"), "w") as f:
        json.dump(resumo, f, indent=2)

    return df


# ==============================================================================
#  STANDALONE
# ==============================================================================

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Modelos Avançados de IA")
    parser.add_argument("--dataset", default=str(_PROJECT / "DATASET_RIO_BI_READY.csv"))
    args = parser.parse_args()

    df = pd.read_csv(args.dataset, sep=";", encoding="utf-8-sig", decimal=",")
    print(f"📄 {len(df)} registros, {len(df.columns)} colunas")

    df = executar_modelos_avancados(df)

    df.to_csv(args.dataset, sep=";", decimal=",", index=False, encoding="utf-8-sig")
    print(f"✓ Dataset atualizado com colunas de IA")
