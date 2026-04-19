#!/usr/bin/env python3
"""
health_check.py — Validação de qualidade do dataset de imóveis.
================================================================

Analisa o CSV gerado pelo scraper e produz um relatório em Markdown
indicando campos quebrados, seletores que precisam de atenção,
e estatísticas de cobertura.

Uso:
    python health_check.py                          # Usa DATASET_RIO_BI_READY.csv
    python health_check.py meu_dataset.csv          # CSV customizado
    python health_check.py --output relatorio.md    # Nome do relatório
"""
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

# ==============================================================================
#  CAMPOS CRÍTICOS E LIMITES
# ==============================================================================

CAMPOS_CRITICOS = {
    # --- Scraping Básico (seletores do card / detail page) ---
    "valor_R$":       {"desc": "Preço do imóvel",          "tipo": "num", "min_pct": 95},
    "area_m2":        {"desc": "Área em m²",               "tipo": "num", "min_pct": 95},
    "condominio_R$":  {"desc": "Valor do condomínio",      "tipo": "num", "min_pct": 50},
    "iptu_R$":        {"desc": "Valor do IPTU",            "tipo": "num", "min_pct": 30},
    "quartos":        {"desc": "Número de quartos",        "tipo": "num", "min_pct": 70},
    "bairro":         {"desc": "Bairro do imóvel",         "tipo": "txt", "min_pct": 95},
    "rua":            {"desc": "Rua do imóvel",            "tipo": "txt", "min_pct": 80},
    "descricao":      {"desc": "Descrição do anúncio",     "tipo": "lng", "min_pct": 90},
    # --- Detail Page ---
    "tipo_imovel":       {"desc": "Tipo (Apartamento, Casa...)",    "tipo": "txt", "min_pct": 90},
    "status_construcao": {"desc": "Status (Pronto, Em construção)", "tipo": "txt", "min_pct": 80},
    "endereco_completo": {"desc": "Endereço completo",              "tipo": "txt", "min_pct": 80},
    "corretora":         {"desc": "Nome da corretora",              "tipo": "txt", "min_pct": 50},
    # --- Geolocalização ---
    "coordenadas": {"desc": "Coordenadas lat/lon", "tipo": "geo", "min_pct": 80},
    # --- Publicação ---
    "publicacao_texto": {"desc": "Texto de publicação",    "tipo": "txt", "min_pct": 70},
    "dias_publicado":   {"desc": "Dias desde publicação",  "tipo": "num", "min_pct": 60},
    # --- Amenidades ---
    "tem_portaria_24h": {"desc": "Portaria 24h",  "tipo": "bool", "min_pct": 0},
    "tem_elevador":     {"desc": "Elevador",      "tipo": "bool", "min_pct": 0},
    "tem_piscina":      {"desc": "Piscina",       "tipo": "bool", "min_pct": 0},
    # --- Campos Calculados ---
    "preco_m2":           {"desc": "Preço por m²",         "tipo": "var", "min_pct": 0},
    "custo_fixo_mensal":  {"desc": "Custo fixo mensal",    "tipo": "var", "min_pct": 0},
    "score_conforto":     {"desc": "Score de conforto",    "tipo": "var", "min_pct": 0},
    "score_investimento": {"desc": "Score de investimento", "tipo": "var", "min_pct": 0},
    "anos_payback":       {"desc": "Payback (anos)",       "tipo": "var", "min_pct": 0},
}


# ==============================================================================
#  VALIDAÇÃO
# ==============================================================================

INVALIDOS = {"", "nan", "Nan", "None", "Rua Não Informada", "Não Informado", "Indefinido", "0", "0.0"}


def validar_campo(df, campo, config):
    """Valida um campo e retorna dict com resultado."""
    if campo not in df.columns:
        return {"status": "❌", "pct": 0, "detalhe": "COLUNA AUSENTE"}

    s = df[campo]
    tipo = config["tipo"]
    minimo = config["min_pct"]

    if tipo == "num":
        num = pd.to_numeric(s, errors="coerce").fillna(0)
        pos = (num > 0).sum()
        pct = round(pos / len(s) * 100, 1)
        detalhe = f"{pos}/{len(s)} positivos, min={num.min():.0f}, média={num.mean():.0f}, max={num.max():.0f}"

    elif tipo == "txt":
        validos = s.astype(str).apply(lambda x: x.strip() not in INVALIDOS and len(x.strip()) > 0)
        pos = validos.sum()
        pct = round(pos / len(s) * 100, 1)
        top = ", ".join(f"{k}({v})" for k, v in s.value_counts().head(3).items())
        detalhe = f"{pos}/{len(s)} preenchidos. Top: {top}"

    elif tipo == "lng":
        lens = s.astype(str).str.len()
        pos = (lens > 50).sum()
        pct = round(pos / len(s) * 100, 1)
        detalhe = f"{pos}/{len(s)} com >50 chars, média={lens.mean():.0f} chars"

    elif tipo == "geo":
        validos = s.astype(str).apply(
            lambda x: "," in x and "0.0,0.0" not in x and "Url" not in x and "Nao" not in x
        )
        pos = validos.sum()
        pct = round(pos / len(s) * 100, 1)
        detalhe = f"{pos}/{len(s)} com coordenadas válidas"

    elif tipo == "bool":
        num = pd.to_numeric(s, errors="coerce").fillna(0)
        pos = (num > 0).sum()
        pct = round(pos / len(s) * 100, 1)
        variou = 0 < pos < len(s)
        detalhe = f"{pos}/{len(s)} = True. {'✓ Variação OK' if variou else ('⚠️ Sempre 0' if pos == 0 else '⚠️ Sempre 1')}"

    elif tipo == "var":
        num = pd.to_numeric(s, errors="coerce").fillna(0)
        unicos = num.nunique()
        pos = (num > 0).sum()
        pct = round(pos / len(s) * 100, 1)
        if unicos <= 1:
            detalhe = f"❌ CONSTANTE ({num.iloc[0]}) — seletor upstream quebrado"
        else:
            detalhe = f"{unicos} valores distintos, min={num.min():.1f}, média={num.mean():.1f}, max={num.max():.1f}"
    else:
        pct = 0
        detalhe = "Tipo desconhecido"

    # Status
    if tipo == "bool":
        num = pd.to_numeric(s, errors="coerce").fillna(0)
        variou = 0 < (num > 0).sum() < len(s)
        status = "✅" if variou else "⚠️"
    elif tipo == "var":
        num = pd.to_numeric(s, errors="coerce").fillna(0)
        status = "✅" if num.nunique() > 1 else "❌"
    elif pct >= minimo:
        status = "✅"
    elif pct >= minimo * 0.5:
        status = "⚠️"
    else:
        status = "❌"

    return {"status": status, "pct": pct, "detalhe": detalhe}


# ==============================================================================
#  RELATÓRIO MARKDOWN
# ==============================================================================

def gerar_relatorio(df, csv_path):
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
    md = []

    md.append(f"# 🏠 Health Check — Dataset de Imóveis RJ\n")
    md.append(f"**Arquivo:** `{csv_path}`  ")
    md.append(f"**Data:** {agora}  ")
    md.append(f"**Registros:** {len(df)} | **Colunas:** {len(df.columns)}\n")

    # --- Resumo ---
    md.append(f"## 📊 Resumo Geral\n")

    if "origem_geo" in df.columns:
        md.append(f"**Geolocalização:**\n")
        for k, v in df["origem_geo"].value_counts().items():
            md.append(f"- {k}: {v} ({round(v/len(df)*100,1)}%)")
        md.append("")

    for col, label in [("tipo_imovel", "Tipos de imóvel"), ("status_construcao", "Status")]:
        if col in df.columns:
            md.append(f"**{label}:**\n")
            for k, v in df[col].value_counts().items():
                md.append(f"- {k}: {v}")
            md.append("")

    # --- Tabela de validação ---
    md.append(f"## 🔍 Validação de Campos Críticos\n")
    md.append(f"| Status | Campo | Descrição | Cobertura | Detalhe |")
    md.append(f"|:------:|-------|-----------|:---------:|---------|")

    problemas = []
    avisos = []
    ok_count = 0

    for campo, config in CAMPOS_CRITICOS.items():
        r = validar_campo(df, campo, config)
        md.append(f"| {r['status']} | `{campo}` | {config['desc']} | {r['pct']}% | {r['detalhe']} |")

        if r["status"] == "✅":
            ok_count += 1
        elif r["status"] == "⚠️":
            avisos.append(f"`{campo}`: {config['desc']} — {r['pct']}%")
        else:
            problemas.append((campo, config, r))

    md.append("")

    # --- Colunas constantes ---
    md.append(f"## 🚨 Colunas Sempre com Mesmo Valor\n")
    constantes = [(c, df[c].iloc[0]) for c in df.columns if df[c].nunique() == 1 and c != "tipo_negocio"]

    if constantes:
        md.append(f"| Coluna | Valor Fixo |")
        md.append(f"|--------|:----------:|")
        for col, val in constantes:
            md.append(f"| `{col}` | `{val}` |")
    else:
        md.append(f"Nenhuma. ✅")
    md.append("")

    # --- Financeiro ---
    md.append(f"## 💰 Diagnóstico Financeiro\n")
    for col, label in [
        ("custo_fixo_mensal", "Custo Fixo"), ("aluguel_estimado", "Aluguel Estimado"),
        ("fluxo_caixa_mensal", "Fluxo de Caixa"), ("anos_payback", "Payback"),
        ("primeira_parcela_estimada", "1ª Parcela SAC"),
    ]:
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce").fillna(0)
            u = s.nunique()
            if u <= 1:
                md.append(f"- **{label}**: ❌ Constante (`{s.iloc[0]}`) — depende de condomínio/IPTU")
            else:
                md.append(f"- **{label}**: ✅ min=R${s.min():,.0f} | média=R${s.mean():,.0f} | max=R${s.max():,.0f}")
    md.append("")

    # --- Scores ---
    md.append(f"## 🎯 Scores\n")
    for col, label in [
        ("score_conforto", "Conforto (0-100)"), ("score_investimento", "Investimento (0-10)"),
        ("score_mobilidade", "Mobilidade (0-10)"), ("score_seguranca", "Segurança (0-10)"),
        ("score_lifestyle", "Lifestyle (0-10)"),
    ]:
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce").fillna(0)
            md.append(f"- **{label}**: min={s.min():.0f} | média={s.mean():.1f} | max={s.max():.0f} ({s.nunique()} valores)")
    md.append("")

    # --- Veredito ---
    total = len(CAMPOS_CRITICOS)
    md.append(f"## 📋 Veredito\n")

    if not problemas and not avisos:
        md.append(f"### ✅ APROVADO — {ok_count}/{total} campos OK\n")
    elif not problemas:
        md.append(f"### ⚠️ APROVADO COM RESSALVAS — {ok_count}/{total} campos OK\n")
        for a in avisos:
            md.append(f"- ⚠️ {a}")
    else:
        md.append(f"### ❌ REPROVADO — {ok_count}/{total} campos OK\n")
        md.append(f"**Problemas críticos:**\n")
        for campo, config, r in problemas:
            md.append(f"- ❌ `{campo}` ({config['desc']}): {r['detalhe']}")
        if avisos:
            md.append(f"\n**Avisos:**\n")
            for a in avisos:
                md.append(f"- ⚠️ {a}")
    md.append("")

    # --- Recomendações ---
    if problemas:
        md.append(f"## 🔧 Recomendações\n")
        for campo, config, r in problemas:
            if "condominio" in campo or "iptu" in campo:
                md.append(f"- **{campo}**: Verificar seletores `p[data-testid=\"condoFee\"]` / `p[data-testid=\"iptu\"]`. O ZAP usa `<p>` (não `<span>`). Texto contém `R$\\xa0` (nbsp).")
            elif "coordenadas" in campo:
                md.append(f"- **{campo}**: Verificar `iframe[data-testid=\"map-iframe\"]`. URL usa `%2C` entre lat/lon.")
            elif "publicacao" in campo or "dias_" in campo:
                md.append(f"- **{campo}**: Elemento tem `min-md:hidden`. Usar JavaScript para extrair texto de elementos ocultos.")
            elif "AUSENTE" in r["detalhe"]:
                md.append(f"- **{campo}**: Adicionar ao `CSV_FIELDNAMES` em `config.py`.")
            elif "CONSTANTE" in r["detalhe"]:
                md.append(f"- **{campo}**: Campo calculado com valor fixo. Verificar se os campos de entrada (condomínio, IPTU, coordenadas) estão sendo extraídos.")
            else:
                md.append(f"- **{campo}**: Cobertura {r['pct']}% (mínimo esperado: {config['min_pct']}%). Verificar seletor CSS.")
        md.append("")

    return "\n".join(md)


# ==============================================================================
#  MAIN
# ==============================================================================

def main():
    csv_path = "DATASET_RIO_BI_READY.csv"
    output_path = "HEALTH_CHECK_REPORT.md"

    for arg in sys.argv[1:]:
        if arg.startswith("--output="):
            output_path = arg.split("=", 1)[1]
        elif arg.endswith(".csv"):
            csv_path = arg

    if not Path(csv_path).exists():
        print(f"❌ Arquivo não encontrado: {csv_path}")
        sys.exit(1)

    print(f"📄 Lendo {csv_path}...")
    df = pd.read_csv(csv_path, sep=";", encoding="utf-8-sig", decimal=",")
    print(f"   ✓ {len(df)} registros, {len(df.columns)} colunas\n")

    relatorio = gerar_relatorio(df, csv_path)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(relatorio)
    print(f"✓ Relatório salvo: {output_path}\n")

    # Resumo no terminal
    print("=" * 50)
    for linha in relatorio.split("\n"):
        if linha.startswith("### "):
            print(f"  {linha.replace('### ', '')}")
        elif "❌" in linha and linha.startswith("- "):
            print(f"  {linha}")
    print("=" * 50)


if __name__ == "__main__":
    main()