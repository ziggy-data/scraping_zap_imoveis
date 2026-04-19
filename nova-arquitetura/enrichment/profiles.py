"""
profiles.py — Dimensões derivadas e perfis de público-alvo.
=============================================================

Gera:
  - zona (Norte/Sul/Oeste/Centro)
  - faixa_preco, andar_classificacao, idade_estimada
  - perfil_familia (0-100)
  - perfil_investidor (0-100) 
  - perfil_primeiro_imovel (0-100)
  - score_qualidade_anuncio (0-10)
"""
import os as _os, sys as _sys
_PARENT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
if _PARENT not in _sys.path:
    _sys.path.insert(0, _PARENT)

import numpy as np
import pandas as pd
from config import logger

# ==============================================================================
#  MAPEAMENTO BAIRRO → ZONA
# ==============================================================================

ZONA_SUL = {
    "Botafogo", "Catete", "Copacabana", "Cosme Velho", "Flamengo", "Gávea",
    "Humaitá", "Ipanema", "Jardim Botânico", "Lagoa", "Laranjeiras", "Leblon",
    "Leme", "Rocinha", "São Conrado", "Urca", "Vidigal", "Glória",
}
ZONA_NORTE = {
    "Abolição", "Água Santa", "Alto Da Boa Vista", "Andaraí", "Bancários",
    "Benfica", "Bonsucesso", "Brás De Pina", "Cachambi", "Campinho",
    "Cascadura", "Cavalcanti", "Cocotá", "Complexo Do Alemão", "Cordovil",
    "Costa Barros", "Del Castilho", "Encantado", "Engenheiro Leal",
    "Engenho Da Rainha", "Engenho De Dentro", "Engenho Novo", "Estácio",
    "Galeão", "Grajaú", "Guadalupe", "Higienópolis", "Honório Gurgel",
    "Ilha Do Governador", "Inhaúma", "Irajá", "Jardim América",
    "Jardim Carioca", "Jardim Guanabara", "Lins De Vasconcelos",
    "Madureira", "Mangueira", "Manguinhos", "Maracanã", "Maré",
    "Maria Da Graça", "Marechal Hermes", "Méier", "Moneró",
    "Olaria", "Oswaldo Cruz", "Parada De Lucas", "Pavuna", "Penha",
    "Piedade", "Pilares", "Pitangueiras", "Praça Da Bandeira",
    "Praça Seca", "Praia Da Bandeira", "Quintino Bocaiúva", "Ramos",
    "Riachuelo", "Ricardo De Albuquerque", "Rio Comprido", "Rocha",
    "Rocha Miranda", "Sampaio", "São Cristóvão", "São Francisco Xavier",
    "Tauá", "Tijuca", "Todos Os Santos", "Turiaçu", "Vaz Lobo",
    "Vicente De Carvalho", "Vigário Geral", "Vila Da Penha",
    "Vila Isabel", "Vila Kosmos", "Vila Valqueire", "Vista Alegre",
    "Bento Ribeiro", "Cacuia", "Cidade Universitária", "Coelho Neto",
    "Colégio", "Freguesia (Ilha)", "Jardim América", "Portuguesa",
    "Ribeira", "Zumbi", "Acari", "Anchieta", "Barros Filho",
    "Caju", "Catumbi", "Cidade Nova", "Saúde",
}
ZONA_OESTE = {
    "Anil", "Barra Da Tijuca", "Barra Olímpica", "Bangu", "Campo Grande",
    "Campo Dos Afonsos", "Camorim", "Cidade De Deus", "Cosmos",
    "Curicica", "Deodoro", "Freguesia (Jacarepaguá)", "Gardênia Azul",
    "Grumari", "Guaratiba", "Inhoaíba", "Itanhangá", "Jacarepaguá",
    "Jardim Sulacap", "Joá", "Magalhães Bastos", "Padre Miguel",
    "Paciência", "Pechincha", "Pedra De Guaratiba", "Praça Seca",
    "Realengo", "Recreio Dos Bandeirantes", "Rio Das Pedras",
    "Santa Cruz", "Santíssimo", "Senador Camará", "Senador Vasconcelos",
    "Sepetiba", "Tanque", "Taquara", "Vargem Grande", "Vargem Pequena",
    "Vila Kennedy", "Vila Militar",
}
ZONA_CENTRO = {
    "Centro", "Gamboa", "Lapa", "Paquetá", "Santa Teresa", "Santo Cristo",
}


def _get_zona(bairro: str) -> str:
    import unicodedata
    def strip_acc(s):
        return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

    b = bairro.strip().title()

    # Se o bairro tem lixo do card, tenta extrair o nome real
    if len(b) > 50 or "Para Comprar" in b or "Para Alugar" in b:
        import re
        m = re.search(r'[Ee][Mm](.+?)(?:,\s*Rio|$)', b)
        if m:
            b = m.group(1).strip().title()

    # Lookup direto
    for zona, bairros in [("Zona Sul", ZONA_SUL), ("Zona Norte", ZONA_NORTE),
                           ("Zona Oeste", ZONA_OESTE), ("Centro", ZONA_CENTRO)]:
        if b in bairros:
            return zona

    # Fuzzy: sem acentos
    b_clean = strip_acc(b).lower()
    for zona, bairros in [("Zona Sul", ZONA_SUL), ("Zona Norte", ZONA_NORTE),
                           ("Zona Oeste", ZONA_OESTE), ("Centro", ZONA_CENTRO)]:
        for bb in bairros:
            if strip_acc(bb).lower() == b_clean:
                return zona

    # Substring match (ex: "Recreio Dos Bandeirantes" contém "Recreio")
    for zona, bairros in [("Zona Sul", ZONA_SUL), ("Zona Norte", ZONA_NORTE),
                           ("Zona Oeste", ZONA_OESTE), ("Centro", ZONA_CENTRO)]:
        for bb in bairros:
            if strip_acc(bb).lower() in b_clean or b_clean in strip_acc(bb).lower():
                return zona

    return "Não classificado"


# ==============================================================================
#  PIPELINE
# ==============================================================================

def gerar_perfis(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("👤 Gerando dimensões derivadas e perfis de público...")

    # === ZONA ===
    df["zona"] = df["bairro"].astype(str).apply(_get_zona)
    logger.info(f"   Zonas: {df['zona'].value_counts().to_dict()}")

    # === FAIXA DE PREÇO ===
    valor = pd.to_numeric(df["valor_R$"], errors="coerce").fillna(0)
    df["faixa_preco"] = np.select(
        [valor <= 300_000, valor <= 500_000, valor <= 1_000_000,
         valor <= 2_000_000, valor > 2_000_000],
        ["Até R$300k", "R$300k-500k", "R$500k-1M", "R$1M-2M", "Acima R$2M"],
        default="N/A"
    )

    # === ANDAR ===
    andar = pd.to_numeric(df.get("andar", 0), errors="coerce").fillna(0).astype(int)
    tipo = df["tipo_imovel"].astype(str).str.lower()
    df["andar_classificacao"] = np.select(
        [tipo.str.contains("cobertura"), andar == 0, andar <= 3,
         andar <= 8, andar > 8],
        ["Cobertura", "Térreo / N.I.", "Baixo (1-3)", "Médio (4-8)", "Alto (9+)"],
        default="N/I"
    )

    # === IDADE ESTIMADA ===
    status = df["status_construcao"].astype(str).str.lower()
    dias_pub = pd.to_numeric(df.get("dias_publicado", 0), errors="coerce").fillna(0)
    df["idade_estimada"] = np.select(
        [status.str.contains("planta"), status.str.contains("construção|construcao"),
         status.str.contains("pronto") & (dias_pub < 365),
         status.str.contains("pronto")],
        ["Lançamento (Na Planta)", "Em Construção", "Novo / Semi-novo", "Usado"],
        default="Usado"
    )

    # === QUALIDADE DO ANÚNCIO ===
    q = np.zeros(len(df))
    desc_len = df["descricao"].astype(str).str.len()
    q += np.where(desc_len > 500, 2, np.where(desc_len > 200, 1, 0))
    q += np.where(df["imagem_url"].astype(str).str.len() > 10, 1, 0)
    q += np.where(pd.to_numeric(df["condominio_R$"], errors="coerce").fillna(0) > 0, 1, 0)
    q += np.where(pd.to_numeric(df["iptu_R$"], errors="coerce").fillna(0) > 0, 1, 0)
    q += np.where(df["origem_geo"].astype(str) != "Nao Encontrado", 2, 0)
    q += np.where(df["corretora"].astype(str).str.strip().isin(["", "Nan", "nan"]) == False, 1, 0)
    q += np.where(df["endereco_completo"].astype(str).str.len() > 20, 1, 0)
    q += np.where(pd.to_numeric(df.get("dias_publicado", 0), errors="coerce").fillna(0) > 0, 1, 0)
    df["score_qualidade_anuncio"] = np.clip(q, 0, 10).astype(int)

    # =================================================================
    #  PERFIS DE PÚBLICO (0-100)
    # =================================================================

    # Helper: normaliza coluna para 0-1
    def _norm(col, invert=False):
        s = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0).values
        mn, mx = s.min(), s.max()
        if mx == mn:
            return np.zeros(len(df))
        n = (s - mn) / (mx - mn)
        return 1 - n if invert else n

    def _bool(col):
        return pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0).values.astype(float)

    # --- PERFIL FAMÍLIA (0-100) ---
    # Valoriza: quartos ≥ 2, escola perto, lazer, condomínio fechado,
    #           playground, baixo andar, segurança
    pf = np.zeros(len(df))
    quartos = pd.to_numeric(df["quartos"], errors="coerce").fillna(0)
    pf += np.where(quartos >= 3, 20, np.where(quartos >= 2, 15, 0))
    pf += np.where(quartos >= 4, 5, 0)  # bônus extra 4+
    pf += _norm("dist_saude_educ_km", invert=True) * 15   # escola/hosp perto
    pf += _bool("tem_playground") * 10
    pf += _bool("tem_piscina") * 5
    pf += _bool("tem_churrasqueira") * 5
    pf += _bool("tem_condominio_fechado") * 10
    pf += _bool("tem_portaria_24h") * 5
    pf += _bool("tem_elevador") * 5
    pf += _norm("score_seguranca") * 10
    pf += _norm("dist_lazer_km", invert=True) * 5
    pf += _norm("dist_mercado_km", invert=True) * 5
    df["perfil_familia"] = np.clip(np.round(pf), 0, 100).astype(int)

    # --- PERFIL INVESTIDOR (0-100) ---
    # Valoriza: yield alto, payback curto, vocação Airbnb, preço abaixo ref,
    #           custo fixo baixo, mobilidade
    pi = np.zeros(len(df))
    diff = pd.to_numeric(df.get("diferenca_percentual", 0), errors="coerce").fillna(0)
    pi += np.where(diff < -20, 20, np.where(diff < -10, 15, np.where(diff < 0, 8, 0)))
    payback = pd.to_numeric(df.get("anos_payback", 999), errors="coerce").fillna(999)
    pi += np.where(payback < 15, 20, np.where(payback < 20, 15, np.where(payback < 30, 8, 0)))
    pi += _norm("custo_fixo_mensal", invert=True) * 10
    pi += _norm("score_mobilidade") * 10
    vocacao = df.get("vocacao_airbnb", "Baixa").astype(str)
    pi += np.where(vocacao == "Altíssima", 15, np.where(vocacao == "Alta", 10,
         np.where(vocacao == "Média", 5, 0)))
    pi += np.where(pd.to_numeric(df.get("mcmv_elegivel", 0), errors="coerce").fillna(0) > 0, 5, 0)
    pi += _norm("score_lifestyle") * 5
    gatilho = pd.to_numeric(df.get("gatilho_urgencia", 0), errors="coerce").fillna(0)
    pi += np.where(gatilho > 0, 5, 0)
    df["perfil_investidor"] = np.clip(np.round(pi), 0, 100).astype(int)

    # --- PERFIL PRIMEIRO IMÓVEL (0-100) ---
    # Valoriza: MCMV elegível, preço acessível, transporte perto,
    #           entrada baixa, parcela viável
    pp = np.zeros(len(df))
    mcmv = pd.to_numeric(df.get("mcmv_elegivel", 0), errors="coerce").fillna(0)
    mcmv_faixa = pd.to_numeric(df.get("mcmv_faixa", 0), errors="coerce").fillna(0)
    pp += np.where(mcmv > 0, 25, 0)
    pp += np.where(mcmv_faixa <= 2, 10, np.where(mcmv_faixa == 3, 5, 0))  # Faixa baixa = mais acessível
    subsidio = pd.to_numeric(df.get("mcmv_subsidio", 0), errors="coerce").fillna(0)
    pp += np.where(subsidio > 0, 10, 0)
    pp += np.where(valor <= 264_000, 10, np.where(valor <= 350_000, 5, 0))
    pp += _norm("score_mobilidade") * 15
    pp += _norm("dist_transporte_km", invert=True) * 10
    pp += _bool("tem_elevador") * 5
    pp += _bool("tem_portaria_24h") * 5
    renda_sac = pd.to_numeric(df.get("mcmv_renda_min_sac", 0), errors="coerce").fillna(0)
    pp += np.where((renda_sac > 0) & (renda_sac < 4000), 10,
         np.where((renda_sac >= 4000) & (renda_sac < 8000), 5, 0))
    df["perfil_primeiro_imovel"] = np.clip(np.round(pp), 0, 100).astype(int)

    # === CLASSIFICAÇÕES TEXTUAIS ===
    for col, label in [("perfil_familia", "Família"), ("perfil_investidor", "Investidor"),
                        ("perfil_primeiro_imovel", "Primeiro Imóvel")]:
        df[f"{col}_label"] = np.select(
            [df[col] >= 70, df[col] >= 50, df[col] >= 30, df[col] < 30],
            [f"Excelente p/ {label}", f"Bom p/ {label}", f"Regular p/ {label}", f"Fraco p/ {label}"],
            default="N/A"
        )

    logger.info(f"   Perfis gerados: família avg={df['perfil_familia'].mean():.0f}, "
                f"investidor avg={df['perfil_investidor'].mean():.0f}, "
                f"1º imóvel avg={df['perfil_primeiro_imovel'].mean():.0f}")

    return df
