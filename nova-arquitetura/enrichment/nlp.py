"""
NLP e features derivadas: tags de descrição, urgência, conforto,
segmentação de mercado, dias no mercado.
"""
import os as _os, sys as _sys
_PARENT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
if _PARENT not in _sys.path:
    _sys.path.insert(0, _PARENT)

import numpy as np
import pandas as pd

from config import logger


# ==============================================================================
#  TAG EXTRACTION (NLP sobre a descrição)
# ==============================================================================

def extrair_tags(df: pd.DataFrame) -> pd.DataFrame:
    """Mineração de atributos qualitativos via regex na descrição (vocabulário expandido)."""
    logger.info("💎 Executando Mineração NLP na descrição...")

    desc = df["descricao"].astype(str).str.lower().fillna("")
    if "titulo" in df.columns:
        desc = desc + " " + df["titulo"].astype(str).str.lower().fillna("")

    # --- Arquitetura ---
    df["tag_lamina"] = desc.str.contains(
        r"\bl[aâ]mina\b|prédio l[aâ]mina|edif[ií]cio l[aâ]mina", regex=True).astype(int)
    df["tag_cobertura_linear"] = desc.str.contains(
        r"cobertura linear|linear com terra[çc]o|cobertura duplex", regex=True).astype(int)
    df["tag_pe_direito_alto"] = desc.str.contains(
        r"p[eé] direito alto|pé-direito|p[eé] direito duplo|p[eé] direito elevado|"
        r"p[eé].direito alto|pé direito de \d", regex=True).astype(int)
    df["tag_janelao"] = desc.str.contains(
        r"janel[aã]o|janelas amplas|janelas grandes|esquadrias de alumínio|"
        r"janelas do ch[aã]o ao teto", regex=True).astype(int)

    # --- Posição e Vista ---
    df["tag_indevassavel"] = desc.str.contains(
        r"indevass[aá]vel|vista livre|vista desimpedida|vista permanente|"
        r"sem vis-[aà]-vis|vista definitiva", regex=True).astype(int)
    df["tag_vista_cristo"] = desc.str.contains(
        r"vista.*(?:cristo|redentor|corcovado)|(?:cristo|corcovado).*vista", regex=True).astype(int)
    df["tag_vista_pao_acucar"] = desc.str.contains(
        r"vista.*(?:p[aã]o de a[cç][uú]car|enseada)|p[aã]o de a[cç][uú]car.*vista", regex=True).astype(int)
    df["tag_vista_mar"] = desc.str.contains(
        r"vista.*(?:mar|oceano|praia)|frente.?mar|de frente (?:para|pro|pra) (?:o )?mar|"
        r"vista para (?:a )?praia", regex=True).astype(int)
    df["tag_sol_passante"] = desc.str.contains(
        r"sol passante|ventila[cç][aã]o cruzada", regex=True).astype(int)
    df["tag_sol_manha"] = desc.str.contains(
        r"sol (?:da|pela|de) manh[ãa]|nascente|face norte|face leste", regex=True).astype(int)

    # --- Estado do imóvel (vocabulário expandido) ---
    df["tag_reformado"] = desc.str.contains(
        r"reformado|reforma completa|totalmente reformado|reforma recente|"
        r"rec[eé]m.reformado|reformado por arquiteto|projeto de ilumina[cç][aã]o|"
        r"fino acabamento|acabamento de primeira|alto padr[aã]o de acabamento|"
        r"reforma total|todo reformado|recem reformado", regex=True).astype(int)
    df["tag_precisa_reforma"] = desc.str.contains(
        r"estado original|precisa de (?:obra|reforma|moderniza)|para reformar|"
        r"necessita reforma|bom para reformar|oportunidade.{0,20}reform", regex=True).astype(int)
    df["tag_retrofit"] = desc.str.contains(
        r"retrofit|toda hidr[aá]ulica e el[eé]trica nova|hidr[aá]ulica nova|"
        r"instala[cç][oõ]es novas|rede el[eé]trica nova", regex=True).astype(int)
    df["tag_novo_nunca_habitado"] = desc.str.contains(
        r"nunca habitado|nunca morou|primeira loca[cç][aã]o|primeira moradia|"
        r"primeira habita[cç][aã]o|zero km|0km|im[oó]vel novo|apartamento novo", regex=True).astype(int)

    # --- Exclusividade (vocabulário expandido) ---
    df["tag_exclusivo"] = desc.str.contains(
        r"um por andar|1 por andar|[uú]nico por andar|hall privativo|"
        r"elevador privativo|andar inteiro|unidade [uú]nica|exclusivo", regex=True).astype(int)
    df["tag_centro_terreno"] = desc.str.contains(
        r"centro de terreno|afastado da rua|recuado|no centro do terreno", regex=True).astype(int)
    df["tag_silencioso"] = desc.str.contains(
        r"silencioso|silêncio|rua tranquila|rua calma|local tranquilo|"
        r"tranquilidade|sem barulho", regex=True).astype(int)
    df["tag_arborizada"] = desc.str.contains(
        r"arborizada|rua arborizada|rodeado de verde|muito verde|"
        r"[aá]rvores|cercado.{0,10}natureza", regex=True).astype(int)

    # --- Logística ---
    df["tag_vazio"] = desc.str.contains(
        r"vazio|chaves em m[aã]os|entrega imediata|desocupado|pronto para morar|"
        r"mude j[aá]|disponível para mudan[cç]a|chaves na m[aã]o", regex=True).astype(int)
    df["tag_doc_ok"] = desc.str.contains(
        r"documenta[cç][aã]o (?:ok|cristalina|perfeita|em ordem|regular)|"
        r"aceita financiamento|aceita fgts|escriturado|"
        r"escritura.{0,15}(?:ok|dia|ordem)", regex=True).astype(int)
    df["tag_aceita_permuta"] = desc.str.contains(
        r"aceita permuta|permuta|troca", regex=True).astype(int)

    # --- Diferenciais (novos) ---
    df["tag_mobiliado"] = desc.str.contains(
        r"mobiliado|mobília|mobiliad[oa]|m[oó]veis planejados|totalmente mobiliado|"
        r"semi.mobiliado|parcialmente mobiliado", regex=True).astype(int)
    df["tag_gourmet"] = desc.str.contains(
        r"gourmet|espa[cç]o gourmet|cozinha gourmet|varanda gourmet|"
        r"[aá]rea gourmet|churrasqueira gourmet", regex=True).astype(int)
    df["tag_coworking"] = desc.str.contains(
        r"coworking|co-working|home.?office|escrit[oó]rio|sala de trabalho", regex=True).astype(int)
    df["tag_sustentavel"] = desc.str.contains(
        r"solar|fotovoltaic|reaproveitamento|sustent[aá]vel|[aá]gua de chuva|"
        r"ecol[oó]gic|green.?building|leed", regex=True).astype(int)
    df["tag_pet_friendly"] = desc.str.contains(
        r"pet.?friendly|aceita pet|permite animal|pet.?place|brinquedoteca pet|"
        r"[aá]rea pet|espa[cç]o pet", regex=True).astype(int)

    return df


# ==============================================================================
#  URGÊNCIA DE VENDA
# ==============================================================================

def detectar_urgencia(df: pd.DataFrame) -> pd.DataFrame:
    """Detecta gatilhos de urgência na descrição + combina com tempo de mercado."""
    logger.info("🔥 Procurando gatilhos de urgência...")

    desc = df["descricao"].str.lower().fillna("")

    gatilhos = [
        "motivo viagem", "mudança", "inventário", "urgente",
        "oportunidade", "abaixo da avaliação", "baixou",
        "oferta", "proposta", "liquidez", "aceito proposta",
        "saída do país", "divórcio", "separação",
    ]
    regex = "|".join(gatilhos)
    df["gatilho_urgencia"] = desc.str.contains(regex, regex=True).astype(int)

    # Oportunidade de Ouro: gatilho + mais de 90 dias no mercado
    dias = pd.to_numeric(df.get("dias_no_mercado", 0), errors="coerce").fillna(0)
    df["alerta_oportunidade_ouro"] = ((df["gatilho_urgencia"] == 1) & (dias > 90)).astype(int)

    return df


# ==============================================================================
#  DIAS NO MERCADO
# ==============================================================================

def calcular_dias_mercado(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula há quantos dias o anúncio está publicado."""
    logger.info("📅 Calculando Dias no Mercado (DOM)...")

    df["dt_anuncio"] = pd.to_datetime(df["anuncio_criado"], errors="coerce")
    hoje = pd.Timestamp.now().normalize()
    df["dias_no_mercado"] = (hoje - df["dt_anuncio"]).dt.days.fillna(0).astype(int)

    # Classificação vetorizada
    dias = df["dias_no_mercado"].values
    status = np.full(len(df), "Normal (3 Meses)", dtype=object)
    status = np.where(dias <= 7, "Recém Chegado (Hype)", status)
    status = np.where((dias > 7) & (dias <= 30), "Recente (1 Mês)", status)
    status = np.where((dias > 90) & (dias <= 180), "Encalhado (6 Meses)", status)
    status = np.where(dias > 180, "Zombie (> 6 Meses - Barganha Extrema)", status)
    df["status_temporal"] = status

    return df


# ==============================================================================
#  SEGMENTAÇÃO DE MERCADO
# ==============================================================================

def segmentar_mercado(df: pd.DataFrame) -> pd.DataFrame:
    """Segmentação por quintis de preço e área."""
    logger.info("📊 Segmentando mercado (Quantiles)...")

    labels_preco = [
        "Econômico (Top 20% Baratos)", "Médio-Baixo",
        "Médio-Alto", "Alto Padrão", "Luxo (Top 20% Caros)",
    ]
    try:
        df["segmento_mercado"] = pd.qcut(df["valor_R$"], q=5, labels=labels_preco, duplicates="drop")
    except ValueError:
        df["segmento_mercado"] = "Padrão"

    labels_area = ["Compacto", "Padrão", "Confortável", "Espaçoso", "Gigante"]
    try:
        df["perfil_tamanho"] = pd.qcut(df["area_m2"], q=5, labels=labels_area, duplicates="drop")
    except ValueError:
        df["perfil_tamanho"] = "Padrão"

    return df


# ==============================================================================
#  ÍNDICE DE CONFORTO
# ==============================================================================

def _col_to_int(series: pd.Series) -> pd.Series:
    """Converte coluna que pode ser bool, string 'True'/'False', ou numérica → int 0/1."""
    # Mapeia strings "True"/"False" para 1/0
    mapped = series.map({"True": 1, "False": 0, "true": 1, "false": 0, True: 1, False: 0})
    # O que não foi mapeado (NaN), tenta converter numericamente
    return pd.to_numeric(mapped.fillna(series), errors="coerce").fillna(0).astype(int)


def calcular_conforto(df: pd.DataFrame) -> pd.DataFrame:
    """Índice de conforto 0-100 baseado em infra, lazer, NLP e geo."""
    logger.info("🌟 Calculando Índice de Conforto 2.0...")

    # Pesos por categoria
    w_infra = {
        "tem_vaga": 15, "tem_portaria_24h": 10, "tem_elevador": 10,
        "tem_varanda": 8, "tem_ar_condicionado": 5,
        "tem_armario_embutido": 2, "tem_box_blindex": 1,
    }
    w_lazer = {
        "tem_piscina": 5, "tem_academia": 4, "tem_churrasqueira": 3,
        "tem_salao_festas": 2, "tem_sauna": 2, "tem_varanda_gourmet": 5,
    }
    w_nlp = {
        "tag_sol_manha": 8, "tag_silencioso": 6, "tag_indevassavel": 5,
        "tag_vista_mar": 5, "tag_reformado": 5, "tag_vazio": 2,
        "tag_mobiliado": 3, "tag_gourmet": 2,
    }

    # Vaga (especial: vem de "vagas" numérico)
    vagas = pd.to_numeric(df["vagas"], errors="coerce").fillna(0)
    score = (vagas > 0).astype(int) * w_infra["tem_vaga"]

    # Loop de features binárias
    for weights in [w_infra, w_lazer, w_nlp]:
        for col, peso in weights.items():
            if col in df.columns and col != "tem_vaga":
                score = score + _col_to_int(df[col]) * peso

    # Bônus geográfico
    if "dist_transporte_km" in df.columns:
        bonus_metro = (
            pd.to_numeric(df["dist_transporte_km"], errors="coerce").fillna(99) < 0.7
        ).astype(int) * 10
        score = score + bonus_metro

    if "dist_mercado_km" in df.columns:
        bonus_mercado = (
            pd.to_numeric(df["dist_mercado_km"], errors="coerce").fillna(99) < 0.4
        ).astype(int) * 5
        score = score + bonus_mercado

    df["score_conforto"] = score.clip(upper=100)

    return df