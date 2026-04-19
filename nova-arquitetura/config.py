"""
Configurações centrais do scraper de imóveis do Rio de Janeiro.
Todas as constantes, POIs e parâmetros ficam aqui.
"""
from dataclasses import dataclass
from typing import Dict, Tuple
import logging

# ==============================================================================
#  LOGGING
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler("scraper_execution.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("scraper_rio")

# ==============================================================================
#  PERFORMANCE
# ==============================================================================
MAX_LISTING_WORKERS = 4
MAX_DETAILS_WORKERS = 8    # Reduzido de 18 → menos RAM, mais estável
NUM_PAGINAS = 300
DRIVER_RESTART_INTERVAL = 50

# ==============================================================================
#  ANTI-DETECÇÃO
# ==============================================================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
]

# ==============================================================================
#  ZONAS DE BUSCA
# ==============================================================================
ZONAS_CONFIG = [
    {"nome": "Zona Norte", "slug": "rj+rio-de-janeiro+zona-norte", "split": True},
    {"nome": "Zona Sul",   "slug": "rj+rio-de-janeiro+zona-sul",   "split": True},
    {"nome": "Zona Oeste", "slug": "rj+rio-de-janeiro+zona-oeste", "split": True},
]

MONTHS_PT = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4, "maio": 5, "junho": 6,
    "julho": 7, "agosto": 8, "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
}

# ==============================================================================
#  PARÂMETROS FINANCEIROS
# ==============================================================================
@dataclass
class ParametrosFinanceiros:
    """Parâmetros de simulação financeira (Caixa Econômica / Mercado)."""
    taxa_itbi: float = 0.03
    taxa_cartorio: float = 0.015
    percentual_financiavel: float = 0.80
    juros_aa_mercado: float = 0.105
    juros_aa_mcmv_faixa3: float = 0.0785
    juros_aa_mcmv_faixa2: float = 0.065
    teto_mcmv: float = 350_000.0
    prazo_meses: int = 420
    yield_aluguel: float = 0.0045

    def juros_para(self, valor: float) -> float:
        if valor <= self.teto_mcmv:
            return self.juros_aa_mcmv_faixa3
        return self.juros_aa_mercado

    def juros_mensais(self, valor: float) -> float:
        return (1 + self.juros_para(valor)) ** (1 / 12) - 1


PARAMS_FIN = ParametrosFinanceiros()

# ==============================================================================
#  SELETORES CSS (centralizados — quando o Zap mudar, editar só aqui)
# ==============================================================================
@dataclass(frozen=True)
class Seletores:
    card_item: str = 'li[data-cy="rp-property-cd"]'
    card_price: str = 'div[data-cy="rp-cardProperty-price-txt"] p.text-2-25'
    card_area: str = 'li[data-cy="rp-cardProperty-propertyArea-txt"] h3'
    card_quartos: str = 'li[data-cy="rp-cardProperty-bedroomQuantity-txt"] h3'
    card_vagas: str = 'li[data-cy="rp-cardProperty-parkingSpacesQuantity-txt"] h3'
    card_banheiros: str = 'li[data-cy="rp-cardProperty-bathroomQuantity-txt"] h3'
    card_location: str = 'h2[data-cy="rp-cardProperty-location-txt"]'
    card_street: str = 'p[data-cy="rp-cardProperty-street-txt"]'
    card_costs: str = 'div[data-cy="rp-cardProperty-price-txt"] p.text-1-75'
    card_tag: str = 'li[data-cy="rp-cardProperty-tag-txt"]'
    detail_desc: str = "p[data-testid='description-content']"
    detail_address: str = "p[data-testid='address-info-value']"
    detail_address_alt: str = "p[data-testid='location-address']"
    detail_broker: str = "a[data-testid='official-store-redirect-link']"
    detail_rating: str = '[data-testid="rating-container"] .rating-container__text'
    detail_date: str = '[data-testid="listing-created-date"]'
    detail_map_iframe: str = "iframe[data-testid='map-iframe']"
    detail_expand_btn: str = "button[data-cy='ldp-TextCollapse-btn']"
    # Tags de classificação (tipo, status, negócio)
    detail_tipo_imovel: str = "div.info-tags__unit-type"
    detail_status_construcao: str = "div.info-tags__construction-status"
    detail_tipo_negocio: str = "div.info-tags__business"
    # Info de publicação/atualização (texto relativo)
    detail_pub_info: str = "section.flex.gap-1.items-center p.font-secondary"
    panel_imovel: str = "panel-unitAmenities"
    panel_condo: str = "panel-sectionAmenities"
    tab_condo: str = "sectionAmenities"


SEL = Seletores()

# ==============================================================================
#  MAPEAMENTO DE AMENIDADES
# ==============================================================================
MAP_IMOVEL = {
    "aceita_pet": "li[itemprop='PETS_ALLOWED']",
    "tem_vista_pro_mar": "li[itemprop='SEA_VIEW']",
    "tem_janela_grande": "li[itemprop='LARGE_WINDOW']",
    "tem_ar_condicionado": "li[itemprop='AIR_CONDITIONING']",
    "tem_banheira": "li[itemprop='BATHTUB']",
    "tem_banheiro_servico": "li[itemprop='SERVICE_BATHROOM']",
    "tem_armario_cozinha": "li[itemprop='KITCHEN_CABINETS']",
    "tem_armario_banheiro": "li[itemprop='BATHROOM_CABINETS']",
    "tem_piso_madeira": "li[itemprop='WOOD_FLOOR']",
    "tem_box_blindex": "li[itemprop='BLINDEX_BOX']",
    "tem_area_servico": "li[itemprop='SERVICE_AREA']",
    "tem_closet": "li[itemprop='CLOSET']",
    "tem_copa": "li[itemprop='COPA']",
    "tem_varanda": "li[itemprop='BALCONY']",
    "tem_varanda_gourmet": "li[itemprop='GOURMET_BALCONY']",
}
 
MAP_CONDOMINIO = {
    "tem_portaria_24h": "li[itemprop='CONCIERGE_24H']",
    "tem_armario_embutido": "li[itemprop='BUILTIN_WARDROBE']",
    "tem_estacionamento": "li[itemprop='PARKING']",
    "tem_academia": "li[itemprop='GYM']",
    "tem_salao_festas": "li[itemprop='PARTY_HALL']",
    "tem_piscina": "li[itemprop='POOL']",
    "tem_interfone": "li[itemprop='INTERCOM']",
    "tem_sala_massagem": "li[itemprop='MASSAGE_ROOM']",
    "tem_churrasqueira": "li[itemprop='BARBECUE_GRILL']",
    "tem_quadra_poliesportiva": "li[itemprop='SPORTS_COURT']",
    "tem_sauna": "li[itemprop='SAUNA']",
    "tem_playground": "li[itemprop='PLAYGROUND']",
    "tem_squash": "li[itemprop='SQUASH']",
    "tem_condominio_fechado": "li[itemprop='GATED_COMMUNITY']",
    "tem_elevador": "li[itemprop='ELEVATOR']",
    "tem_loja": "li[itemprop='STORES']",
    "tem_administracao": "li[itemprop='ADMINISTRATION']",
    "tem_zelador": "li[itemprop='CARETAKER']",
}
 
CSV_FIELDNAMES = [
    "url", "valor_R$", "area_m2", "quartos", "vagas", "banheiros", "suites", "andar",
    "tipo_imovel", "status_construcao", "tipo_negocio",
    "bairro", "cidade", "rua", "numero", "endereco_completo",
    "condominio_R$", "iptu_R$",
    "destaque", "imagem_url", "descricao", "corretora",
    "nota_media", "total_avaliacoes", "anuncio_criado",
    "publicacao_texto", "dias_publicado", "dias_atualizado",
    "latitude", "longitude", "coordenadas", "origem_geo",
    *MAP_IMOVEL.keys(),
    *MAP_CONDOMINIO.keys(),
]

# ==============================================================================
#  PONTOS DE INTERESSE (POIs)
# ==============================================================================
Coord = Tuple[float, float]
POICategory = Dict[str, Coord]

POIS_RIO: Dict[str, POICategory] = {
    "praias": {
        "Leme": (-22.9635, -43.1717),
        "Copacabana (Copacabana Palace)": (-22.9700, -43.1830),
        "Copacabana (Posto 5)": (-22.9778, -43.1903),
        "Ipanema (Posto 9)": (-22.9866, -43.2046),
        "Leblon (Posto 12)": (-22.9873, -43.2215),
        "São Conrado (Pepino)": (-22.9975, -43.2647),
        "Barra (Jardim Oceânico)": (-23.0125, -43.3087),
        "Barra (Posto 4)": (-23.0116, -43.3245),
        "Reserva/Recreio": (-23.0182, -43.4332),
        "Flamengo (Aterro)": (-22.9324, -43.1730),
        "Botafogo (Enseada)": (-22.9463, -43.1819),
    },
    "lazer_verde": {
        "Lagoa Rodrigo de Freitas": (-22.9734, -43.2114),
        "Aterro do Flamengo": (-22.9221, -43.1733),
        "Jardim Botânico": (-22.9676, -43.2233),
        "Parque Lage": (-22.9602, -43.2119),
        "Quinta da Boa Vista": (-22.9056, -43.2244),
        "Parque Madureira": (-22.8732, -43.3402),
        "Parque Piedade": (-22.8929, -43.3080),
        "Parque Rita Lee": (-22.9770, -43.3940),
        "Parque Realengo": (-22.8760, -43.4300),
    },
    "shoppings_premium": {
        "Shopping Leblon/Design": (-22.9824, -43.2173),
        "RioSul": (-22.9567, -43.1769),
        "Botafogo Praia": (-22.9510, -43.1820),
        "BarraShopping": (-22.9995, -43.3556),
        "Village Mall": (-23.0003, -43.3533),
        "Shopping Tijuca": (-22.9234, -43.2360),
        "NorteShopping": (-22.8879, -43.2831),
        "Nova América": (-22.8794, -43.2721),
    },
    "transporte_hub": {
        "Metro Cardeal Arcoverde": (-22.9644, -43.1812),
        "Metro General Osório": (-22.9846, -43.1977),
        "Metro Nossa Sra Paz": (-22.9839, -43.2065),
        "Metro Jardim de Alah": (-22.9829, -43.2154),
        "Metro Antero de Quental": (-22.9849, -43.2232),
        "Metro Botafogo": (-22.9510, -43.1840),
        "Metro Flamengo": (-22.9324, -43.1790),
        "Metro Largo do Machado": (-22.9298, -43.1780),
        "Metro Catete": (-22.9267, -43.1775),
        "Metro Carioca": (-22.9079, -43.1772),
        "Metro Saens Peña": (-22.9244, -43.2325),
        "Metro Uruguai": (-22.9317, -43.2405),
        "Metro São Francisco Xavier": (-22.9208, -43.2241),
        "Metro Jd Oceânico": (-23.0076, -43.3106),
        "Metro São Conrado": (-22.9912, -43.2543),
        "Metro Pavuna": (-22.8126, -43.3600),
        "Metro São Cristóvão": (-22.9097, -43.2223),
        "Estação Central do Brasil": (-22.9042, -43.1887),
        "Estação Méier": (-22.9018, -43.2781),
        "Estação Madureira": (-22.8769, -43.3374),
        "Estação Deodoro": (-22.8549, -43.3837),
        "Estação Bangu": (-22.8754, -43.4658),
        "Estação Campo Grande": (-22.9022, -43.5604),
    },
    "brt_stations": {
        "Term. Alvorada": (-22.9992, -43.3663),
        "Term. Jardim Oceânico": (-23.0076, -43.3106),
        "Estação Vicente de Carvalho": (-22.8546, -43.3134),
        "Term. Recreio": (-23.0139, -43.4533),
        "Estação Taquara": (-22.9213, -43.3725),
        "Term. Paulo da Portela": (-22.8765, -43.3385),
        "Estação Galeão (Aeroporto)": (-22.8079, -43.2530),
    },
    "saude_educacao": {
        "Colégio Pedro II (Humaitá)": (-22.9569, -43.1936),
        "Colégio Pedro II (Tijuca)": (-22.9189, -43.2183),
        "Colégio Pedro II (Centro)": (-22.9067, -43.2044),
        "Colégio Pedro II (São Cristóvão)": (-22.8997, -43.2217),
        "Colégio Pedro II (Realengo)": (-22.8789, -43.4300),
        "CAp UFRJ (Lagoa)": (-22.9714, -43.2033),
        "Colégio Militar (Tijuca)": (-22.9158, -43.2269),
        "Santo Inácio": (-22.9535, -43.1912),
        "São Bento": (-22.8983, -43.1772),
        "Hosp. Copa D'Or": (-22.9695, -43.1878),
        "Hosp. Barra D'Or": (-22.9942, -43.3637),
        "Hosp. Souza Aguiar": (-22.9077, -43.1908),
        "Hosp. Miguel Couto": (-22.9803, -43.2250),
        "Hosp. Lourenço Jorge": (-22.9992, -43.3663),
        "INCA (Centro)": (-22.9103, -43.1856),
    },
    "areas_sensiveis": {
        "Rocinha": (-22.9934, -43.2547),
        "Vidigal": (-22.9943, -43.2348),
        "Cantagalo/Pavão": (-22.9763, -43.1952),
        "Santa Marta": (-22.9482, -43.1903),
        "Tabajaras": (-22.9619, -43.1936),
        "Babilônia/Chapéu": (-22.9608, -43.1678),
        "Complexo do Lins": (-22.9189, -43.2798),
        "Morro dos Macacos": (-22.9187, -43.2530),
        "Salgueiro/Borel": (-22.9376, -43.2435),
        "Turano": (-22.9268, -43.2133),
        "Mangueira": (-22.9038, -43.2393),
        "Jacarezinho": (-22.8877, -43.2533),
        "Complexo do Alemão": (-22.8587, -43.2725),
        "Complexo da Maré": (-22.8617, -43.2422),
        "Complexo da Penha": (-22.8468, -43.2829),
        "Serrinha": (-22.8681, -43.3323),
        "Chapadão": (-22.8336, -43.3592),
        "Cidade de Deus": (-22.9489, -43.3622),
        "Rio das Pedras": (-22.9737, -43.3283),
        "Vila Kennedy": (-22.8608, -43.4862),
    },
    "seguranca_publica": {
        "2º BPM (Botafogo)": (-22.9525, -43.1868),
        "19º BPM (Copacabana)": (-22.9688, -43.1925),
        "23º BPM (Leblon)": (-22.9863, -43.2210),
        "6º BPM (Tijuca)": (-22.9272, -43.2355),
        "3º BPM (Méier)": (-22.9022, -43.2801),
        "16º BPM (Olaria)": (-22.8465, -43.2625),
        "41º BPM (Irajá)": (-22.8358, -43.3370),
        "9º BPM (Rocha Miranda)": (-22.8569, -43.3422),
        "31º BPM (Barra/Recreio)": (-23.0033, -43.3600),
        "18º BPM (Jacarepaguá)": (-22.9300, -43.3522),
    },
    "mercados_essenciais": {
        "Mundial (Botafogo)": (-22.9497, -43.1865),
        "Mundial (Copacabana)": (-22.9680, -43.1870),
        "Zona Sul (Ipanema)": (-22.9842, -43.1983),
        "Zona Sul (Leblon)": (-22.9860, -43.2250),
        "Pão de Açúcar (Copacabana)": (-22.9682, -43.1855),
        "Pão de Açúcar (Flamengo)": (-22.9360, -43.1765),
        "Guanabara (Tijuca)": (-22.9238, -43.2458),
        "Mundial (Tijuca)": (-22.9254, -43.2343),
        "Guanabara (Vila Isabel)": (-22.9152, -43.2483),
        "Prezunic (Méier)": (-22.8988, -43.2758),
        "Guanabara (Engenho de Dentro)": (-22.8950, -43.2950),
        "Guanabara (Piedade)": (-22.8902, -43.3031),
        "Mundial (Cachambi)": (-22.8880, -43.2750),
        "Guanabara (Penha)": (-22.8400, -43.2800),
        "Guanabara (Barra)": (-23.0062, -43.3389),
        "Mundial (Jd Oceânico)": (-23.0128, -43.3052),
        "Carrefour (Barra)": (-22.9980, -43.3600),
        "Guanabara (Campo Grande)": (-22.9022, -43.5604),
    },
        "feiras_alimentacao": {
        # Fonte: Prefeitura do RJ - Divisão de Feiras
        # Geocodificado via Nominatim (158 feiras)
        "Feira Saude (Sexta/Rua Livramento Do)": (-22.8971835, -43.1841545),
        "Feira Caju (Domingo/Rua Gal. Gurjao)": (-22.8746831, -43.210112),
        "Feira Centro (Sábado/Rua Tadeu Kosciusko)": (-22.9144389, -43.1883697),
        "Feira Centro (Quinta/Rua Conde Lages)": (-22.2800004, -42.5325303),
        "Feira Catumbi (Segunda/Rua Emilia Guimaraes)": (-22.9163437, -43.1968102),
        "Feira Rio Comprido (Quarta/Rua Barao De Sertorio)": (-22.9220818, -43.2124574),
        "Feira Rio Comprido (Sábado/Rua Costa Ferraz)": (-22.9232167, -43.2056131),
        "Feira Cidade Nova (Quinta/Praça Cel. Castelo Branco)": (-22.9120636, -43.1980436),
        "Feira Estacio (Quarta/Rua Sampaio Ferraz)": (-22.9161369, -43.2067705),
        "Feira Sao Cristovao (Quinta/Rua Gal Argolo)": (-22.8957998, -43.2258396),
        "Feira Sao Cristovao (Domingo/Rua Gal Bruce)": (-22.894897, -43.2217056),
        "Feira Santa Teresa (Sexta/Rua Terezina)": (-22.9209485, -43.1882594),
        "Feira Gloria (Domingo/Avenida Augusto Severo)": (-22.9199515, -43.1752504),
        "Feira Laranjeiras (Sábado/Rua Prof. Ortiz Monteiro)": (-22.9419116, -43.1923443),
        "Feira Laranjeiras (Sexta/Viaduto Jardel Filho)": (-22.9344207, -43.1843685),
        "Feira Botafogo (Sábado/Rua Paulo Barreto)": (-22.9544213, -43.1867694),
        "Feira Botafogo (Segunda/Rua Vicente De Souza)": (-22.9458155, -43.1855854),
        "Feira Botafogo (Quarta/Praça Nicaragua)": (-22.9419604, -43.1771205),
        "Feira Botafogo (Sexta/Rua Rodrigo De Brito)": (-22.9560424, -43.1827788),
        "Feira Botafogo (Terça/Rua Barao De Macaubas)": (-22.949421, -43.1926911),
        "Feira Humaita (Quarta/Rua Maria Eugenia)": (-22.9558999, -43.2030147),
        "Feira Urca (Domingo/Praça Tenente Gil Guilherme)": (-22.9446153, -43.1614452),
        "Feira Leme (Segunda/Praça Almte Julio De Noronha)": (-22.961704, -43.1669042),
        "Feira Copacabana (Domingo/Praça Serzedelo Correia)": (-22.9693857, -43.1835612),
        "Feira Copacabana (Quinta/Ronald De Carvalho)": (-22.9629176, -43.1770235),
        "Feira Copacabana (Quarta/Praça Edmundo Bittencourt)": (-22.9669699, -43.1900479),
        "Feira Ipanema (Sexta/Praça Nossa Senhora Da Paz)": (-22.9835431, -43.2059821),
        "Feira Ipanema (Terça/Praça Gal Osorio)": (-22.9852258, -43.1977884),
        "Feira Leblon (Quinta/Praça Nossa Senhora Auxiliadora)": (-22.9784126, -43.2231894),
        "Feira Lagoa (Domingo/Avenida Lineu De Paula Machado)": (-22.9636249, -43.2136968),
        "Feira Jardim Botanico (Sábado/Rua Frei Leandro)": (-22.9636362, -43.2233457),
        "Feira Gavea (Sexta/Praça Santos Dumont)": (-22.9737813, -43.2264527),
        "Feira Praca Da Bandeira (Domingo/Rua Vicente Licinio)": (-22.9112162, -43.2131983),
        "Feira Tijuca (Sexta/Rua Garibaldi)": (-22.9343332, -43.2450677),
        "Feira Tijuca (Sexta/Rua Alzira Brandao)": (-22.9211509, -43.2248084),
        "Feira Tijuca (Segunda/Rua Aguiar)": (-22.9234916, -43.2210815),
        "Feira Tijuca (Terça/Rua Gabriela Prado Maia)": (-22.9247351, -43.2327165),
        "Feira Tijuca (Terça/Praça Prof Pinheiro Guimaraes)": (-22.9393931, -43.2486884),
        "Feira Tijuca (Quarta/Rua Visconde De Figueiredo)": (-22.9227968, -43.2273209),
        "Feira Maracana (Quinta/Rua Moraes E Silva)": (-22.9134587, -43.2229415),
        "Feira Maracana (Sábado/Rua Professor Manoel De Abreu)": (-22.9121619, -43.2311861),
        "Feira Vila Isabel (Terça/Rua Jorge Rudge)": (-22.9122509, -43.2392765),
        "Feira Vila Isabel (Quarta/Rua Mendes Tavares)": (-22.9185766, -43.2539891),
        "Feira Andarai (Domingo/Rua Araripe Junior)": (-22.9257425, -43.2518965),
        "Feira Andarai (Quinta/Rua Silva Teles)": (-22.9225043, -43.2434238),
        "Feira Grajau (Sábado/Rua Duquesa De Bragança)": (-22.9228154, -43.2552025),
        "Feira Grajau (Terça/Rua Mearim)": (-22.9209485, -43.26296),
        "Feira Grajau (Sexta/Avenida Julio Furtado)": (-22.9201725, -43.2655268),
        "Feira Bonsucesso (Terça/Rua Mal Foch)": (-22.8700853, -43.2565995),
        "Feira Ramos (Quinta/Rua Senador Mourao Vieira)": (-22.8593533, -43.2599924),
        "Feira Ramos (Sábado/Rua Felisbelo Freire)": (-22.8498012, -43.2611275),
        "Feira Olaria (Sexta/Rua Antonio Rego)": (-22.8520635, -43.2692573),
        "Feira Olaria (Quarta/Rua Firmino Gameleira)": (-22.8423974, -43.2628323),
        "Feira Penha (Domingo/Rua Belisário Pena)": (-22.8321644, -43.2746776),
        "Feira Penha (Quinta/Rua Gal Silveira Sobrinho)": (-22.8424947, -43.3063587),
        "Feira Penha (Domingo/Rua Macapuri)": (-22.8391217, -43.2709343),
        "Feira Penha (Quarta/Rua Jacui)": (-22.8381127, -43.2764454),
        "Feira Penha (Quinta/Estrada Jose Rucas)": (-22.8431835, -43.280689),
        "Feira Braz De Pina (Sábado/Rua Iricume)": (-22.8309084, -43.318807),
        "Feira Vigario Geral (Sábado/Rua Valentim Magalhaes)": (-22.8092415, -43.3067852),
        "Feira Jardim America (Terça/Rua Franz Liszt)": (-22.810005, -43.323212),
        "Feira Maria Da Graca (Terça/Rua Prof Boscoli)": (-22.8825029, -43.2638344),
        "Feira Del Castilho (Domingo/Rua Bispo Lacerda)": (-22.8772185, -43.2675496),
        "Feira Del Castilho (Sábado/Rua Van Gogh)": (-22.8829164, -43.2730718),
        "Feira Inhauma (Domingo/Rua Dona Emilia)": (-22.8726994, -43.284236),
        "Feira Engenho Da Rainha (Sábado/Rua Mario Ferreira)": (-22.8707625, -43.2951085),
        "Feira Rocha (Sábado/Rua Rocha Do)": (-22.9954898, -44.3037932),
        "Feira Riachuelo (Quinta/Rua Vitor Meireles)": (-22.9026953, -43.255175),
        "Feira Riachuelo (Terça/Rua Doutor Manoel Cotrim)": (-22.8966552, -43.2570585),
        "Feira Engenho Novo (Sexta/Rua Manoel Miranda)": (-22.9090942, -43.2678381),
        "Feira Engenho Novo (Segunda/Rua Grao Para)": (-22.9123404, -43.270428),
        "Feira Lins De Vasconcelos (Sexta/Rua Joaquim Meier)": (-22.9073268, -43.2810463),
        "Feira Meier (Terça/Rua Galdino Pimentel)": (-22.9029852, -43.2870713),
        "Feira Meier (Sexta/Rua Vaz De Caminha)": (-22.90173, -43.2797093),
        "Feira Meier (Quarta/Rua Salvador Pires)": (-22.8941373, -43.2780846),
        "Feira Meier (Quinta/Rua Silva Rabelo)": (-22.9014139, -43.279754),
        "Feira Cachambi (Domingo/Rua Basilio De Brito)": (-22.8887152, -43.2682514),
        "Feira Cachambi (Terça/Rua Odorico Mendes)": (-22.8888337, -43.2824098),
        "Feira Engenho De Dentro (Domingo/Rua Afonso Ferreira)": (-22.890837, -43.2956915),
        "Feira Engenho De Dentro (Quarta/Rua Gustavo Riedel)": (-22.8984412, -43.2999744),
        "Feira Engenho De Dentro (Terça/Rua Catulo Cearence)": (-22.9026591, -43.2967757),
        "Feira Encantado (Sábado/Rua Cruz E Souza)": (-22.9005968, -43.3064781),
        "Feira Piedade (Quarta/Rua Antonio Vargas)": (-22.8811254, -43.3086846),
        "Feira Piedade (Terça/Rua Caminho Do Mateus)": (-22.8926665, -43.3110001),
        "Feira Piedade (Sábado/Rua Teresa Cavalcanti)": (-22.8926665, -43.3110001),
        "Feira Pilares (Quarta/Rua Casemiro De Abreu)": (-22.881006, -43.297209),
        "Feira Vila Kosmos (Sábado/Avenida Vicente De Carvalho)": (-22.8502645, -43.3086478),
        "Feira Vicente De Carvalho (Quarta/Rua Cambuci Do Vale)": (-22.8530137, -43.3186933),
        "Feira Vista Alegre (Sexta/Rua Florania)": (-22.8308303, -43.3172144),
        "Feira Vista Alegre (Domingo/Rua Ponta Pora)": (-22.8280528, -43.3170224),
        "Feira Iraja (Domingo/Rua Marques De Queluz)": (-22.8434875, -43.3259497),
        "Feira Iraja (Sexta/Rua Jose Sombra)": (-22.8261639, -43.3279215),
        "Feira Iraja (Quarta/Avenida Tenente Rebelo)": (-22.8138284, -43.3315294),
        "Feira Iraja (Sexta/Rua Lopes Ferreira)": (-22.8490996, -43.3354951),
        "Feira Campinho (Terça/Rua Ana Teles)": (-22.8834197, -43.3533075),
        "Feira Quintino Bocaiuva (Quarta/Rua Eufrasio Correa)": (-22.8852974, -43.3200725),
        "Feira Cavalcanti (Sábado/Rua Laurindo Filho)": (-22.8691475, -43.3186826),
        "Feira Engenheiro Leal (Quarta/Rua Valerio)": (-22.8743923, -43.3255315),
        "Feira Cascadura (Sexta/Rua Caetano Da Silva)": (-22.8802361, -43.32049),
        "Feira Madureira (Quinta/Rua Henrique Braga)": (-22.8780951, -43.3475508),
        "Feira Madureira (Domingo/Rua Operario Sadock De Sa)": (-22.871658, -43.3371976),
        "Feira Vaz Lobo (Terça/Rua Oliveira Figueiredo)": (-22.857214, -43.3272147),
        "Feira Vaz Lobo (Sábado/Rua Jacina)": (-22.8540722, -43.3268013),
        "Feira Rocha Miranda (Segunda/Rua Rubis)": (-22.8434562, -43.350433),
        "Feira Honorio Gurgel (Sábado/Rua Jurubaiba)": (-22.853217, -43.3588685),
        "Feira Oswaldo Cruz (Quarta/Rua Adelaide Badajos)": (-22.8694433, -43.3483392),
        "Feira Bento Ribeiro (Sexta/Rua Teresa Santos)": (-22.8618814, -43.3602953),
        "Feira Bento Ribeiro (Terça/Rua Obidos)": (-22.8754292, -43.3582163),
        "Feira Bento Ribeiro (Quarta/Rua Sapopemba)": (-22.8666778, -43.3647368),
        "Feira Marechal Hermes (Quinta/Rua Jorge Schmidt)": (-22.8627709, -43.3705907),
        "Feira Ribeira (Sábado/Rua Fernandes Da Fonseca)": (-22.8248988, -43.1692527),
        "Feira Cacuia (Domingo/Rua Sargento Joao Lopes)": (-22.8112067, -43.1916236),
        "Feira Freguesia(Ilha) (Quinta/Rua Aruja)": (-22.7934201, -43.1740699),
        "Feira Jardim Guanabara (Sexta/Avenida Francisco Alves)": (-22.8164185, -43.2002799),
        "Feira Taua (Segunda/Rua Prof Hilariao Da Rocha)": (-22.7975704, -43.1861671),
        "Feira Portuguesa (Quinta/Governador)": (-22.7955649, -43.2104056),
        "Feira Guadalupe (Domingo/Rua Bétula)": (-22.8414181, -43.3780048),
        "Feira Guadalupe (Quinta/Rua Loasa)": (-22.849104, -43.37927),
        "Feira Guadalupe (Quinta/Rua Eneas Martins)": (-22.8350084, -43.3770502),
        "Feira Ricardo De Albuquerque (Domingo/Rua Pereira Da Rocha)": (-22.8422911, -43.4026091),
        "Feira Coelho Neto (Domingo/Rua Ouseley)": (-22.8277261, -43.3496376),
        "Feira Coelho Neto (Terça/Praça Prof Virginia Cidade)": (-22.8308521, -43.3452741),
        "Feira Costa Barros (Quarta/Rua Cel Moreira Cesar)": (-22.8211841, -43.3791273),
        "Feira Jacarepagua (Sábado/Largo Do Anil)": (-22.9531726, -43.3715825),
        "Feira Jacarepagua (Terça/Rua Gal Olivio Uzeda)": (-22.9531726, -43.3715825),
        "Feira Jacarepagua (Quarta/Vossio Brigido, Rua Gal.)": (-22.9531726, -43.3715825),
        "Feira Jacarepagua (Domingo/Avenida Eng. Souza Filho)": (-22.9774627, -43.3341519),
        "Feira Gardenia Azul (Sábado/Avenida Das Lagoas)": (-22.9583617, -43.3445873),
        "Feira Cidade De Deus (Domingo/Rua Edgard Cavaleiro)": (-22.94803, -43.362903),
        "Feira Cidade De Deus (Quarta/Rua Moises)": (-22.9480908, -43.3592755),
        "Feira Freguesia(Jacarepagua) (Terça/Rua Araguaia)": (-22.9327985, -43.3308987),
        "Feira Taquara (Quinta/Rua Ariapo)": (-22.920712, -43.3732435),
        "Feira Taquara (Quarta/Rua José Perigault)": (-22.9347662, -43.3689929),
        "Feira Tanque (Quarta/Rua Alexandre Ramos E Coronel Tedim)": (-22.9151927, -43.3557471),
        "Feira Praca Seca (Domingo/Rua Barão)": (-22.8985621, -43.3525275),
        "Feira Vila Valqueire (Quinta/Rua Das Margaridas)": (-22.8829699, -43.3648548),
        "Feira Vila Valqueire (Domingo/Avenida Jambeiro)": (-22.8818822, -43.3713474),
        "Feira Barra Da Tijuca (Domingo/Praça Sao Perpetuo)": (-23.0124289, -43.3177439),
        "Feira Barra Da Tijuca (Quinta/Praça Sao Perpetuo)": (-23.0124289, -43.3177439),
        "Feira Campo Dos Afonsos (Quarta/Rua Olimpio De Castro)": (-22.8795186, -43.3803636),
        "Feira Magalhaes Bastos (Sexta/Rua Abrantes)": (-22.8755654, -43.4109812),
        "Feira Magalhaes Bastos (Terça/Rua Sao Caetano)": (-22.8813181, -43.4103913),
        "Feira Realengo (Quinta/Rua Magoari)": (-22.8805567, -43.4377955),
        "Feira Realengo (Domingo/Rua Mal Modestino)": (-22.8721608, -43.4319767),
        "Feira Realengo (Sábado/Rua Eunapio Deiro)": (-22.8821456, -43.4258625),
        "Feira Realengo (Domingo/Estrada Manuel Nogueira De Sa)": (-22.8937209, -43.41906),
        "Feira Padre Miguel (Sábado/Rua Helianto)": (-22.8896216, -43.4453999),
        "Feira Bangu (Sábado/Praça Iguatama)": (-22.8753053, -43.4648805),
        "Feira Bangu (Domingo/Rua Prof Clemente Ferreira)": (-22.8777715, -43.4633271),
        "Feira Bangu (Quarta/Rua Mal Marciano)": (-22.8693531, -43.4534184),
        "Feira Bangu (Domingo/Rua Cherburgo)": (-22.87331, -43.4511241),
        "Feira Bangu (Quinta/Rua Urucum)": (-22.8844758, -43.4770386),
        "Feira Senador Camara (Domingo/Rua Carnauba)": (-22.8696986, -43.4866168),
        "Feira Campo Grande (Terça/Rua Campo Maior)": (-22.9102521, -43.5423541),
        "Feira Campo Grande (Domingo/Rua Laudelino Vieira De Campos)": (-22.9009768, -43.5669418),
        "Feira Santa Cruz (Terça/Rua Campeiro-Mor)": (-22.9137209, -43.6888405),
        "Feira Sepetiba (Sábado/Rua Floresta)": (-22.9767037, -43.7029537),
        "Feira Mare (Quarta/Rua Roberto Da Silveira)": (-22.8505356, -43.2414378),
    },
    "cultura_esporte": {
        "Aterro do Flamengo (Esportes)": (-22.9221, -43.1733),
        "Ciclovia da Lagoa": (-22.9734, -43.2114),
        "Cidade das Artes (Barra)": (-22.9992, -43.3663),
        "Teatro Municipal": (-22.9090, -43.1770),
        "Imperator (Méier)": (-22.8988, -43.2758),
        "CCBB (Centro)": (-22.9015, -43.1765),
        "Parque Madureira": (-22.8732, -43.3402),
    },
    "pontos_turisticos": {
        "Cristo Redentor": (-22.9519, -43.2105),
        "Pão de Açúcar": (-22.9492, -43.1560),
        "Escadaria Selarón": (-22.9153, -43.1792),
        "Arcos da Lapa": (-22.9133, -43.1803),
        "Maracanã": (-22.9122, -43.2302),
        "Museu do Amanhã": (-22.8946, -43.1797),
        "Forte de Copacabana": (-22.9862, -43.1870),
        "Santa Teresa": (-22.9195, -43.1870),
    },
}

BAIRROS_TURISTICOS = {
    "Copacabana", "Ipanema", "Leblon", "Botafogo", "Flamengo",
    "Leme", "Catete", "Glória", "Laranjeiras", "Humaitá",
    "Jardim Botânico", "Gávea", "São Conrado", "Urca",
    "Santa Teresa", "Lapa", "Centro",
    "Barra Da Tijuca", "Recreio Dos Bandeirantes",
}