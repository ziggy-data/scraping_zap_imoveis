import pandas as pd
import re
import time
import logging
import random
import threading
import glob
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional
import undetected_chromedriver as uc 
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- CONFIGURAÇÕES ---
MAX_WORKERS = 7  
DRIVER_LOCK = threading.Lock() 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s %(message)s",
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÃO DAS ZONAS ---
ZONAS_CONFIG = [
    {"nome": "Zona Norte", "slug": "rj+rio-de-janeiro+zona-norte", "split": True}, 
    {"nome": "Zona Sul",   "slug": "rj+rio-de-janeiro+zona-sul",   "split": True},
    {"nome": "Zona Oeste", "slug": "rj+rio-de-janeiro+zona-oeste", "split": True},
]

MONTHS_PT = {
    'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4, 'maio': 5, 'junho': 6,
    'julho': 7, 'agosto': 8, 'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12,
}

# --- FUNÇÕES DE LIMPEZA E TRATAMENTO ---

def aplicar_regras_qualidade(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"🧼 Iniciando tratamento de qualidade. Dados brutos: {len(df)}")
    
    # Canonicalização
    df['id_imovel'] = df['url'].apply(lambda x: re.search(r'id-(\d+)', x).group(1) if re.search(r'id-(\d+)', x) else x)
    df.drop_duplicates(subset=['id_imovel'], keep='first', inplace=True)
    
    # Conversão Numérica
    cols_num = ['valor_R$', 'area_m2', 'condominio_R$', 'iptu_R$', 'quartos', 'vagas', 'banheiros', 'suites']
    for col in cols_num:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Filtro de Sanidade
    mask_valido = (df['valor_R$'] > 10000) & (df['area_m2'] >= 10)
    df_clean = df[mask_valido].copy()
    
    # Engenharia de Features
    df_clean['preco_m2'] = (df_clean['valor_R$'] / df_clean['area_m2']).round(2)
    
    # Outliers
    q_low = df_clean['preco_m2'].quantile(0.01)
    q_hi  = df_clean['preco_m2'].quantile(0.99)
    df_final = df_clean[(df_clean['preco_m2'] > q_low) & (df_clean['preco_m2'] < q_hi)]
    
    # Padronização de Texto
    colunas_texto = ['bairro', 'tipo', 'cidade', 'descricao', 'rua', 'corretora']
    for col in colunas_texto:
        if col in df_final.columns:
            df_final[col] = df_final[col].astype(str).str.strip().str.title()

    logger.info(f"✨ Dados qualificados: {len(df_final)} registros finais.")
    return df_final

def calcular_media_inteligente(df: pd.DataFrame, min_amostras_rua: int = 3) -> pd.DataFrame:
    logger.info("🧠 Calculando médias inteligentes (Hierarquia Rua > Bairro)...")
    
    df['preco_m2_real'] = df.apply(lambda row: row['valor_R$'] / row['area_m2'] if row['area_m2'] > 0 else 0, axis=1)
    df_calc = df[df['preco_m2_real'] > 100].copy()

    # Médias
    stats_bairro = df_calc.groupby('bairro')['preco_m2_real'].mean().reset_index().rename(columns={'preco_m2_real': 'media_bairro_m2'})
    stats_rua = df_calc.groupby(['bairro', 'rua'])['preco_m2_real'].agg(['mean', 'count']).reset_index().rename(columns={'mean': 'media_rua_m2', 'count': 'qtd_amostras_rua'})

    df_final = pd.merge(df, stats_bairro, on='bairro', how='left')
    df_final = pd.merge(df_final, stats_rua, on=['bairro', 'rua'], how='left')
    
    def definir_preco_referencia(row):
        if not row['rua'] or row['rua'] in ["Rua Não Informada", "Endereço Não Disponível", "Rua Não Informada"]:
            return row['media_bairro_m2'], "Bairro (Rua desconhecida)"
        if row['qtd_amostras_rua'] >= min_amostras_rua:
            return row['media_rua_m2'], "Rua"
        return row['media_bairro_m2'], "Bairro (Amostra insuficiente na rua)"

    df_final[['preco_m2_referencia', 'origem_referencia']] = df_final.apply(lambda row: pd.Series(definir_preco_referencia(row)), axis=1)
    df_final['preco_m2_referencia'] = df_final['preco_m2_referencia'].round(2)
    df_final['diferenca_percentual'] = (((df_final['preco_m2_real'] - df_final['preco_m2_referencia']) / df_final['preco_m2_referencia']) * 100).round(1)

    return df_final

# --- FUNÇÕES UTILITÁRIAS DE EXTRAÇÃO (DO CÓDIGO ANTIGO) ---

def extract_number(text: str, default: int = 0) -> int:
    if not text: return default
    digits = re.sub(r'[^\d]', '', text)
    return int(digits) if digits else default

def extract_decimal_number(text: str, default: float = 0.0) -> float:
    if not text: return default
    match = re.search(r'R\$\s*([\d\.,]+)', text)
    if match:
        num_str = match.group(1).replace('.', '').replace(',', '.') 
        try: return float(num_str)
        except: return default
    return default

def parse_pt_date_to_iso(pt_date: str) -> str:
    m = re.search(r'(\d{1,2})\s+de\s+([A-Za-zç]+)\s+de\s+(\d{4})', pt_date, re.IGNORECASE)
    if not m: return pt_date 
    day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
    month = MONTHS_PT.get(month_name)
    if not month: return pt_date
    return f"{int(year):04d}-{month:02d}-{int(day):02d}"

def safe_get_text(driver, selector: str, default: str = "") -> str:
    try:
        el = driver.find_element(By.CSS_SELECTOR, selector)
        raw = el.get_attribute("textContent") or ""
        return re.sub(r"\s+", " ", raw).strip()
    except Exception:
        return default

def safe_get_attribute(element, selector: str, attribute: str, default: str = "") -> str:
    try:
        return element.find_element(By.CSS_SELECTOR, selector).get_attribute(attribute) or default
    except Exception:
        return default

# --- NAVEGAÇÃO ---
def start_driver() -> uc.Chrome:
    with DRIVER_LOCK:
        options = uc.ChromeOptions()
        options.add_argument("--disable-blink-features=AutomationControlled")
        driver = uc.Chrome(options=options)
        driver.set_window_size(1100, 800)
    return driver

def scroll_pagina_listagem(driver):
    for _ in range(15): # Mais scrolls para garantir carregamento
        try:
            body = driver.find_element(By.TAG_NAME, "body")
            body.send_keys(Keys.PAGE_DOWN)
            time.sleep(random.uniform(0.2, 0.6))
        except: break
    try:
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
        time.sleep(2)
    except: pass

# --- EXTRAÇÃO (FUSÃO DA LÓGICA ANTIGA COM A ESTRUTURA NOVA) ---

def parse_card_resumo(card_element: WebElement) -> Optional[Dict]:
    """Extrai dados da listagem (incluindo Cond/IPTU/Rua do código antigo)"""
    try:
        data = {}
        data['url'] = safe_get_attribute(card_element, 'a', 'href', '')
        if not data['url']: return None

        # Dados Básicos
        price_txt = safe_get_text(card_element, 'div[data-cy="rp-cardProperty-price-txt"] p.text-2-25')
        data['valor_R$'] = extract_number(price_txt)
        data['area_m2'] = extract_number(safe_get_text(card_element, 'li[data-cy="rp-cardProperty-propertyArea-txt"] h3'))
        data['quartos'] = extract_number(safe_get_text(card_element, 'li[data-cy="rp-cardProperty-bedroomQuantity-txt"] h3'))
        data['vagas']   = extract_number(safe_get_text(card_element, 'li[data-cy="rp-cardProperty-parkingSpacesQuantity-txt"] h3'))
        data['banheiros'] = extract_number(safe_get_text(card_element, 'li[data-cy="rp-cardProperty-bathroomQuantity-txt"] h3'))
        
        # Localização (Bairro/Cidade)
        loc_text = safe_get_text(card_element, 'h2[data-cy="rp-cardProperty-location-txt"]')
        if " em " in loc_text:
            data['tipo'], _, resto = loc_text.partition(' em ')
            bairro, _, cidade = resto.partition(',')
            data['bairro'] = bairro.strip()
            data['cidade'] = cidade.strip()
        else:
            data['tipo'] = "Indefinido"
            data['bairro'] = loc_text
            data['cidade'] = "Rio de Janeiro"

        # Rua (Do código antigo)
        data['rua'] = safe_get_text(card_element, 'p[data-cy="rp-cardProperty-street-txt"]', 'Rua Não Informada')

        # Custos (Cond/IPTU da listagem - Do código antigo)
        costs_text = safe_get_text(card_element, 'div[data-cy="rp-cardProperty-price-txt"] p.text-1-75')
        data['condominio_R$'] = 0.0
        data['iptu_R$'] = 0.0
        if costs_text:
            cond_match = re.search(r'Cond\.\s*(R\$\s*[\d\.,]+)', costs_text, re.IGNORECASE)
            if cond_match: data['condominio_R$'] = extract_decimal_number(cond_match.group(1))
            iptu_match = re.search(r'IPTU\s*(R\$\s*[\d\.,]+)', costs_text, re.IGNORECASE)
            if iptu_match: data['iptu_R$'] = extract_decimal_number(iptu_match.group(1))

        # Extras
        data['destaque'] = safe_get_text(card_element, 'li[data-cy="rp-cardProperty-tag-txt"]', 'Sem Destaque')
        data['imagem_url'] = safe_get_attribute(card_element, 'img', 'src', 'Imagem Não Encontrada')

        return data
    except Exception:
        return None

def enriquecer_detalhes(driver, card_data: Dict):
    """
    Entra na página e pega TUDO (Features completas, Mapa, Datas - Do código antigo)
    """
    url = card_data.get('url')
    if not url: return

    try:
        driver.get(url)
        if "Checking your browser" in driver.title:
            time.sleep(5)
        time.sleep(2)

        # Descrição e Endereço Básico
        card_data['descricao'] = safe_get_text(driver, "p[data-testid='description-content']")
        end_raw = safe_get_text(driver, "p[data-testid='address-info-value']")
        card_data['endereco_completo'] = end_raw
        card_data['numero'] = extract_number(end_raw)
        
        # Corretora
        card_data["corretora"] = safe_get_text(driver, "a[data-testid='official-store-redirect-link']")

        # Suites e Andar
        card_data['suites'] = extract_number(safe_get_text(driver, "li[itemprop='numberOfSuites']"))
        card_data['andar'] = extract_number(safe_get_text(driver, "li[itemprop='floorLevel']"))
        
        # --- FEATURES COMPLETAS (Do código antigo) ---
        features_map = {
            'aceita_pet': "li[itemprop='PETS_ALLOWED']",
            'tem_portaria_24h': "li[itemprop='CONCIERGE_24H']",
            'tem_vista_pro_mar': "li[itemprop='SEA_VIEW']",
            'tem_interfone': "li[itemprop='INTERCOM']",
            'tem_condominio_fechado': "li[itemprop='GATED_COMMUNITY']",
            'tem_piscina': "li[itemprop='POOL']",
            'tem_churrasqueira': "li[itemprop='BARBECUE_GRILL']",
            'tem_varanda_gourmet': "li[itemprop='GOURMET_BALCONY']",
            'tem_varanda': "li[itemprop='BALCONY']",
            'tem_armario_embutido': "li[itemprop='BUILTIN_WARDROBE']",
            'tem_estacionamento': "li[itemprop='PARKING']",
            'tem_academia': "li[itemprop='GYM']",
            'tem_salao_festas': "li[itemprop='PARTY_HALL']",
            'metro_proximo': "li[itemprop='SUBWAY']" 
        }

        for key, selector in features_map.items():
            val_raw = safe_get_text(driver, selector)
            card_data[key] = bool(val_raw)

        # --- AVALIAÇÕES E DATAS (Do código antigo) ---
        rating_raw = safe_get_text(driver, '[data-testid="rating-container"] .rating-container__text')
        m = re.search(r"(\d+(?:\.\d+)?)\/\d+\s*\((\d+)", rating_raw)
        card_data['nota_media'] = float(m.group(1)) if m else 0.0
        card_data['total_avaliacoes'] = int(m.group(2)) if m else 0

        date_raw = safe_get_text(driver, '[data-testid="listing-created-date"]')
        if date_raw:
            created_part = date_raw.split(",")[0].replace("Anúncio criado em", "").strip()
            card_data['anuncio_criado'] = parse_pt_date_to_iso(created_part)
        else:
            card_data['anuncio_criado'] = pd.Timestamp.now().strftime('%Y-%m-%d')

        # --- MAPA / COORDENADAS (Lógica Robusta do código antigo) ---
        try:
            # 1. Clica no botão
            botao_localizacao = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-testid='location']"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", botao_localizacao)
            #time.sleep(0.5)
            driver.execute_script("arguments[0].click();", botao_localizacao)

            # 2. Espera Iframe e muda contexto
            WebDriverWait(driver, 8).until(
                EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR, "iframe[data-testid='map-iframe']"))
            )
            
            # 3. Pega o texto
            place_el = WebDriverWait(driver, 5).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div.place-name"))
            )
            card_data['coordenadas'] = place_el.get_attribute("textContent").strip()
            # 4. Volta contexto
            driver.switch_to.default_content()
            
        except Exception:
            card_data['coordenadas'] = "Não encontradas"
            driver.switch_to.default_content() # Garante volta mesmo se der erro

    except Exception as e:
        logger.warning(f"Erro detalhe {url}: {e}")

# --- WORKER ---

def processar_tarefa(tarefa: Dict):
    nome_zona = tarefa['zona']['nome']
    slug = tarefa['zona']['slug']
    paginas_range = tarefa['paginas']
    worker_id = tarefa['worker_id']
    
    label = f"{nome_zona} [{worker_id}]"
    logger.info(f"🚀 Iniciando worker: {label}")
    
    driver = None
    dados_coletados = []
    filename_partial = f"parcial_{slug}_{worker_id}.csv"
    
    try:
        time.sleep(random.uniform(2, 6))
        driver = start_driver()
        base_url = f"https://www.zapimoveis.com.br/venda/imoveis/{slug}/"
        
        for pagina in paginas_range:
            url_pag = f"{base_url}?pagina={pagina}"
            logger.info(f"[{label}] Acessando página {pagina}...")
            
            try:
                driver.get(url_pag)
                if "Checking your browser" in driver.title:
                    time.sleep(10)
                scroll_pagina_listagem(driver)
                
                card_elements = driver.find_elements(By.CSS_SELECTOR, 'li[data-cy="rp-property-cd"]')
                logger.info(f"[{label}] Pág {pagina}: {len(card_elements)} cards encontrados.")
                
                cards_da_pagina = []
                for el in card_elements:
                    resumo = parse_card_resumo(el)
                    if resumo: cards_da_pagina.append(resumo)
                
                for card in cards_da_pagina:
                    enriquecer_detalhes(driver, card)
                    dados_coletados.append(card)
                    
                    if len(dados_coletados) % 10 == 0:
                        pd.DataFrame(dados_coletados).to_csv(filename_partial, sep=";", index=False, encoding="utf-8-sig")

            except Exception as e:
                logger.error(f"[{label}] Erro pág {pagina}: {e}")
                
    except Exception as e:
        logger.error(f"[{label}] Erro fatal: {e}")
    finally:
        if driver:
            try: driver.quit()
            except: pass
            
    if dados_coletados:
        df_final = pd.DataFrame(dados_coletados)
        filename_final = f"Final_{nome_zona.replace(' ', '')}_{worker_id}.csv"
        df_final.to_csv(filename_final, sep=";", index=False, encoding="utf-8-sig")
        df_final.to_csv(filename_partial, sep=";", index=False, encoding="utf-8-sig")
        logger.info(f"✅ [{label}] Concluído! {len(dados_coletados)} itens.")
    else:
        logger.warning(f"⚠️ [{label}] Nada coletado.")

# --- MAIN ---

if __name__ == "__main__":
    
    NUM_PAGINAS_TOTAL = 100 # Configure aqui suas páginas
    tarefas = []
    
    for zona in ZONAS_CONFIG:
        if zona.get('split') is True:
            tarefas.append({"zona": zona, "paginas": range(1, NUM_PAGINAS_TOTAL + 1, 2), "worker_id": "impar"})
            tarefas.append({"zona": zona, "paginas": range(2, NUM_PAGINAS_TOTAL + 1, 2), "worker_id": "par"})
        else:
            tarefas.append({"zona": zona, "paginas": range(1, NUM_PAGINAS_TOTAL + 1), "worker_id": "unico"})

    logger.info(f"--- INICIANDO {len(tarefas)} TAREFAS EM PARALELO ---")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(processar_tarefa, t) for t in tarefas]
        for future in as_completed(futures):
            try: future.result()
            except: pass
                
    logger.info("--- UNIFICANDO E TRATANDO DADOS ---")
    try:
        all_files = glob.glob("Final_*.csv")
        if all_files:
            df_list = [pd.read_csv(f, sep=";") for f in all_files]
            df_full = pd.concat(df_list, ignore_index=True)
            
            # Limpeza e Inteligência
            df_tratado = aplicar_regras_qualidade(df_full) 
            df_inteligente = calcular_media_inteligente(df_tratado, min_amostras_rua=3)
            
            df_inteligente.to_csv("DATASET_RIO_GOLD_INTELIGENTE.csv", sep=";", index=False, encoding="utf-8-sig")
            
            logger.info("📊 Análise Completa Gerada.")
            logger.info(f"Arquivo salvo: DATASET_RIO_GOLD_INTELIGENTE.csv com {len(df_inteligente)} imóveis.")
        else:
            logger.warning("Nenhum dado encontrado.")
    except Exception as e:
        logger.error(f"Erro no tratamento final: {e}")