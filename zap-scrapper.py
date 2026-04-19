# Importando as bibliotecas
import pandas as pd
import re
from selenium.webdriver.common.keys import Keys
import random
import re
import time
import logging
import random
from typing import List, Dict, Optional
import undetected_chromedriver as uc 
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# configurações de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

def carregar_cards_zap_imoveis(driver):
    for i in range(30):
        subir_pagina(driver)
        time.sleep(random.uniform(0.1, 2.0))
        descer_pagina(driver)
    
def subir_pagina(driver):
    for i in range(6):
        body_element = driver.find_element(By.TAG_NAME, "body")
        body_element.send_keys(Keys.PAGE_UP)
        #time.sleep(1) # Pequena pausa

def descer_pagina(driver):
    resultado = random.choice([True,False])
    if resultado:
        for i in range(12):
            body_element = driver.find_element(By.TAG_NAME, "body")
            body_element.send_keys(Keys.PAGE_DOWN)
            #time.sleep(2)
    else:
        body_element = driver.find_element(By.TAG_NAME, "body")
        body_element.send_keys(Keys.END)
        
def safe_get_text(element: WebElement, selector: str, default: str = "") -> str:
    """Busca um elemento filho por seletor CSS e retorna seu texto de forma segura."""
    try:
        raw = element.find_element(By.CSS_SELECTOR, selector).get_attribute("textContent") or ""
        # transforma múltiplos espaços, tabs e quebras de linha em um único espaço
        cleaned = re.sub(r"\s+", " ", raw).strip()
        return cleaned.replace(";", ",")
    except NoSuchElementException:
        return default
    except Exception as e:
        logger.error(f"Erro inesperado ao buscar texto com seletor '{selector}': {e}")
        return default

def safe_get_attribute(element: WebElement, selector: str, attribute: str, default: str = "") -> str:
    """Busca um elemento filho por seletor CSS e retorna um atributo de forma segura."""
    try:
        return element.find_element(By.CSS_SELECTOR, selector).get_attribute(attribute) or default
    except NoSuchElementException:
        return default
    except Exception as e:
        logger.error(f"Erro inesperado ao buscar atributo '{attribute}' com seletor '{selector}': {e}")
        return default

def extract_number(text: str, default: int = 0) -> int:
    """Extrai o primeiro número inteiro de uma string, removendo não-dígitos."""
    if not text:
        return default
    # Remove R$, pontos de milhar, espaços e pega apenas os dígitos
    digits = re.sub(r'[^\d]', '', text)
    return int(digits) if digits else default

def extract_decimal_number(text: str, default: float = 0.0) -> float:
    """Extrai um número decimal (IPTU/Cond), tratando vírgula como separador decimal."""
    if not text:
        return default
    # Encontra padrões como R$ 1.234,56 ou R$ 570
    match = re.search(r'R\$\s*([\d\.,]+)', text)
    if match:
        num_str = match.group(1).replace('.', '').replace(',', '.') # Transforma para formato float padrão
        try:
            return float(num_str)
        except ValueError:
            logger.warning(f"Não foi possível converter '{num_str}' para float.")
            return default
    return default

def safe_get(driver, url, retries=3):
    """Tenta carregar página, detecta Cloudflare e faz retry com novo driver."""
    for attempt in range(retries):
        driver = start_driver()
        driver.get(url)
        #time.sleep(1)
        if "Checking your browser" not in driver.title:
            return driver
        logger.warning("Bloqueado pela Cloudflare, trocando driver/proxy...")
        driver.quit()
    raise RuntimeError("Falha ao passar pelo Cloudflare após várias tentativas")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/124.0.6367.80 Mobile/15E148 Safari/604.1"
]

MONTHS_PT = {
    'janeiro':   1,  'fevereiro': 2, 'março':      3,
    'abril':     4,  'maio':      5, 'junho':      6,
    'julho':     7,  'agosto':    8, 'setembro':   9,
    'outubro':  10,  'novembro': 11, 'dezembro':  12,
}

def start_driver() -> uc.Chrome:
    """Inicializa undetected ChromeDriver com opções stealth."""
    options = uc.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    driver = uc.Chrome(options=options)
    driver.set_window_size(1200, 800)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
      "source": """
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['pt-BR','en-US'] });
      """
    })
    return driver

def parse_pt_date_to_iso(pt_date: str) -> str:
    """
    Recebe data em português no formato 'D de mês de YYYY'
    e retorna 'YYYY-MM-DD'. Se o formato não bater, retorna original.
    """
    m = re.search(r'(\d{1,2})\s+de\s+([A-Za-zç]+)\s+de\s+(\d{4})', pt_date, re.IGNORECASE)
    if not m:
        return pt_date  # fallback
    day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
    month = MONTHS_PT.get(month_name)
    if not month:
        return pt_date
    return f"{int(year):04d}-{month:02d}-{int(day):02d}"

def parse_card_casas(card_element: WebElement) -> Optional[Dict]:

    try:
        card_data = {}

        # Área (m²)
        area_text = safe_get_text(card_element, 'li[data-cy="rp-cardProperty-propertyArea-txt"] h3')
        card_data['area_m2'] = extract_number(area_text)
        logger.debug(f"  Área Texto Bruto: '{area_text}', Extraído: {card_data['area_m2']}")

        # Quartos
        quartos_text = safe_get_text(card_element, 'li[data-cy="rp-cardProperty-bedroomQuantity-txt"] h3')
        card_data['quartos'] = extract_number(quartos_text)
        logger.debug(f"  Quartos Texto Bruto: '{quartos_text}', Extraído: {card_data['quartos']}")

        # Banheiros
        banheiros_text = safe_get_text(card_element, 'li[data-cy="rp-cardProperty-bathroomQuantity-txt"] h3')
        card_data['banheiros'] = extract_number(banheiros_text)
        logger.debug(f"  Banheiros Texto Bruto: '{banheiros_text}', Extraído: {card_data['banheiros']}")

        # Vagas
        vagas_text = safe_get_text(card_element, 'li[data-cy="rp-cardProperty-parkingSpacesQuantity-txt"] h3')
        card_data['vagas'] = extract_number(vagas_text)
        logger.debug(f"  Vagas Texto Bruto: '{vagas_text}', Extraído: {card_data['vagas']}")

        # Valor do Imóvel
        price_text = safe_get_text(card_element, 'div[data-cy="rp-cardProperty-price-txt"] p.text-2-25')
        card_data['valor_R$'] = extract_number(price_text) # Renomeei para indicar que é número
        logger.debug(f"  Valor Texto Bruto: '{price_text}', Extraído: {card_data['valor_R$']}")
        
        # Preço por m²
        card_data["preco_m2"] = card_data["valor_R$"] / card_data["area_m2"] if card_data["area_m2"] > 0 else 0.0
        logger.debug(f"  Preço por m²: {card_data['preco_m2']}")

        # URL do Anúncio
        card_data['url'] = safe_get_attribute(card_element, 'a', 'href', 'URL Não Encontrada')

        # Tipo de Destaque
        card_data['destaque'] = safe_get_text(card_element, 'li[data-cy="rp-cardProperty-tag-txt"]', 'Sem Destaque')

        # Imagem Principal
        card_data['imagem_url'] = safe_get_attribute(card_element, 'img', 'src', 'Imagem Não Encontrada')

        # Localização: tipo, bairro e cidade separadamente
        loc_text = safe_get_text(card_element, 'h2[data-cy="rp-cardProperty-location-txt"]')
        # Exemplo: "Casa em Andaraí, Rio de Janeiro"
        tipo, _, loc_part = loc_text.partition(' em ')
        card_data['tipo_imovel_titulo'] = tipo.strip() or None
        # separar bairro e cidade por vírgula
        bairro, sep, cidade = loc_part.partition(',')
        card_data['bairro']       = bairro.strip() if bairro else None
        card_data['cidade']       = cidade.strip() if sep and cidade else None

        # Rua
        card_data['rua'] = safe_get_text(card_element, 'p[data-cy="rp-cardProperty-street-txt"]', 'Rua Não Informada')

        # Condomínio e IPTU
        costs_text = safe_get_text(card_element, 'div[data-cy="rp-cardProperty-price-txt"] p.text-1-75')
        card_data['condominio_R$'] = 0.0
        card_data['iptu_R$'] = 0.0
        if costs_text:
            cond_match = re.search(r'Cond\.\s*(R\$\s*[\d\.,]+)', costs_text, re.IGNORECASE)
            if cond_match:
                card_data['condominio_R$'] = extract_decimal_number(cond_match.group(1))
            iptu_match = re.search(r'IPTU\s*(R\$\s*[\d\.,]+)', costs_text, re.IGNORECASE)
            if iptu_match:
                card_data['iptu_R$'] = extract_decimal_number(iptu_match.group(1))
        logger.debug(f"  Custos Texto Bruto: '{costs_text}', Cond: {card_data['condominio_R$']}, IPTU: {card_data['iptu_R$']}")

        # Timestamp
        card_data['timestamp_coleta'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S') # Formato mais legível

        logger.debug(f"Card parseado com sucesso: {card_data.get('url', 'URL Desconhecida')}")
        return card_data

    except Exception as e:
        logger.exception(f"Erro crítico ao parsear um card. Detalhes: {e}")
        url_on_error = safe_get_attribute(card_element, 'a', 'href', 'URL não recuperável no erro')
        logger.error(f"Erro no card com URL (aproximada): {url_on_error}")
        return None
    
def fetch_detail_with_safe_get(card_data):
    """
    Usa safe_get para criar um driver que já passou pelo Cloudflare,
    carrega a página de detalhe, extrai 'descricao' e 'corretora' e encerra o driver.
    """
    url = card_data.get('url')
    if not url or url.startswith('URL Não'):
        card_data['descricao'] = "Descrição não disponível"
        card_data['corretora'] = "Corretora não disponível"
        card_data['anuncio_criado'] = "Data não disponível"
        card_data['suites'] = "Suites não disponíveis"
        card_data['andar'] = "Andar não disponível"
        card_data['endereco'] = "Endereco não disponível"
        card_data['estado'] = "Estado não disponível"
        card_data['numero'] = "Número não disponível"
        card_data['coordenadas'] = "Coordenadas não encontradas"
        card_data['tem_portaria_24h'] = False
        card_data['tem_vista_pro_mar'] = False
        card_data['tem_interfone'] = False
        card_data['tem_condominio_fechado'] = False
        card_data['tem_piscina'] = False
        card_data['tem_churrasqueira'] = False
        card_data['tem_varanda_gourmet'] = False
        card_data['tem_varanda'] = False
        card_data['tem_armario_embutido'] = False
        card_data['aceita_pet'] = False
        card_data['tem_academia'] = False
        card_data['tem_salao_festas'] = False
        card_data['tem_estacionamento'] = False
        card_data['nota_media'] = 0.0
        card_data['total_avaliacoes'] = 0
        card_data['total_imoveis'] = 0
        return

    try:
        driver_aba = safe_get(None, url) 
        time.sleep(2)
        
        # Estado
        endereco_selector = "p[data-testid='address-info-value']"
        endereco_raw = safe_get_text(driver_aba, endereco_selector)
        card_data['estado'] = endereco_raw.split(",")[-1].split("-")[-1].strip() if endereco_raw else "Estado não disponível"
        
        # Numero
        numero_selector = "p[data-testid='address-info-value']"
        numero_raw = safe_get_text(driver_aba, numero_selector)
        numero = extract_number(numero_raw, "Número não disponível")
        card_data['numero'] = numero
        
        # Suites
        suite_selector = "li[itemprop='numberOfSuites']"
        suite_raw = safe_get_text(driver_aba, suite_selector)
        card_data['suites'] = extract_number(suite_raw) if suite_raw else 0
        
        # Andar
        andar_selector = "li[itemprop='floorLevel']"
        andar_raw = safe_get_text(driver_aba, andar_selector)
        card_data['andar'] = extract_number(andar_raw) if andar_raw else 1
        
        #permite pets?
        pet_selector = "li[itemprop='PETS_ALLOWED']"
        pet_raw = safe_get_text(driver_aba, pet_selector)
        if pet_raw:
            card_data['aceita_pet'] = True
        else:
            card_data['aceita_pet'] = False   
        
        #portaria 24h
        portaria_selector = "li[itemprop='CONCIERGE_24H']"
        portaria_raw = safe_get_text(driver_aba, portaria_selector)
        if portaria_raw:
            card_data['tem_portaria_24h'] = True
        else:
            card_data['tem_portaria_24h'] = False
          
        #vista pro mar
        mar_selector = "li[itemprop='SEA_VIEW']"
        mar_raw = safe_get_text(driver_aba, mar_selector)
        if mar_raw:  #Vista para o mar
            card_data['tem_vista_pro_mar'] = True
        else:
            card_data['tem_vista_pro_mar'] = False  
            
        #interfone
        interfone_selector = "li[itemprop='INTERCOM']"
        interfone_raw = safe_get_text(driver_aba, interfone_selector)
        if interfone_raw:  #== "Interfone"
            card_data['tem_interfone'] = True
        else:
            card_data['tem_interfone'] = False
        
        #condominio fechado
        condominio_selector = "li[itemprop='GATED_COMMUNITY']"
        condominio_raw = safe_get_text(driver_aba, condominio_selector)
        if condominio_raw:
            card_data['tem_condominio_fechado'] = True
        else:
            card_data['tem_condominio_fechado'] = False
        
        # piscina
        piscina_selector = "li[itemprop='POOL']"
        piscina_raw = safe_get_text(driver_aba, piscina_selector)
        if piscina_raw:
            card_data['tem_piscina'] = True
        else:
            card_data['tem_piscina'] = False
            
        # churrasqueira
        churrasqueira_selector = "li[itemprop='BARBECUE_GRILL']"
        churrasqueira_raw = safe_get_text(driver_aba, churrasqueira_selector)
        if churrasqueira_raw:
            card_data['tem_churrasqueira'] = True
        else:
            card_data['tem_churrasqueira'] = False    
            
        # Varanda gourmet
        varanda_selector = "li[itemprop='GOURMET_BALCONY']"
        varanda_raw = safe_get_text(driver_aba, varanda_selector)
        if varanda_raw:
            card_data['tem_varanda_gourmet'] = True
        else:
            card_data['tem_varanda_gourmet'] = False
            
        # Varanda
        varanda_selector = "li[itemprop='BALCONY']"
        varanda_raw = safe_get_text(driver_aba, varanda_selector)
        if varanda_raw:
            card_data['tem_varanda'] = True
        else:
            card_data['tem_varanda'] = False
            
        # Armario embutido
        armario_selector = "li[itemprop='BUILTIN_WARDROBE']"
        armario_raw = safe_get_text(driver_aba, armario_selector)
        if armario_raw:
            card_data['tem_armario_embutido'] = True
        else:
            card_data['tem_armario_embutido'] = False
            
        # Estacionamento
        estacionamento_selector = "li[itemprop='PARKING']"
        estacionamento_raw = safe_get_text(driver_aba, estacionamento_selector)
        if estacionamento_raw:
            card_data['tem_estacionamento'] = True
        else:
            card_data['tem_estacionamento'] = False
            
        # Academia
        academia_selector = "li[itemprop='GYM']"
        academia_raw = safe_get_text(driver_aba, academia_selector)
        if academia_raw:
            card_data['tem_academia'] = True
        else:
            card_data['tem_academia'] = False
            
        # Salão de festas
        salao_selector = "li[itemprop='PARTY_HALL']"
        salao_raw = safe_get_text(driver_aba, salao_selector)
        if salao_raw:
            card_data['tem_salao_festas'] = True
        else:
            card_data['tem_salao_festas'] = False       
        
        # Corretora
        card_data["corretora"]  = safe_get_text(driver_aba, "a[data-testid='official-store-redirect-link']")

        # Avaliações
        rating_selector = '[data-testid="rating-container"] .rating-container__text'
        rating_raw = safe_get_text(driver_aba, rating_selector)
        m = re.search(r"(\d+(?:\.\d+)?)\/\d+\s*\((\d+)", rating_raw)
        card_data['nota_media'] = float(m.group(1)) if m else 0
        card_data['total_avaliacoes'] = int(m.group(2)) if m else 0

        # Total imóveis
        total_selector = "p.properties-container"
        total_raw = safe_get_text(driver_aba, total_selector)
        card_data['total_imoveis'] = extract_number(total_raw)

        # Anuncio criado
        date_raw = safe_get_text(driver_aba, '[data-testid="listing-created-date"]')
        created_part = date_raw.split(",")[0].replace("Anúncio criado em", "").strip()
        card_data['anuncio_criado'] = parse_pt_date_to_iso(created_part)  
       
        # Descrição
        desc_selector = "p[data-testid='description-content']"
        desc_raw = safe_get_text(driver_aba, desc_selector)
        card_data['descricao'] = desc_raw        
        
        # Endereco
        desc_selector = "p[data-testid='location']"
        desc_raw = safe_get_text(driver_aba, desc_selector)
        card_data['endereco'] = desc_raw    

        try:
            botao_localizacao = WebDriverWait(driver_aba, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-testid='address-info-button']"))
            )

            # Scroll para o botão ser visível e clicável
            driver_aba.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", botao_localizacao)
            time.sleep(0.5) # Pequena pausa após scroll
            driver_aba.execute_script("arguments[0].click();", botao_localizacao)
            time.sleep(1) # Pausa para dar tempo do iframe iniciar o carregamento

            # 2. Espera o IFRAME do mapa aparecer
            WebDriverWait(driver_aba, 15).until(
                EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR, "iframe[data-testid='map-iframe']"))
            )
        
            place_name_el = WebDriverWait(driver_aba, 10).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div.place-name"))
            )
            coordenadas_texto = place_name_el.get_attribute("textContent").strip()
            card_data['coordenadas'] = coordenadas_texto
            
        except Exception as e_inner:
                card_data['coordenadas'] = "Coordenadas não encontradas"
                 
    except Exception as e:
        logger.error(f"Erro ao buscar detalhe {url}: {e}")
    finally:
        driver_aba.quit()  # garante encerramento do driver de detalhe
    

def scrape_pages(pages: int) -> pd.DataFrame:
    #driver = start_driver()
    cards: List[Dict] = []
    base_url = "https://www.zapimoveis.com.br/venda/imoveis/rj+rio-de-janeiro+zona-sul/" #"https://www.zapimoveis.com.br/venda/apartamentos/"
    try:
        for i in range(1, pages + 1):
            url = f"{base_url}?pagina={i}"
            logger.info(f"Abrindo página {i}: {url}")
            try:
                #driver.get(url)
                driver = safe_get(None, url)
                # espera até container de cards aparecer
                carregar_cards_zap_imoveis(driver)
                
                elems = driver.find_elements(By.CSS_SELECTOR, 'li[data-cy="rp-property-cd"]')
                logger.info(f"Encontrados {len(elems)} anúncios na página {i}")
                for e in elems:
                    card = parse_card_casas(e)
                    if card:
                        # extrai detalhes na página do anúncio
                        fetch_detail_with_safe_get(card)
                        cards.append(card)
                        time.sleep(2)
                    
                driver.quit()
            except TimeoutException:
                logger.error(f"Timeout ao carregar página {i}")
    except WebDriverException as e:
        logger.exception("Erro no WebDriver:")
    finally:
        driver.quit()
    df = pd.DataFrame(cards)
    return df

if __name__ == "__main__":
    NUM_PAGES = 1
    df = scrape_pages(NUM_PAGES)
    logger.info(f"Total de anúncios coletados: {len(df)}")
    df.to_csv("Zona-Sul-RJ-2026-dataset-imoveis-geral.csv", sep=";", index=False, encoding="utf-8-sig")
    logger.info("Dados salvos em 'Zona-Sul-RJ-2026-dataset-imoveis-geral.csv'")