"""
Scraper core: produtor (listagem) e consumidor (detalhes).
Implementa dedup thread-safe, retry e save-as-you-go.
"""
import os as _os, sys as _sys
_DIR = _os.path.dirname(_os.path.abspath(__file__))
if _DIR not in _sys.path:
    _sys.path.insert(0, _DIR)

import csv
import os
import re
import time
import random
import threading
import queue
from typing import Dict, Optional

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from config import (
    logger, SEL, MAP_IMOVEL, MAP_CONDOMINIO, CSV_FIELDNAMES,
    DRIVER_RESTART_INTERVAL,
)
from driver import start_driver, scroll_inteligente, restart_driver_safe
from utils import (
    extract_number, extract_decimal_number, safe_get_text,
    safe_get_attribute, parse_pt_date_to_iso, geocode_fallback_nominatim,
    retry,
)

# ==============================================================================
#  ESTADO COMPARTILHADO (Thread-Safe)
# ==============================================================================

LINK_QUEUE: queue.Queue = queue.Queue()
PRODUCERS_DONE = threading.Event()

# Deduplicação de URLs para evitar processar o mesmo imóvel 2x
_SEEN_URLS: set = set()
_SEEN_LOCK = threading.Lock()

# Contadores globais para monitoramento
_stats_lock = threading.Lock()
_stats = {"enqueued": 0, "processed": 0, "errors": 0, "skipped_dup": 0}


def get_stats() -> dict:
    with _stats_lock:
        return dict(_stats)


def _inc_stat(key: str, n: int = 1):
    with _stats_lock:
        _stats[key] += n


# ==============================================================================
#  PARSE DE CARD (LISTAGEM)
# ==============================================================================

def parse_card_resumo(card_element) -> Optional[Dict]:
    """Extrai dados resumidos de um card de listagem."""
    try:
        data = {}
        data["url"] = safe_get_attribute(card_element, "a", "href", "")
        if not data["url"]:
            return None

        # Preço — tenta múltiplos seletores (ZAP muda as classes frequentemente)
        price_txt = ""
        price_selectors = [
            SEL.card_price,                                          # Original: p.text-2-25
            'div[data-cy="rp-cardProperty-price-txt"] p:first-child',  # Primeiro <p> do bloco
            'div[data-cy="rp-cardProperty-price-txt"]',                # O bloco todo (pega tudo)
            'p[data-cy="rp-cardProperty-price-txt"]',                  # Caso mude pra <p> direto
            '[class*="price"] p:first-child',                          # Qualquer classe com "price"
        ]
        for sel in price_selectors:
            price_txt = safe_get_text(card_element, sel)
            if price_txt and extract_number(price_txt) > 0:
                break

        data["valor_R$"] = extract_number(price_txt)

        # Métricas básicas
        data["area_m2"] = extract_number(safe_get_text(card_element, SEL.card_area))
        data["quartos"] = extract_number(safe_get_text(card_element, SEL.card_quartos))
        data["vagas"] = extract_number(safe_get_text(card_element, SEL.card_vagas))
        data["banheiros"] = extract_number(safe_get_text(card_element, SEL.card_banheiros))

        # Localização
        loc_text = safe_get_text(card_element, SEL.card_location)
        if " em " in loc_text:
            data["tipo"], _, resto = loc_text.partition(" em ")
            bairro, _, cidade = resto.partition(",")
            data["bairro"] = bairro.strip()
            data["cidade"] = cidade.strip()
        else:
            data["tipo"] = "Indefinido"
            data["bairro"] = loc_text
            data["cidade"] = "Rio de Janeiro"

        data["rua"] = safe_get_text(card_element, SEL.card_street, "Rua Não Informada")

        # Custos
        costs_text = safe_get_text(card_element, SEL.card_costs)
        data["condominio_R$"] = 0.0
        data["iptu_R$"] = 0.0
        if costs_text:
            cond_match = re.search(r"Cond\.\s*(R\$\s*[\d\.,]+)", costs_text, re.IGNORECASE)
            if cond_match:
                data["condominio_R$"] = extract_decimal_number(cond_match.group(1))
            iptu_match = re.search(r"IPTU\s*(R\$\s*[\d\.,]+)", costs_text, re.IGNORECASE)
            if iptu_match:
                data["iptu_R$"] = extract_decimal_number(iptu_match.group(1))

        data["destaque"] = safe_get_text(card_element, SEL.card_tag, "Sem Destaque")
        data["imagem_url"] = safe_get_attribute(card_element, "img", "src", "")

        return data
    except Exception:
        return None


# ==============================================================================
#  ENRIQUECIMENTO DE DETALHES (CONSUMIDOR)
# ==============================================================================

def _parse_dias_relativos(texto: str, tipo: str) -> int:
    """
    Extrai dias a partir de texto relativo como 'Publicado há 3 dias, atualizado há 1 dia.'
    
    Args:
        texto: Texto completo da section (ex: "Publicado há 1 dia, atualizado há 1 dia.")
        tipo: "publicado" ou "atualizado"
    Returns:
        Número de dias (int). 0 se não encontrar.
    """
    texto_lower = texto.lower()

    # Busca o trecho correspondente ao tipo
    pattern = rf'{tipo}\s+h[aá]\s+(\d+)\s+(dia|dias|semana|semanas|m[eê]s|meses|ano|anos|hora|horas|minuto|minutos)'
    match = re.search(pattern, texto_lower)
    if not match:
        return 0

    valor = int(match.group(1))
    unidade = match.group(2)

    if "hora" in unidade or "minuto" in unidade:
        return 0  # Menos de 1 dia
    elif "dia" in unidade:
        return valor
    elif "semana" in unidade:
        return valor * 7
    elif "mes" in unidade or "mês" in unidade or "meses" in unidade:
        return valor * 30
    elif "ano" in unidade:
        return valor * 365

    return 0


def _processar_aba(driver, painel_id: str, feature_map: dict, card_data: dict):
    """Extrai amenidades de uma aba específica (Imóvel ou Condomínio)."""
    try:
        painel = driver.find_element(By.ID, painel_id)

        # Tenta expandir "Ver mais" dentro do painel
        try:
            btn = painel.find_element(By.CSS_SELECTOR, SEL.detail_expand_btn)
            if btn.is_displayed():
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(0.4)
        except Exception:
            pass

        # Verifica cada amenidade
        for key, selector in feature_map.items():
            try:
                painel.find_element(By.CSS_SELECTOR, selector)
                card_data[key] = True
            except Exception:
                card_data[key] = False
    except Exception:
        for key in feature_map:
            card_data[key] = False


@retry(max_attempts=2, delay=1.5, backoff=2.0)
def enriquecer_detalhes(driver, card_data: Dict):
    """
    Acessa a página de detalhes e enriquece o dicionário do imóvel.
    Decorado com @retry para resiliência.
    """
    url = card_data.get("url")
    if not url:
        return

    driver.get(url)

    # Cloudflare check
    if "Checking your browser" in driver.title:
        time.sleep(6)

    time.sleep(1.2)

    # --- DADOS BÁSICOS ---
    card_data["descricao"] = safe_get_text(driver, SEL.detail_desc)

    end_raw = safe_get_text(driver, SEL.detail_address)
    if not end_raw:
        end_raw = safe_get_text(driver, SEL.detail_address_alt)
    card_data["endereco_completo"] = end_raw
    card_data["numero"] = extract_number(end_raw)
    card_data["corretora"] = safe_get_text(driver, SEL.detail_broker)

    # --- PREÇO FALLBACK (se o card não extraiu o preço) ---
    if not card_data.get("valor_R$") or card_data["valor_R$"] == 0:
        price_detail_selectors = [
            'h3[data-testid="price-info-value"]',
            '[data-testid="price-info-value"]',
            'div[data-testid="price-info"] h3',
            'h3[class*="price"]',
            'span[class*="price"]',
        ]
        for sel in price_detail_selectors:
            txt = safe_get_text(driver, sel)
            val = extract_number(txt)
            if val > 0:
                card_data["valor_R$"] = val
                break

    # Numéricos do cabeçalho
    card_data["suites"] = extract_number(safe_get_text(driver, "li[itemprop='numberOfSuites']"))
    card_data["quartos"] = extract_number(safe_get_text(driver, "li[itemprop='numberOfRooms']"))
    card_data["andar"] = extract_number(safe_get_text(driver, "li[itemprop='floorLevel']"))

    # --- TAGS DE CLASSIFICAÇÃO (tipo, status, negócio) ---
    card_data["tipo_imovel"] = safe_get_text(driver, SEL.detail_tipo_imovel, "Não informado")
    card_data["status_construcao"] = safe_get_text(driver, SEL.detail_status_construcao, "Pronto")
    card_data["tipo_negocio"] = safe_get_text(driver, SEL.detail_tipo_negocio, "Venda")

    # --- AMENIDADES ---
    _processar_aba(driver, SEL.panel_imovel, MAP_IMOVEL, card_data)

    # Aba condomínio (precisa clicar)
    try:
        tab_condo = driver.find_element(By.ID, SEL.tab_condo)
        if "olx-tabs__tab--active" not in (tab_condo.get_attribute("class") or ""):
            driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", tab_condo
            )
            time.sleep(0.3)
            driver.execute_script("arguments[0].click();", tab_condo)
            time.sleep(0.8)
        _processar_aba(driver, SEL.panel_condo, MAP_CONDOMINIO, card_data)
    except Exception:
        for k in MAP_CONDOMINIO:
            card_data[k] = False

    # --- RATING ---
    rating_raw = safe_get_text(driver, SEL.detail_rating)
    m = re.search(r"(\d+(?:\.\d+)?)\/\d+\s*\((\d+)", rating_raw)
    card_data["nota_media"] = float(m.group(1)) if m else 0.0
    card_data["total_avaliacoes"] = int(m.group(2)) if m else 0

    # --- DATA DO ANÚNCIO (data-testid) ---
    date_raw = safe_get_text(driver, SEL.detail_date)
    if date_raw:
        created_part = date_raw.split(",")[0].replace("Anúncio criado em", "").strip()
        card_data["anuncio_criado"] = parse_pt_date_to_iso(created_part)
    else:
        card_data["anuncio_criado"] = pd.Timestamp.now().strftime("%Y-%m-%d")

    # --- CONDOMÍNIO / IPTU (detail page) ---
    # HTML: <p class="value-item__value" data-testid="condoFee">R$&nbsp;1.400/mês</p>
    # HTML: <p class="value-item__value" data-testid="iptu">R$&nbsp;290</p>
    condo_selectors = [
        'p[data-testid="condoFee"]',
        '[data-testid="condoFee"]',
        'p.value-item__value[data-testid="condoFee"]',
    ]
    for sel in condo_selectors:
        condo_txt = safe_get_text(driver, sel)
        if condo_txt:
            val_condo = extract_decimal_number(condo_txt)
            if val_condo > 0:
                card_data["condominio_R$"] = val_condo
                break

    iptu_selectors = [
        'p[data-testid="iptu"]',
        '[data-testid="iptu"]',
        'p.value-item__value[data-testid="iptu"]',
    ]
    for sel in iptu_selectors:
        iptu_txt = safe_get_text(driver, sel)
        if iptu_txt:
            val_iptu = extract_decimal_number(iptu_txt)
            if val_iptu > 0:
                card_data["iptu_R$"] = val_iptu
                break

    # --- PUBLICAÇÃO / ATUALIZAÇÃO (section com texto relativo) ---
    # A section com essa info tem min-md:hidden, então pode estar oculta.
    # Usa JavaScript para buscar o texto mesmo em elementos ocultos.
    pub_text = ""
    try:
        # Tenta via JavaScript: busca qualquer <p> que contenha "Publicado"
        pub_text = driver.execute_script("""
            var els = document.querySelectorAll('p.font-secondary, p.text-1-5');
            for (var i = 0; i < els.length; i++) {
                var t = els[i].textContent.trim();
                if (t.indexOf('Publicado') !== -1 || t.indexOf('ublicado') !== -1) {
                    return t;
                }
            }
            // Fallback: busca em qualquer texto da página
            var all = document.querySelectorAll('p, span');
            for (var i = 0; i < all.length; i++) {
                var t = all[i].textContent.trim();
                if (t.indexOf('Publicado h') !== -1) {
                    return t;
                }
            }
            return '';
        """) or ""
    except Exception:
        pub_text = safe_get_text(driver, SEL.detail_pub_info)

    # Limpa espaços extras e comentários HTML residuais
    pub_text = re.sub(r"\s+", " ", pub_text).strip()

    if pub_text and "ublicado" in pub_text:
        card_data["publicacao_texto"] = pub_text
        card_data["dias_publicado"] = _parse_dias_relativos(pub_text, "publicado")
        card_data["dias_atualizado"] = _parse_dias_relativos(pub_text, "atualizado")
    else:
        card_data["publicacao_texto"] = ""
        card_data["dias_publicado"] = 0
        card_data["dias_atualizado"] = 0

    # --- PARSE ENDEREÇO COMPLETO (fonte de verdade p/ bairro/cidade/UF/rua) ---
    # Formato: "Rua Bolivar - Copacabana, Rio de Janeiro - RJ"
    # ou:      "Rua Hilário de Gouveia, 116 - Copacabana, Rio de Janeiro - RJ"
    endereco = card_data.get("endereco_completo", "")
    if endereco and " - " in endereco:
        partes_uf = endereco.rsplit(" - ", 1)
        if len(partes_uf) == 2:
            card_data["uf"] = partes_uf[1].strip()
            restante = partes_uf[0]

            partes_bairro = restante.rsplit(" - ", 1)
            if len(partes_bairro) == 2:
                rua_raw = partes_bairro[0].strip()
                bairro_cidade = partes_bairro[1].strip()

                if ", " in bairro_cidade:
                    bairro_parsed, cidade_parsed = bairro_cidade.rsplit(", ", 1)
                    card_data["bairro"] = bairro_parsed.strip()
                    card_data["cidade"] = cidade_parsed.strip()
                else:
                    card_data["bairro"] = bairro_cidade.strip()
                    card_data["cidade"] = "Rio de Janeiro"

                rua_limpa = rua_raw.split(",")[0].strip()
                if rua_limpa and len(rua_limpa) > 3:
                    card_data["rua"] = rua_limpa
    else:
        card_data["uf"] = "RJ"

    # --- COORDENADAS (iframe → Nominatim endereço → Nominatim bairro) ---
    lat_val, lon_val = 0.0, 0.0
    origem_geo = "Nao Encontrado"

    # Tentativa 1: iframe do mapa
    try:
        iframe = WebDriverWait(driver, 4).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, SEL.detail_map_iframe))
        )
        src_url = iframe.get_attribute("src") or ""
        src_decoded = src_url.replace("%2C", ",").replace("%2c", ",")
        match = re.search(r"[?&]q=(-?\d{1,3}\.\d+),(-?\d{1,3}\.\d+)", src_decoded)
        if match:
            lat_val = float(match.group(1))
            lon_val = float(match.group(2))
            origem_geo = "Site ZAP"
    except Exception:
        pass

    # Tentativa 2: Nominatim com endereço completo
    if lat_val == 0.0 and endereco:
        lat_val, lon_val = geocode_fallback_nominatim(endereco)
        if lat_val != 0.0:
            origem_geo = "Nominatim (Endereco)"

    # Tentativa 3: Nominatim rua + bairro (já parseados)
    if lat_val == 0.0:
        rua = card_data.get("rua", "")
        bairro = card_data.get("bairro", "")
        if rua and len(rua) > 3 and bairro and len(bairro) > 2:
            query = f"{rua}, {bairro}, Rio de Janeiro, RJ, Brasil"
            lat_val, lon_val = geocode_fallback_nominatim(query)
            if lat_val != 0.0:
                origem_geo = "Nominatim (Rua)"

    # Tentativa 4: Nominatim só bairro
    if lat_val == 0.0:
        bairro = card_data.get("bairro", "")
        if bairro and len(bairro) > 2:
            query = f"{bairro}, Rio de Janeiro, RJ, Brasil"
            lat_val, lon_val = geocode_fallback_nominatim(query)
            if lat_val != 0.0:
                origem_geo = "Nominatim (Bairro)"

    card_data["latitude"] = lat_val
    card_data["longitude"] = lon_val
    card_data["origem_geo"] = origem_geo
    card_data["coordenadas"] = (
        f"{lat_val},{lon_val}" if lat_val != 0.0 else "Url encontrada sem coordenadas"
    )


# ==============================================================================
#  PRODUTOR (LISTAGEM)
# ==============================================================================

def produtor_listagem(tarefa: Dict, headless: bool = False):
    """Varre páginas de listagem e enfileira links para os consumidores."""
    label = f"{tarefa['zona']['nome']} [{tarefa['worker_id']}]"
    slug = tarefa["zona"]["slug"]
    paginas = tarefa["paginas"]

    logger.info(f"📡 PRODUTOR INICIADO: {label}")
    driver = None
    total_enqueued = 0

    try:
        time.sleep(random.uniform(1, 4))
        driver = start_driver(headless=headless)
        base_url = f"https://www.zapimoveis.com.br/venda/imoveis/{slug}/"

        for pagina in paginas:
            try:
                driver.get(f"{base_url}?pagina={pagina}")

                if "Checking your browser" in driver.title:
                    logger.warning(f"[{label}] Cloudflare check. Aguardando...")
                    time.sleep(8)

                # --- Detecção de fim de anúncios ---
                # O ZAP mostra "Não conseguimos encontrar a página solicitada."
                # quando não há mais resultados
                try:
                    page_text = driver.find_element(By.TAG_NAME, "body").text
                    if "Não conseguimos encontrar a página solicitada" in page_text:
                        logger.info(
                            f"🛑 [{label}] Fim de anúncios na pág {pagina}. "
                            f"Zona esgotada."
                        )
                        break
                except Exception:
                    pass

                scroll_inteligente(driver)

                card_elements = driver.find_elements(By.CSS_SELECTOR, SEL.card_item)

                if len(card_elements) < 5:
                    scroll_inteligente(driver)
                    card_elements = driver.find_elements(By.CSS_SELECTOR, SEL.card_item)

                # Se não encontrou nenhum card, pode ser fim também
                if len(card_elements) == 0:
                    logger.info(
                        f"🛑 [{label}] 0 cards na pág {pagina}. "
                        f"Zona provavelmente esgotada."
                    )
                    break

                count_pag = 0
                for i, el in enumerate(card_elements):
                    resumo = parse_card_resumo(el)
                    if resumo:
                        # Log diagnóstico do primeiro card
                        if i == 0 and count_pag == 0:
                            logger.info(
                                f"[{label}] 🔍 Amostra card #1: "
                                f"valor={resumo.get('valor_R$')}, "
                                f"area={resumo.get('area_m2')}, "
                                f"bairro={resumo.get('bairro', '?')}"
                            )

                        # Dedup thread-safe
                        with _SEEN_LOCK:
                            if resumo["url"] in _SEEN_URLS:
                                _inc_stat("skipped_dup")
                                continue
                            _SEEN_URLS.add(resumo["url"])

                        LINK_QUEUE.put(resumo)
                        _inc_stat("enqueued")
                        count_pag += 1

                total_enqueued += count_pag
                logger.info(f"➡️ [{label}] Pág {pagina}: {count_pag} links (total: {total_enqueued})")

            except Exception as e:
                logger.error(f"[{label}] Erro pág {pagina}: {e}")

    except Exception as e:
        logger.error(f"[{label}] Erro fatal: {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        logger.info(f"🏁 PRODUTOR FINALIZADO: {label} — {total_enqueued} links enfileirados")


# ==============================================================================
#  CONSUMIDOR (DETALHES + SAVE-AS-YOU-GO)
# ==============================================================================

def consumidor_detalhes(worker_id: int, headless: bool = False):
    """
    Worker consumidor: puxa itens da fila, enriquece e salva no CSV.
    Encerra quando PRODUCERS_DONE é setado e a fila está vazia.
    """
    logger.info(f"🔨 CONSUMIDOR #{worker_id} PRONTO.")
    driver = None
    processed_count = 0
    filename_temp = f"temp_worker_{worker_id}.csv"

    file_exists = os.path.isfile(filename_temp)
    csv_file = open(filename_temp, "a", newline="", encoding="utf-8-sig", buffering=1)
    writer = csv.DictWriter(
        csv_file, fieldnames=CSV_FIELDNAMES, delimiter=";", extrasaction="ignore"
    )

    if not file_exists:
        writer.writeheader()
        csv_file.flush()

    try:
        time.sleep(random.uniform(1, 8))
        driver = start_driver(headless=headless)

        while True:
            try:
                item = LINK_QUEUE.get(timeout=10)
            except queue.Empty:
                # Verifica se produtores já terminaram E fila está vazia
                if PRODUCERS_DONE.is_set() and LINK_QUEUE.empty():
                    logger.info(f"[Consumidor #{worker_id}] Fila vazia + produtores finalizados. Encerrando.")
                    break
                continue

            if item is None:
                break

            try:
                enriquecer_detalhes(driver, item)
                writer.writerow(item)
                csv_file.flush()
                _inc_stat("processed")
            except Exception as e:
                logger.error(f"[Consumidor #{worker_id}] Erro em {item.get('url', '?')}: {e}")
                _inc_stat("errors")
                # Salva dados parciais mesmo com erro
                item["origem_geo"] = f"ERRO: {str(e)[:80]}"
                writer.writerow(item)
                csv_file.flush()

            LINK_QUEUE.task_done()
            processed_count += 1

            # Restart periódico do driver para liberar memória
            if processed_count % DRIVER_RESTART_INTERVAL == 0:
                driver = restart_driver_safe(driver, headless=headless)
                logger.info(
                    f"[Consumidor #{worker_id}] Driver reiniciado após {processed_count} itens."
                )

    except Exception as e:
        logger.error(f"Consumidor #{worker_id} falhou: {e}")
    finally:
        csv_file.close()
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        logger.info(
            f"💤 Consumidor #{worker_id} encerrou. {processed_count} salvos em {filename_temp}."
        )