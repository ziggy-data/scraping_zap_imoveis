"""
Funções utilitárias: retry, extração de texto/números, haversine, geocoding.
"""
import os as _os, sys as _sys
_DIR = _os.path.dirname(_os.path.abspath(__file__))
if _DIR not in _sys.path:
    _sys.path.insert(0, _DIR)

import re
import time
import math
import functools
import threading
import urllib.parse
from typing import Tuple

import numpy as np
import requests

from config import logger, MONTHS_PT

# ==============================================================================
#  RETRY DECORATOR
# ==============================================================================

def retry(max_attempts: int = 3, delay: float = 2.0, backoff: float = 1.5,
          exceptions: tuple = (Exception,)):
    """
    Decorator de retry com backoff exponencial.
    
    Args:
        max_attempts: Número máximo de tentativas.
        delay: Delay base entre tentativas (segundos).
        backoff: Multiplicador do delay a cada tentativa.
        exceptions: Tupla de exceções que devem ser retentadas.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_attempts:
                        logger.error(
                            f"[retry] {func.__name__} falhou após {max_attempts} tentativas: {e}"
                        )
                        raise
                    logger.warning(
                        f"[retry] {func.__name__} tentativa {attempt}/{max_attempts} falhou: {e}. "
                        f"Retentando em {current_delay:.1f}s..."
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff
            return None
        return wrapper
    return decorator


# ==============================================================================
#  EXTRAÇÃO DE TEXTO / NÚMEROS
# ==============================================================================

def extract_number(text: str, default: int = 0) -> int:
    """Extrai primeiro número inteiro de uma string."""
    if not text:
        return default
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else default


def extract_decimal_number(text: str, default: float = 0.0) -> float:
    """
    Extrai valor monetário de uma string.
    Aceita formatos: "R$ 1.200,50", "R$ 1200", "1.200,50", "1200", etc.
    """
    if not text:
        return default

    # Tenta com prefixo R$ primeiro
    match = re.search(r"R\$\s*([\d\.,]+)", text)
    if match:
        num_str = match.group(1).replace(".", "").replace(",", ".")
        try:
            return float(num_str)
        except ValueError:
            pass

    # Fallback: qualquer número no texto (formato BR: 1.200,50 ou 1200)
    match = re.search(r"([\d]+(?:\.[\d]{3})*(?:,[\d]{1,2})?)", text)
    if match:
        num_str = match.group(1).replace(".", "").replace(",", ".")
        try:
            val = float(num_str)
            if val > 0:
                return val
        except ValueError:
            pass

    return default


def safe_get_text(driver, selector: str, default: str = "") -> str:
    """Extrai texto de um elemento via CSS selector, sem lançar exceção."""
    try:
        from selenium.webdriver.common.by import By
        el = driver.find_element(By.CSS_SELECTOR, selector)
        raw = el.get_attribute("textContent") or ""
        return re.sub(r"\s+", " ", raw).strip()
    except Exception:
        return default


def safe_get_attribute(element, selector: str, attribute: str, default: str = "") -> str:
    """Extrai atributo de um sub-elemento via CSS selector, sem lançar exceção."""
    try:
        from selenium.webdriver.common.by import By
        return element.find_element(By.CSS_SELECTOR, selector).get_attribute(attribute) or default
    except Exception:
        return default


def parse_pt_date_to_iso(pt_date: str) -> str:
    """Converte '12 de março de 2025' → '2025-03-12'."""
    m = re.search(r"(\d{1,2})\s+de\s+([A-Za-zç]+)\s+de\s+(\d{4})", pt_date, re.IGNORECASE)
    if not m:
        return pt_date
    day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
    month = MONTHS_PT.get(month_name)
    if not month:
        return pt_date
    return f"{int(year):04d}-{month:02d}-{int(day):02d}"


# ==============================================================================
#  HAVERSINE (escalar + vetorizado)
# ==============================================================================

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distância em km entre dois pontos (escalar)."""
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def haversine_vectorized(lat1: np.ndarray, lon1: np.ndarray,
                         lat2: float, lon2: float) -> np.ndarray:
    """
    Distância em km — vetorizado com NumPy.
    lat1/lon1 são arrays (todos os imóveis), lat2/lon2 é um ponto fixo (POI).
    """
    R = 6371
    lat1_r, lon1_r = np.radians(lat1), np.radians(lon1)
    lat2_r, lon2_r = np.radians(lat2), np.radians(lon2)
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1_r) * np.cos(lat2_r) * np.sin(dlon / 2) ** 2
    return R * 2 * np.arcsin(np.sqrt(a))


# ==============================================================================
#  GEOCODING FALLBACK (Nominatim) — com rate limiting global
# ==============================================================================

_nominatim_lock = threading.Semaphore(1)   # Máximo 1 req/s (exigência da API)
_nominatim_last_call = 0.0
_nominatim_lock_time = threading.Lock()


def geocode_fallback_nominatim(endereco: str) -> Tuple[float, float]:
    """
    Geocoding via OpenStreetMap Nominatim com rate-limiting thread-safe.
    Retorna (lat, lon) ou (0.0, 0.0) se falhar.
    """
    global _nominatim_last_call

    if not endereco or len(endereco) < 10:
        return 0.0, 0.0

    # Garante no mínimo 1.1s entre chamadas (exigência do Nominatim)
    with _nominatim_lock:
        with _nominatim_lock_time:
            elapsed = time.time() - _nominatim_last_call
            if elapsed < 1.1:
                time.sleep(1.1 - elapsed)
            _nominatim_last_call = time.time()

        try:
            clean_address = endereco.split(" - CEP")[0]
            url = (
                f"https://nominatim.openstreetmap.org/search?"
                f"q={urllib.parse.quote(clean_address)}&format=json&limit=1"
            )
            headers = {
                "User-Agent": "ScraperImoveisRio_StudentProject_v2.0",
                "Referer": "https://google.com",
            }
            response = requests.get(url, headers=headers, timeout=5)

            if response.status_code == 200:
                data = response.json()
                if data:
                    lat = float(data[0]["lat"])
                    lon = float(data[0]["lon"])
                    return lat, lon
        except Exception:
            pass

    return 0.0, 0.0