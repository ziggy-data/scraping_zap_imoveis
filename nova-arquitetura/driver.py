"""
Gerenciamento do Chrome driver (undetected_chromedriver).
"""
import os as _os, sys as _sys
_DIR = _os.path.dirname(_os.path.abspath(__file__))
if _DIR not in _sys.path:
    _sys.path.insert(0, _DIR)

import random
import time
import threading

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from config import USER_AGENTS, logger

# Lock global para criação de drivers (uc.Chrome não é thread-safe no init)
_DRIVER_LOCK = threading.Lock()

# Patch para evitar erro no __del__ do Chrome no Windows
def _safe_del_patch(self):
    try:
        self.quit()
    except Exception:
        pass

uc.Chrome.__del__ = _safe_del_patch


def start_driver(headless: bool = False) -> uc.Chrome:
    """
    Cria uma instância do Chrome com anti-detecção e otimizações de performance.
    
    Args:
        headless: Se True, roda sem janela (menos RAM, mais rápido).
    """
    with _DRIVER_LOCK:
        options = uc.ChromeOptions()
        options.add_argument("--disable-blink-features=AutomationControlled")

        if headless:
            options.add_argument("--headless=new")

        # Performance: bloquear imagens e notificações
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.notifications": 2,
        }
        options.add_experimental_option("prefs", prefs)

        # Estabilidade em containers/Linux
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")

        # Reduzir footprint de memória
        options.add_argument("--disable-background-timer-throttling")
        options.add_argument("--disable-renderer-backgrounding")
        options.add_argument("--disable-backgrounding-occluded-windows")

        # Rotação de User-Agent
        ua = random.choice(USER_AGENTS)
        options.add_argument(f"--user-agent={ua}")

        driver = uc.Chrome(options=options)
        driver.set_window_size(1100, 800)

    return driver


def scroll_inteligente(driver, num_scrolls: int = 5, fast: bool = True):
    """
    Simula scroll humano na página para disparar lazy loading do React.
    
    Args:
        driver: Instância do Chrome.
        num_scrolls: Número de PAGE_DOWN.
        fast: Se True, usa delays menores (modo imagem bloqueada).
    """
    try:
        body = driver.find_element(By.TAG_NAME, "body")
        delay_range = (0.2, 0.5) if fast else (0.4, 0.8)

        for _ in range(num_scrolls):
            body.send_keys(Keys.PAGE_DOWN)
            time.sleep(random.uniform(*delay_range))

        body.send_keys(Keys.PAGE_UP)
        time.sleep(0.3)
        body.send_keys(Keys.END)
        time.sleep(1.5)
    except Exception:
        pass


def restart_driver_safe(driver, headless: bool = False) -> uc.Chrome:
    """Encerra o driver atual e cria um novo (libera memória)."""
    if driver:
        try:
            driver.quit()
        except Exception:
            pass
    time.sleep(1.5)
    return start_driver(headless=headless)