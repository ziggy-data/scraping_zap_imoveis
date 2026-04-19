import pandas as pd
import re
import time
import logging
import random
import threading
import glob
import os
import queue
import numpy as np
import math
import csv
import requests
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional
import undetected_chromedriver as uc 
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- CONFIGURAÇÕES DE PERFORMANCE ---
MAX_LISTING_WORKERS = 4   # Produtores (Listagem)
MAX_DETAILS_WORKERS = 18  # Consumidores (Detalhes)

DRIVER_LOCK = threading.Lock() 
LINK_QUEUE = queue.Queue()

# Defina aqui quantas páginas por zona você quer varrer
NUM_PAGINAS = 300

# --- LISTA DE USER-AGENTS PARA ROTAÇÃO (CAMUFLAGEM) ---
USER_AGENTS_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
]

# --- CORREÇÃO ERRO WINDOWS ---
def _safe_del_patch(self):
    try: self.quit()
    except: pass
uc.Chrome.__del__ = _safe_del_patch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s %(message)s",
    datefmt='%H:%M:%S',
    handlers=[
        logging.FileHandler("scraper_execution.log", encoding='utf-8'), # Salva em arquivo
        logging.StreamHandler() # Mostra no terminal
    ]
)
logger = logging.getLogger(__name__)

# --- ZONAS ---
ZONAS_CONFIG = [
    {"nome": "Zona Norte", "slug": "rj+rio-de-janeiro+zona-norte", "split": True}, 
    {"nome": "Zona Sul",   "slug": "rj+rio-de-janeiro+zona-sul",   "split": True},
    {"nome": "Zona Oeste", "slug": "rj+rio-de-janeiro+zona-oeste", "split": True},
]

MONTHS_PT = {
    'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4, 'maio': 5, 'junho': 6,
    'julho': 7, 'agosto': 8, 'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12,
}

# --- MAPEAMENTO ESTRATÉGICO DO RIO DE JANEIRO ---
POIS_RIO = {
    "praias": {
        "Leme": (-22.9635, -43.1717),
        "Copacabana (Posto 5)": (-22.9778, -43.1903),
        "Ipanema (Posto 9)": (-22.9866, -43.2046),
        "Leblon (Posto 12)": (-22.9873, -43.2215),
        "São Conrado": (-22.9975, -43.2647),
        "Barra (Jardim Oceânico)": (-23.0125, -43.3087),
        "Barra (Posto 4)": (-23.0116, -43.3245),
        "Reserva/Recreio": (-23.0182, -43.4332),
        "Flamengo (Aterro)": (-22.9324, -43.1730),
        "Botafogo (Enseada)": (-22.9463, -43.1819)
    },
    "lazer_verde": {
        "Lagoa Rodrigo de Freitas": (-22.9734, -43.2114),
        "Aterro do Flamengo": (-22.9221, -43.1733),
        "Jardim Botânico": (-22.9676, -43.2233),
        "Parque Lage": (-22.9602, -43.2119),
        "Quinta da Boa Vista": (-22.9056, -43.2244),
        "Parque Madureira": (-22.8732, -43.3402),
        "Parque Piedade (Antiga Gama Filho)": (-22.8929, -43.3080),
        "Parque Rita Lee (Barra Olímpica)": (-22.9770, -43.3940),
        "Parque Realengo (Susana Naspolini)": (-22.8760, -43.4300)
    },
    "shoppings_premium": {
        "Shopping Leblon": (-22.9824, -43.2173),
        "RioSul": (-22.9567, -43.1769),
        "BarraShopping": (-22.9995, -43.3556),
        "Village Mall": (-23.0003, -43.3533),
        "Shopping Tijuca": (-22.9234, -43.2360),
        "NorteShopping": (-22.8879, -43.2831),
        "Nova América": (-22.8794, -43.2721)
    },
    "metro_hub": {
        # Zona Sul / Centro / Tijuca
        "Metro Cardeal Arcoverde": (-22.9644, -43.1812),
        "Metro General Osório": (-22.9846, -43.1977),
        "Metro Nossa Sra Paz": (-22.9839, -43.2065),
        "Metro Jardim de Alah": (-22.9829, -43.2154),
        "Metro Botafogo": (-22.9510, -43.1840),
        "Metro Flamengo": (-22.9324, -43.1790),
        "Metro Largo do Machado": (-22.9298, -43.1780),
        "Metro Carioca": (-22.9079, -43.1772),
        "Metro Saens Peña": (-22.9244, -43.2325),
        "Metro Uruguai": (-22.9317, -43.2405),
        "Metro Jd Oceânico": (-23.0076, -43.3106),
        "Metro São Conrado": (-22.9912, -43.2543),
        "Metro Pavuna": (-22.8126, -43.3600)
    },
    "trem_supervia": {
        # Trens Urbanos (Crucial para ZN e ZO)
        "Estação Central do Brasil": (-22.9042, -43.1887),
        "Estação São Cristóvão": (-22.9097, -43.2223),
        "Estação Méier": (-22.9018, -43.2781),
        "Estação Madureira": (-22.8769, -43.3374),
        "Estação Deodoro": (-22.8549, -43.3837),
        "Estação Bangu": (-22.8754, -43.4658),
        "Estação Campo Grande": (-22.9022, -43.5604)
    },
    
    "brt_stations": { # NOVO: Conectividade Zona Oeste/Norte
        "Term. Alvorada": (-22.9992, -43.3663), "Term. Jardim Oceânico": (-23.0076, -43.3106),
        "Estação Vicente de Carvalho": (-22.8546, -43.3134), "Term. Recreio": (-23.0139, -43.4533),
        "Estação Taquara": (-22.9213, -43.3725), "Term. Paulo da Portela": (-22.8765, -43.3385),
        "Estação Galeão (Aeroporto)": (-22.8079, -43.2530)
    },
    "saude_educacao": {
        # Escolas de Referência (Ouro para famílias)
        "Colégio Pedro II (Humaitá)": (-22.9569, -43.1936),
        "Colégio Pedro II (Tijuca)": (-22.9189, -43.2183),
        "Colégio Pedro II (Centro)": (-22.9067, -43.2044),
        "Colégio Pedro II (São Cristóvão)": (-22.8997, -43.2217),
        "Colégio Pedro II (Realengo)": (-22.8789, -43.4300),
        "CAp UFRJ (Lagoa)": (-22.9714, -43.2033),
        "Colégio Militar (Tijuca)": (-22.9158, -43.2269),
        "Santo Inácio": (-22.9535, -43.1912), "São Bento": (-22.8983, -43.1772),
        
        # Hospitais de Ponta
        "Hosp. Copa D'Or": (-22.9695, -43.1878), "Hosp. Barra D'Or": (-22.9942, -43.3637),
        "Hosp. Souza Aguiar (Emergência)": (-22.9077, -43.1908),
        "Hosp. Miguel Couto (Gávea)": (-22.9803, -43.2250),
        "Hosp. Lourenço Jorge (Barra)": (-22.9992, -43.3663),
        "INCA (Centro)": (-22.9103, -43.1856)
    },
    
    "areas_sensiveis": {
        # Zona Sul
        "Rocinha (Baixo/Autoestrada)": (-22.9934, -43.2547),
        "Vidigal (Entrada)": (-22.9943, -43.2348),
        "Cantagalo/Pavão (Copacabana)": (-22.9763, -43.1952),
        "Santa Marta (Botafogo)": (-22.9482, -43.1903),
        "Tabajaras (Copacabana/Botafogo)": (-22.9619, -43.1936),
        "Babilônia/Chapéu (Leme)": (-22.9608, -43.1678),
        
        # Tijuca / Grande Méier (Zona Norte)
        "Complexo do Lins": (-22.9189, -43.2798),
        "Morro dos Macacos (Vila Isabel)": (-22.9187, -43.2530),
        "Salgueiro/Borel (Tijuca)": (-22.9376, -43.2435),
        "Turano (Rio Comprido)": (-22.9268, -43.2133),
        "Mangueira": (-22.9038, -43.2393),
        "Jacarezinho": (-22.8877, -43.2533),
        
        # Eixo Suburbana / Leopoldina (Zona Norte Profunda)
        "Complexo do Alemão": (-22.8587, -43.2725),
        "Complexo da Maré": (-22.8617, -43.2422),
        "Complexo da Penha": (-22.8468, -43.2829),
        "Serrinha (Madureira)": (-22.8681, -43.3323),
        "Juramento": (-22.8633, -43.3130),
        "Chapadão": (-22.8336, -43.3592),
        "Pedreira": (-22.8258, -43.3670),
        
        # Zona Oeste
        "Cidade de Deus (CDD)": (-22.9489, -43.3622),
        "Rio das Pedras": (-22.9737, -43.3283),
        "Vila Kennedy": (-22.8608, -43.4862),
        "Gardênia Azul": (-22.9599, -43.3486)
    },

    "seguranca_publica": {
        # Batalhões da PM (Presença Ostensiva)
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
        
        # Delegacias Chave (Polícia Civil) e Centros de Comando
        "Cidade da Polícia (Jacaré)": (-22.8923, -43.2519),
        "12ª DP (Copacabana)": (-22.9678, -43.1843),
        "14ª DP (Leblon)": (-22.9839, -43.2227),
        "19ª DP (Tijuca)": (-22.9242, -43.2320),
        "16ª DP (Barra)": (-23.0089, -43.3130),
        "Batalhão de Choque (Centro)": (-22.9103, -43.1928),
        "QG da PM (Centro)": (-22.9077, -43.1866)
    }, 
        "mercados_essenciais": {
        # --- ZONA SUL (Alta densidade, foco em Pão de Açúcar, Zona Sul e Mundial) ---
        "Mundial (Botafogo)": (-22.9497, -43.1865),
        "Mundial (Copacabana/Siqueira)": (-22.9680, -43.1870),
        "Zona Sul (Ipanema/Gen. Osório)": (-22.9842, -43.1983),
        "Zona Sul (Leblon/Bartolomeu)": (-22.9860, -43.2250),
        "Pão de Açúcar (Copacabana)": (-22.9682, -43.1855),
        "Pão de Açúcar (Flamengo)": (-22.9360, -43.1765),
        "Mundial (Largo do Machado)": (-22.9305, -43.1785),
        "Princesa (Leme)": (-22.9630, -43.1680),
        "Zona Sul (São Conrado)": (-22.9970, -43.2630),
        "Pão de Açúcar (Botafogo/Voluntários)": (-22.9530, -43.1900),
        
        # --- GRANDE TIJUCA (Hubs pesados) ---
        "Guanabara (Tijuca/Maxwell)": (-22.9238, -43.2458),
        "Mundial (Tijuca/Santo Afonso)": (-22.9254, -43.2343),
        "Extra/Assaí (Mariz e Barros)": (-22.9150, -43.2180),
        "Guanabara (Vila Isabel)": (-22.9152, -43.2483),
        "Campeão (Praça da Bandeira)": (-22.9120, -43.2150),
        "Prezunic (Tijuca/Fonseca Telles)": (-22.9050, -43.2250),
        "Mundial (Grajaú)": (-22.9200, -43.2600),
        
        # --- ZONA NORTE: MÉIER / SUBURBANA / CAMPINHO ---
        "Guanabara (Campinho)": (-22.8833, -43.3467), 
        "Prezunic (Campinho/Jaurú)": (-22.8850, -43.3500),
        "Prezunic (Méier)": (-22.8988, -43.2758),
        "Assaí (Méier/Dias da Cruz)": (-22.9050, -43.2800),
        "Guanabara (Engenho de Dentro)": (-22.8950, -43.2950),
        "Guanabara (Piedade)": (-22.8902, -43.3031),
        "Inter (Cascadura)": (-22.8800, -43.3250),
        "Mundial (Cachambi)": (-22.8880, -43.2750),
        "Prezunic (Benfica)": (-22.8900, -43.2400),
        
        # --- ZONA NORTE: LEOPOLDINA / ILHA ---
        "Mundial (Ramos)": (-22.8553, -43.2621),
        "Guanabara (Penha)": (-22.8400, -43.2800),
        "Guanabara (Bonsucesso)": (-22.8600, -43.2500),
        "Mundial (Ilha/Cacuia)": (-22.8050, -43.1950),
        "Assaí (Ilha/Galeão)": (-22.8100, -43.2300),
        "Mundial (Vaz Lobo)": (-22.8550, -43.3200),
        "Guanabara (Irajá)": (-22.8350, -43.3300),
        "Prezunic (Vista Alegre)": (-22.8300, -43.3100),
        
        # --- JACAREPAGUÁ (Freguesia / Taquara / Tanque) ---
        "Mundial (Freguesia)": (-22.9350, -43.3400),
        "Prezunic (Freguesia)": (-22.9450, -43.3450),
        "Assaí (Tanque)": (-22.9248, -43.3592),
        "Guanabara (Tanque)": (-22.9200, -43.3600),
        "Prezunic (Taquara)": (-22.9213, -43.3725),
        "Guanabara (Taquara)": (-22.9150, -43.3800),
        "Prezunic (Pechincha)": (-22.9300, -43.3550),
        
        # --- BARRA DA TIJUCA / RECREIO ---
        "Guanabara (Barra)": (-23.0062, -43.3389),
        "Mundial (Jd Oceânico)": (-23.0128, -43.3052),
        "Zona Sul (Barra/Alfa)": (-23.0000, -43.3500),
        "Carrefour (Barra)": (-22.9980, -43.3600),
        "Prezunic (Recreio)": (-23.0075, -43.4431),
        "Mundial (Recreio)": (-23.0180, -43.4600),
        "Zona Sul (Recreio)": (-23.0150, -43.4500),
        
        # --- ZONA OESTE PROFUNDA (Campo Grande / Bangu / Realengo) ---
        "Guanabara (Realengo)": (-22.8750, -43.4300),
        "Inter (Bangu)": (-22.8754, -43.4658),
        "Guanabara (Bangu)": (-22.8800, -43.4600),
        "Guanabara (Campo Grande)": (-22.9022, -43.5604),
        "Prezunic (Campo Grande)": (-22.9100, -43.5500),
        "Assaí (Santa Cruz)": (-22.9200, -43.6800)
    },
        "feiras_alimentacao": {
        # --- ZONA SUL & CENTRO (Orgânicas e Tradicionais) ---
        "Feira Ipanema (Pça N. Sra Paz - Orgânica)": (-22.9839, -43.2065),
        "Feira Leblon (Pça Antero de Quental - Orgânica)": (-22.9849, -43.2232),
        "Feira Botafogo (Pça Nelson Mandela)": (-22.9510, -43.1840),
        "Feira Flamengo (Pça José de Alencar)": (-22.9298, -43.1780),
        "Feira Copacabana (Domingo/Serzedelo)": (-22.9680, -43.1870),
        "Feira Humaitá (Quarta)": (-22.9560, -43.1930),
        "Feira Laranjeiras (General Glicério)": (-22.9344, -43.1878),
        "Feira Glória (Domingo/Augusto Severo)": (-22.9180, -43.1760),
        "Feira Catete (Sábado/Bento Lisboa)": (-22.9280, -43.1780),
        
        # --- GRANDE TIJUCA (Alta Densidade) ---
        "Feira Tijuca (Sexta/S. Fco Xavier)": (-22.9208, -43.2241),
        "Feira Tijuca (Domingo/Haddock Lobo)": (-22.9150, -43.2180),
        "Feira Orgânica Tijuca (Pça Xavier de Brito)": (-22.9246, -43.2433),
        "Feira Grajaú (Domingo/Verdun)": (-22.9260, -43.2620),
        "Feira Vila Isabel (Terça/Boulevard)": (-22.9170, -43.2450),
        "Feira Andaraí (Sábado/Barão de Mesquita)": (-22.9320, -43.2500),
        
        # --- ZONA NORTE: GRANDE MÉIER / SUBURBANA ---
        "Feira Méier (Domingo/Dias da Cruz)": (-22.9020, -43.2800),
        "Feira Engenho de Dentro (Quinta/Adolfo Bergamini)": (-22.8960, -43.2960),
        "Feira Cachambi (Domingo/Honório)": (-22.8880, -43.2750),
        "Feira Piedade (Sábado/Guanabara)": (-22.8900, -43.3030),
        "Feira Pilares (Domingo/João Ribeiro)": (-22.8820, -43.2950),
        "Feira Engenho Novo (Terça/Barão)": (-22.9080, -43.2650),
        "Feira Abolição (Domingo/Largo)": (-22.8850, -43.2990),
        "Feira Encantado (Quarta/Clarimundo)": (-22.8940, -43.3000),
        
        # --- ZONA NORTE: LEOPOLDINA / ILHA ---
        "Feira Olaria (Quarta/Cinco Bocas)": (-22.8460, -43.2630),
        "Feira Ramos (Sábado/Euclides Faria)": (-22.8520, -43.2600),
        "Feira Bonsucesso (Terça/Teixeira de Castro)": (-22.8600, -43.2500),
        "Feira Penha (Domingo/IAPI)": (-22.8400, -43.2800),
        "Feira Vila da Penha (Domingo/Oliveira Belo)": (-22.8450, -43.3150),
        "Feira Vista Alegre (Sábado/Estr. Água Grande)": (-22.8300, -43.3100),
        "Feira Ilha (Domingo/Cacuia)": (-22.8050, -43.1950),
        "Feira Ilha (Sábado/Jardim Guanabara)": (-22.8100, -43.2050),
        
        # --- ZONA NORTE: MADUREIRA / IRAJÁ / PAVUNA ---
        "Feira Madureira (Sábado/Viaduto)": (-22.8730, -43.3400),
        "Feira Irajá (Quarta/Metrô)": (-22.8350, -43.3300),
        "Feira Vicente de Carvalho (Sexta)": (-22.8540, -43.3130),
        "Feira Pavuna (Domingo/Metrô)": (-22.8120, -43.3600),
        "Feira Rocha Miranda (Sábado/Praça)": (-22.8560, -43.3420),
        
        # --- JACAREPAGUÁ / BARRA / RECREIO ---
        "Feira Freguesia (Sábado/Pça Camisão)": (-22.9370, -43.3400),
        "Feira Pechincha (Quinta/Geremário)": (-22.9300, -43.3550),
        "Feira Taquara (Domingo/Janio Quadros)": (-22.9210, -43.3720),
        "Feira Vila Valqueire (Sábado/Pça Saiqui)": (-22.8880, -43.3650), # Clássica da região
        "Feira Praça Seca (Domingo)": (-22.8883, -43.3550),
        "Feira Orgânica Barra (Pça do Ó)": (-23.0113, -43.3193),
        "Feira Recreio (Domingo/Glaucio Gil)": (-23.0180, -43.4600),
        
        # --- ZONA OESTE (Realengo / Bangu / Campo Grande) ---
        "Feira Bangu (Domingo/Cônego de Vasconcelos)": (-22.8750, -43.4650),
        "Feira Realengo (Sábado/Piraquara)": (-22.8760, -43.4300),
        "Feira Campo Grande (Domingo/Rodoviária)": (-22.9020, -43.5600),
        "Feira Santa Cruz (Domingo/Areia Branca)": (-22.9200, -43.6800)
    },
    
    "cultura_esporte": {
        # Vilas Olímpicas e Parques Esportivos (Gratuitos)
        "Vila Olímpica do Encantado": (-22.8950, -43.2950),
        "Parque Radical de Deodoro": (-22.8549, -43.3837),
        "Vila Olímpica da Maré": (-22.8617, -43.2422),
        "Aterro do Flamengo (Esportes)": (-22.9221, -43.1733),
        "Ciclovia da Lagoa": (-22.9734, -43.2114),
        
        # Cultura
        "Cidade das Artes (Barra)": (-22.9992, -43.3663),
        "Teatro Municipal / Cinelândia": (-22.9090, -43.1770),
        "Imperator (Méier)": (-22.8988, -43.2758),
        "Sesc Tijuca": (-22.9230, -43.2350),
        "Espaço Cultural Madureira": (-22.8732, -43.3402),
        "CCBB (Centro)": (-22.9015, -43.1765)
    }
}

def geocode_fallback_nominatim(endereco: str) -> tuple:
    """
    Tenta descobrir Lat/Lon usando OpenStreetMap quando o ZAP falha.
    Retorna (lat, lon) ou (0.0, 0.0) se falhar.
    """
    if not endereco or len(endereco) < 10:
        return 0.0, 0.0

    try:
        # Limpeza básica do endereço para melhorar a busca
        # Remove "Apartamento..." e deixa só Rua/Número/Bairro/Cidade
        # Ex: "Rua Barão da Torre, 32 - Ipanema, Rio de Janeiro - RJ" é o ideal
        clean_address = endereco.split(" - CEP")[0] # Remove CEP se tiver
        
        url = f"https://nominatim.openstreetmap.org/search?q={urllib.parse.quote(clean_address)}&format=json&limit=1"
        
        # Nominatim EXIGE um User-Agent único para não bloquear
        headers = {
            'User-Agent': 'ScraperImoveisRio_StudentProject_v1.0',
            'Referer': 'https://google.com'
        }
        
        # Timeout curto para não travar o worker se a API demorar
        response = requests.get(url, headers=headers, timeout=3)
        
        if response.status_code == 200:
            data = response.json()
            if data:
                lat = float(data[0]['lat'])
                lon = float(data[0]['lon'])
                # logger.info(f"📍 Geocode recuperado: {lat}, {lon}") # Descomente para debug
                return lat, lon
                
    except Exception as e:
        # logger.warning(f"Erro no Geocode Fallback: {e}")
        pass
        
    return 0.0, 0.0

def calcular_dias_mercado(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("📅 Calculando Dias no Mercado (DOM)...")
    
    # Converte a data de string para datetime
    df['dt_anuncio'] = pd.to_datetime(df['anuncio_criado'], errors='coerce')
    
    # Data de hoje (data da coleta)
    hoje = pd.Timestamp.now().normalize()
    
    # Calcula a diferença em dias
    df['dias_no_mercado'] = (hoje - df['dt_anuncio']).dt.days.fillna(0).astype(int)
    
    # Classificação para o Power BI
    def classificar_tempo(dias):
        if dias <= 7: return "Recém Chegado (Hype)"
        if dias <= 30: return "Recente (1 Mês)"
        if dias <= 90: return "Normal (3 Meses)"
        if dias <= 180: return "Encalhado (6 Meses)"
        return "Zombie (> 6 Meses - Barganha Extrema)"
        
    df['status_temporal'] = df['dias_no_mercado'].apply(classificar_tempo)
    
    return df

def calcular_indice_conforto_plus(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("🌟 Calculando Índice de Conforto 2.0 (Bem-Estar + Infra + Praticidade)...")
    
    # 1. Definição de Pesos (Total Ideal ~ 100 pontos)
    # A soma teórica máxima passa de 100, então vamos travar no final.
    
    # Infraestrutura (O Básico)
    w_infra = {
        'tem_vaga': 15,             # Carro é essencial/valorizado
        'tem_portaria_24h': 10,     # Segurança
        'tem_elevador': 10,         # Acessibilidade
        'tem_varanda': 8,           # Respiro
        'tem_ar_condicionado': 5,   # Sobrevivência no Rio
        'tem_armario_embutido': 2,  # Praticidade
        'tem_box_blindex': 1
    }
    
    # Lazer (O Clube)
    w_lazer = {
        'tem_piscina': 5,
        'tem_academia': 4,
        'tem_churrasqueira': 3,
        'tem_salao_festas': 2,
        'tem_sauna': 2,
        'tem_varanda_gourmet': 5    # Upgrade da varanda
    }
    
    # Bem-Estar (Extraído via NLP - As "Jóias")
    w_bem_estar = {
        'tag_sol_manha': 8,         # Menos calor, mais valorizado
        'tag_silencioso': 6,        # Ouro na Zona Sul/Norte
        'tag_indevassavel': 5,      # Privacidade
        'tag_vista_mar': 5,         # Visual
        'tag_reformado_arquiteto': 5, # Pronto para morar
        'tag_vazio': 2              # Mudança rápida
    }
    
    # 2. Inicializa o Score
    # Garante que a coluna de vaga seja numérica para o cálculo
    vagas_safe = pd.to_numeric(df['vagas'], errors='coerce').fillna(0)
    score = (vagas_safe > 0).astype(int) * w_infra['tem_vaga']
    
    # 3. Loop de Infra e Lazer (Checklists do Site)
    for col, peso in {**w_infra, **w_lazer}.items():
        if col in df.columns and col != 'tem_vaga': # Vaga já foi
            score += df[col].fillna(0).astype(int) * peso
            
    # 4. Loop de Bem-Estar (Tags NLP)
    for col, peso in w_bem_estar.items():
        if col in df.columns:
            score += df[col].fillna(0).astype(int) * peso
            
    # 5. Bônus Geográfico (Conveniência é Conforto)
    # Se tiver metrô a menos de 700m = +10 pontos
    if 'dist_transporte_km' in df.columns:
        # Cria uma série de bônus onde distância < 0.7 ganha 10, senão 0
        bonus_metro = (pd.to_numeric(df['dist_transporte_km'], errors='coerce').fillna(99) < 0.7).astype(int) * 10
        score += bonus_metro
        
    # Se tiver mercado a menos de 400m = +5 pontos (ir a pé com sacola)
    if 'dist_mercado_km' in df.columns:
        bonus_mercado = (pd.to_numeric(df['dist_mercado_km'], errors='coerce').fillna(99) < 0.4).astype(int) * 5
        score += bonus_mercado

    # 6. Normalização (0 a 100)
    # Trava em 100 para não quebrar gráficos, mas permite que imóveis top gabaritem
    df['score_conforto'] = score.clip(upper=100)
    
    # 7. Classificação Textual (Para Filtros no Power BI)
    def classificar(s):
        if s >= 80: return "💎 Super Conforto (Premium)"
        if s >= 60: return "🥇 Muito Confortável"
        if s >= 40: return "🥈 Padrão / Bom"
        if s >= 20: return "🥉 Básico"
        return "⚠️ Simples / Precisa Melhorias"
        
    df['classificacao_conforto'] = df['score_conforto'].apply(classificar)
    
    return df

def detectar_urgencia_venda(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("🔥 Procurando gatilhos de urgência na descrição...")
    
    desc = df['descricao'].str.lower().fillna("")
    
    gatilhos = [
        "motivo viagem", "mudança", "inventário", "urgente", 
        "oportunidade", "abaixo da avaliação", "baixou", 
        "oferta", "proposta", "liquidez"
    ]
    
    # Cria uma regex única (ex: "motivo viagem|mudança|inventário")
    regex_urgencia = '|'.join(gatilhos)
    
    df['gatilho_urgencia'] = desc.str.contains(regex_urgencia, regex=True).astype(int)
    
    # Se tiver gatilho E estiver há mais de 3 meses, é Oportunidade de Ouro
    df['alerta_oportunidade_ouro'] = ((df['gatilho_urgencia'] == 1) & (df['dias_no_mercado'] > 90)).astype(int)
    
    return df


def segmentar_mercado_dinamico(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("📊 Segmentando mercado via Estatística (Quantiles)...")
    
    # Remove zeros para não distorcer a estatística
    df_valido = df[df['valor_R$'] > 1000].copy()
    
    # Cria 5 faixas iguais baseadas na distribuição dos preços (Quintis)
    # Ex: Os 20% mais baratos, os 20% seguintes, etc.
    labels = ['Econômico (Top 20% Baratos)', 'Médio-Baixo', 'Médio-Alto', 'Alto Padrão', 'Luxo (Top 20% Caros)']
    
    try:
        # qcut divide os dados em 'baldes' de quantidades iguais
        df['segmento_mercado'] = pd.qcut(df['valor_R$'], q=5, labels=labels)
    except ValueError:
        # Fallback caso tenhamos poucos dados únicos
        df['segmento_mercado'] = "Padrão"

    # Fazemos o mesmo para o tamanho (m2)
    labels_area = ['Compacto', 'Padrão', 'Confortável', 'Espaçoso', 'Gigante']
    try:
        df['perfil_tamanho'] = pd.qcut(df['area_m2'], q=5, labels=labels_area)
    except:
        df['perfil_tamanho'] = "Padrão"
        
    return df

def haversine(lat1, lon1, lat2, lon2):
    """Calcula distância em km entre dois pontos (Latitude/Longitude)."""
    R = 6371  # Raio da terra em km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def analisar_saturacao_rua(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("🚨 Analisando concentração de ofertas (Risco de Fuga)...")
    
    # 1. Conta quantos imóveis tem em cada rua
    oferta_rua = df.groupby(['bairro', 'rua'])['url'].count().reset_index()
    oferta_rua.rename(columns={'url': 'qtd_na_rua'}, inplace=True)
    
    # 2. Calcula a média de imóveis por rua DENTRO daquele bairro
    media_bairro = oferta_rua.groupby('bairro')['qtd_na_rua'].mean().reset_index()
    media_bairro.rename(columns={'qtd_na_rua': 'media_ofertas_por_rua_no_bairro'}, inplace=True)
    
    # Junta tudo
    df = pd.merge(df, oferta_rua, on=['bairro', 'rua'], how='left')
    df = pd.merge(df, media_bairro, on='bairro', how='left')
    
    def classificar_risco(row):
        qtd = row['qtd_na_rua']
        media = row['media_ofertas_por_rua_no_bairro']
        
        # Se a rua tem menos de 3 ofertas, é irrelevante estatisticamente
        if qtd < 3: return "Normal"
        
        # Lógica: Se a rua tem 3x mais ofertas que a média das ruas do bairro
        if qtd > (media * 3):
            return "ALERTA: Fuga Possível (Oferta Muito Alta)"
        elif qtd > (media * 1.5):
            return "Oferta Elevada"
        else:
            return "Normal/Baixa"
            
    df['alerta_oferta_rua'] = df.apply(classificar_risco, axis=1)
    
    return df

def enriquecer_geo_contexto_avancado(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("🌍 Calculando Geo-Contexto 360º (Mercados, Feiras, BRT, Pedro II)...")

    def analisar_imovel(row):
        # --- 1. Validação de Coordenadas ---
        if pd.isna(row.get('coordenadas')) or ',' not in str(row.get('coordenadas')):
            # Retorna Nulos para todas as colunas novas (20 colunas)
            return pd.Series([None] * 20)

        try:
            lat_imov, lon_imov = map(float, str(row['coordenadas']).split(','))
        except:
            return pd.Series([None] * 20)

        # --- 2. Helper de Busca (Retorna Lista Ordenada) ---
        def get_sorted_items(categoria, limit=1):
            """Busca os N itens mais próximos em uma categoria do POIS_RIO."""
            items_dist = []
            # O .get evita erro se a chave não existir no dicionário
            for nome, (lat, lon) in POIS_RIO.get(categoria, {}).items():
                dist = haversine(lat_imov, lon_imov, lat, lon)
                items_dist.append((dist, nome))
            
            # Ordena do mais perto para o mais longe
            items_dist.sort(key=lambda x: x[0])
            
            # Se a lista estiver vazia (categoria sem dados), retorna placeholders
            if not items_dist:
                return [(99.9, "N/A")] * limit
                
            return items_dist[:limit]

        # --- 3. Buscas Individuais (O Campeão de cada categoria) ---
        
        # Transporte: Faz um "Mashup" de Metrô, Trem e BRT para ver qual ganha
        transporte_heavy = get_sorted_items('transporte_hub', 3) # Metrô/Trem
        transporte_brt = get_sorted_items('brt_stations', 3)     # BRT
        
        # Junta e ordena para pegar o campeão absoluto de mobilidade
        todos_transportes = transporte_heavy + transporte_brt
        todos_transportes.sort(key=lambda x: x[0])
        top_transporte = todos_transportes[0] # O vencedor
        
        # Infraestrutura e Lazer
        top_praia = get_sorted_items('praias', 1)[0]
        top_lazer = get_sorted_items('lazer_verde', 1)[0]
        top_cultura = get_sorted_items('cultura_esporte', 1)[0]
        top_shop  = get_sorted_items('shoppings_premium', 1)[0]
        
        # Abastecimento
        top_mercado = get_sorted_items('mercados_essenciais', 1)[0]
        top_feira = get_sorted_items('feiras_alimentacao', 1)[0]
        
        # Saúde e Educação (Top 3 solicitados)
        top3_saude = get_sorted_items('saude_educacao', 3)
        
        # Segurança
        dist_risco, nome_risco = get_sorted_items('areas_sensiveis', 1)[0]
        dist_policia, nome_policia = get_sorted_items('seguranca_publica', 1)[0]

        # --- 4. Montagem do RESUMO VISUAL (Tooltip do Power BI) ---
        resumo_parts = []
        
        # A. Transporte Principal
        icone_transp = "🚌" if "BRT" in top_transporte[1] or "Estação" in top_transporte[1] else "🚇"
        resumo_parts.append(f"{icone_transp} {top_transporte[1]} ({top_transporte[0]:.1f}km)")
        
        # B. Mercado (Com alerta se longe)
        dist_m = top_mercado[0]
        icone_mercado = "🛒" if dist_m < 2.0 else "⚠️🛒"
        resumo_parts.append(f"{icone_mercado} {top_mercado[1]} ({dist_m:.1f}km)")
        
        # C. Feira (Só mostra se for caminhável < 1.5km)
        if top_feira[0] < 1.5:
            resumo_parts.append(f"🍎 {top_feira[1]} ({top_feira[0]:.1f}km)")
            
        # D. Lazer/Shopping (Praia, Shopping ou Verde)
        resumo_parts.append(f"🏖️ {top_praia[1]} ({top_praia[0]:.1f}km)")
        if top_shop[0] < 4.0: # Só mostra shopping se for relevante
            resumo_parts.append(f"🛍️ {top_shop[1]} ({top_shop[0]:.1f}km)")
            
        # E. Saúde e Educação (Os 3 itens pedidos)
        saude_str = ", ".join([f"{item[1]} ({item[0]:.1f}km)" for item in top3_saude])
        resumo_parts.append(f"🏥/🎓 {saude_str}")
        
        itens_proximos_str = " | ".join(resumo_parts)

        # --- 5. Geração de Link Maps ---
        link_maps = f"https://www.google.com/maps/search/?api=1&query={lat_imov},{lon_imov}"

        # --- 6. Scores e Índices ---
        
        # Score Mobilidade (0-10)
        dist_t = top_transporte[0]
        score_mobilidade = max(0, 10 - (dist_t * 2))
        if dist_t < 0.6: score_mobilidade = 10
        elif dist_t < 1.5: score_mobilidade = 8
        
        # Score Segurança (Saldo Líquido)
        # Começa com 5. Perde se perto de risco. Ganha se perto de polícia.
        score_seguranca = 5.0
        if dist_risco < 0.5: score_seguranca -= 4
        elif dist_risco < 1.0: score_seguranca -= 2
        
        if dist_policia < 0.8: score_seguranca += 2.5
        elif dist_policia < 2.0: score_seguranca += 1
        score_seguranca = max(0, min(10, score_seguranca)) # Trava entre 0 e 10
        
        # Walkability Index (Quantos serviços essenciais a < 1.5km?)
        servicos_perto = 0
        lista_verificacao = [top_transporte[0], top_mercado[0], top_feira[0], top_lazer[0], top_praia[0], top_shop[0]]
        for d in lista_verificacao:
            if d < 1.5: servicos_perto += 1
            
        walkability = "Baixa"
        if servicos_perto >= 4: walkability = "Excelente (Tudo a pé)"
        elif servicos_perto >= 2: walkability = "Média"

        # Vocação Airbnb (Simples)
        vocacao_airbnb = "Baixa"
        is_zona_sul = any(b in str(row['bairro']) for b in ['Copacabana', 'Ipanema', 'Leblon', 'Botafogo', 'Flamengo'])
        if is_zona_sul and (top_praia[0] < 0.8 or top_transporte[0] < 0.5): 
            vocacao_airbnb = "Altíssima"
        
        return pd.Series([
            # Distâncias brutas para filtros numéricos
            top_praia[0], top_praia[1],           
            top_transporte[0], top_transporte[1], 
            top_mercado[0], top_mercado[1],
            top_feira[0], top_feira[1],
            top_lazer[0], top_lazer[1],
            top_cultura[0], top_cultura[1],
            
            # Segurança
            dist_risco, nome_risco,               
            dist_policia, nome_policia,           
            
            # Indicadores de Negócio
            round(score_mobilidade, 1), 
            round(score_seguranca, 1),
            walkability,
            vocacao_airbnb,
            
            # Texto Rico e Links
            itens_proximos_str,
            link_maps 
        ])

    # Lista de colunas (Deve bater com a quantidade do pd.Series acima = 20)
    cols = [
        'dist_praia_km', 'praia_prox', 
        'dist_transporte_km', 'transporte_prox',
        'dist_mercado_km', 'mercado_prox', 
        'dist_feira_km', 'feira_prox',
        'dist_lazer_km', 'lazer_prox',
        'dist_cultura_km', 'cultura_prox',
        
        'dist_risco_km', 'risco_prox',
        'dist_policia_km', 'policia_prox',
        
        'score_mobilidade', 'score_seguranca', 'walkability_index', 'vocacao_airbnb',
        'itens_proximos_resumo',
        'link_google_maps'
    ]
    
    df[cols] = df.apply(analisar_imovel, axis=1)
    return df

def analise_financeira_avancada(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("💰 Executando Análise Financeira 2.0 (Caixa Econômica)...")
    
    # Constantes de Mercado (Estimativas RJ)
    TAXA_ITBI = 0.03 # 3%
    TAXA_CARTORIO = 0.015 # 1.5% estimado
    
    # Constantes de Financiamento (Caixa - Tabela SAC Estimada)
    PERCENTUAL_FINANCIAVEL = 0.80 # 80%
    JUROS_ANUAIS_EFETIVOS = 0.105 # 10.5% a.a (Média de mercado para usados)
    PRAZO_MESES = 420 # 35 anos (Padrão máximo atual)
    AMORTIZACAO_MENSAL = 1 / PRAZO_MESES # Constante na SAC

    # 1. Custo Total de Aquisição (Preço + Taxas)
    df['custo_aquisicao_total'] = df['valor_R$'] * (1 + TAXA_ITBI + TAXA_CARTORIO)
    
    # --- NOVO: SIMULADOR DE FINANCIAMENTO ---
    
    # 2. Entrada Mínima (20%)
    df['entrada_minima'] = df['valor_R$'] * (1 - PERCENTUAL_FINANCIAVEL)
    
    # 3. Valor Financiado
    df['valor_financiado'] = df['valor_R$'] * PERCENTUAL_FINANCIAVEL
    
    # 4. Primeira Parcela (Estimativa SAC)
    # Fórmula SAC Simplificada: (Saldo / Prazo) + (Saldo * JurosMensais)
    juros_mensais = (1 + JUROS_ANUAIS_EFETIVOS)**(1/12) - 1
    
    # Calcula amortização + juros sobre o total
    amortizacao = df['valor_financiado'] * AMORTIZACAO_MENSAL
    juros = df['valor_financiado'] * juros_mensais
    
    df['primeira_parcela_estimada'] = (amortizacao + juros).round(2)
    
    # 5. Renda Mínima Exigida (Parcela não pode passar de 30% da renda)
    df['renda_minima_familiar'] = (df['primeira_parcela_estimada'] / 0.30).round(2)
    
    # ----------------------------------------

    # 6. Custo Fixo Mensal (O "Burn Rate" do imóvel vazio)
    df['custo_fixo_mensal'] = df['condominio_R$'] + (df['iptu_R$'] / 12)
    
    # 7. Estimativa de Aluguel (Baseada no Yield médio de mercado)
    df['aluguel_estimado'] = df['valor_R$'] * 0.0045
    
    # 8. Potencial de Lucro Líquido (Aluguel - Condomínio - IPTU)
    df['fluxo_caixa_mensal'] = df['aluguel_estimado'] - df['custo_fixo_mensal']
    
    # 9. Payback Simples (Em anos)
    df['anos_payback'] = df.apply(
        lambda row: (row['custo_aquisicao_total'] / (row['fluxo_caixa_mensal'] * 12)) 
        if row['fluxo_caixa_mensal'] > 0 else 999, axis=1
    )
    df['anos_payback'] = df['anos_payback'].round(1)

    # 10. Score de Investimento
    def calcular_score(row):
        score = 5 
        if row.get('diferenca_percentual', 0) < -15: score += 2 
        elif row.get('diferenca_percentual', 0) > 15: score -= 2 
        
        if row.get('dist_transporte_km', 99) < 0.8: score += 1
        
        if row['custo_fixo_mensal'] < 1000: score += 1
        elif row['custo_fixo_mensal'] > 2500: score -= 1
        
        return min(max(score, 0), 10)

    df['score_investimento'] = df.apply(calcular_score, axis=1)

    return df

def extrair_tags_avancadas(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("💎 Executando Mineração de Dados Imobiliários (NLP Avançado)...")
    
    # 1. Garante que a descrição seja tratada como string (blindagem)
    desc = df['descricao'].astype(str).str.lower().fillna("")
    
    # 2. Só tenta somar o título se a coluna realmente existir
    if 'titulo' in df.columns:
        titulos = df['titulo'].astype(str).str.lower().fillna("")
        desc = desc + " " + titulos
    
    # --- 1. ARQUITETURA DE ELITE ---
    df['tag_lamina'] = desc.str.contains(r'\bl[aâ]mina\b', regex=True).astype(int)
    df['tag_planta_circular'] = desc.str.contains(r'planta circular|circular', regex=True).astype(int)
    df['tag_cobertura_linear'] = desc.str.contains(r'cobertura linear|linear', regex=True).astype(int)
    df['tag_pe_direito_alto'] = desc.str.contains(r'p[eé] direito alto|pé-direito', regex=True).astype(int)
    df['tag_janelao'] = desc.str.contains(r'janel[aã]o|janelas amplas', regex=True).astype(int)

    # --- 2. POSIÇÃO E VISTA (Corrigido com ?:) ---
    df['tag_indevassavel'] = desc.str.contains(r'indevass[aá]vel|vista livre', regex=True).astype(int)
    
    # Grupos de não-captura (?:...) para evitar UserWarning
    df['tag_vista_cristo'] = desc.str.contains(r'vista.*(?:cristo|redentor|corcovado)', regex=True).astype(int)
    df['tag_vista_pao_acucar'] = desc.str.contains(r'vista.*(?:p[aã]o de a[cç][uú]car|enseada)', regex=True).astype(int)
    df['tag_vista_mar'] = desc.str.contains(r'vista.*(?:mar|oceano|praia)', regex=True).astype(int)
    
    df['tag_sol_passante'] = desc.str.contains(r'sol passante', regex=True).astype(int)
    df['tag_sol_manha'] = desc.str.contains(r'sol (?:da|pela) manh[ãa]', regex=True).astype(int)

    # --- 3. ESTADO DO IMÓVEL ---
    df['tag_reformado_arquiteto'] = desc.str.contains(r'reformado por arquiteto|projeto de ilumina[cç][aã]o|fino acabamento', regex=True).astype(int)
    
    # Grupo de não-captura aqui também
    df['tag_estado_original'] = desc.str.contains(r'estado original|precisa de (?:obra|moderniza[cç][aã]o)|antigo', regex=True).astype(int)
    
    df['tag_retrofit'] = desc.str.contains(r'retrofit|toda hidr[aá]ulica e el[eé]trica nova', regex=True).astype(int)

    # --- 4. EXCLUSIVIDADE & CONFORTO ---
    df['tag_centro_terreno'] = desc.str.contains(r'centro de terreno|afastado', regex=True).astype(int)
    df['tag_exclusivo'] = desc.str.contains(r'um por andar|1 por andar|hall privativo|elevador privativo', regex=True).astype(int)
    df['tag_silencioso'] = desc.str.contains(r'silencioso|silêncio|rua tranquila|arborizada', regex=True).astype(int)

    # --- 5. LOGÍSTICA & NEGÓCIO ---
    df['tag_vazio'] = desc.str.contains(r'vazio|chaves|entrega imediata|desocupado', regex=True).astype(int)
    
    # Grupo de não-captura aqui também
    df['tag_doc_ok'] = desc.str.contains(r'documenta[cç][aã]o (?:ok|cristalina|perfeita)|aceita financiamento', regex=True).astype(int)

    return df

# --- FUNÇÕES DE LIMPEZA E TRATAMENTO ---

def aplicar_regras_qualidade(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"🧼 Iniciando tratamento de qualidade. Dados brutos: {len(df)}")
    
    # Canonicalização
    df['id_imovel'] = df['url'].apply(lambda x: re.search(r'id-(\d+)', str(x)).group(1) if re.search(r'id-(\d+)', str(x)) else x)
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
    
    # Outliers (só aplica se tiver dados suficientes)
    if len(df_clean) > 20:
        q_low = df_clean['preco_m2'].quantile(0.01)
        q_hi  = df_clean['preco_m2'].quantile(0.99)
        df_final = df_clean[(df_clean['preco_m2'] > q_low) & (df_clean['preco_m2'] < q_hi)].copy()
    else:
        df_final = df_clean
    
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


# --- FUNÇÕES AUXILIARES ---

def limpar_temps():
    for f in glob.glob("temp_worker_*.csv"):
        try: os.remove(f)
        except: pass

def start_driver() -> uc.Chrome:
    with DRIVER_LOCK:
        options = uc.ChromeOptions()
        options.add_argument("--disable-blink-features=AutomationControlled")
        
        # --- MELHORIA 1: TURBO MODE (BLOQUEIO DE IMAGENS) ---
        prefs = {
            "profile.managed_default_content_settings.images": 2, # 2 = Bloquear carregamento de imagens
            "profile.default_content_setting_values.notifications": 2, # Bloquear notificações
        }
        options.add_experimental_option("prefs", prefs)
        
        # --- MELHORIA 2: ROTAÇÃO DE USER-AGENT ---
        ua = random.choice(USER_AGENTS_LIST)
        options.add_argument(f'--user-agent={ua}')

        driver = uc.Chrome(options=options)
        driver.set_window_size(1100, 800)
    return driver

def scroll_inteligente(driver):
    """
    Simula um humano rolando a página.
    """
    body = driver.find_element(By.TAG_NAME, "body")
    for _ in range(5):
        body.send_keys(Keys.PAGE_DOWN)
        time.sleep(random.uniform(0.3, 0.6)) # Mais rápido pois não tem imagem para carregar
    body.send_keys(Keys.PAGE_UP)
    time.sleep(0.5)
    body.send_keys(Keys.END)
    time.sleep(2) # Espera render do React

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

def parse_pt_date_to_iso(pt_date: str) -> str:
    m = re.search(r'(\d{1,2})\s+de\s+([A-Za-zç]+)\s+de\s+(\d{4})', pt_date, re.IGNORECASE)
    if not m: return pt_date 
    day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
    month = MONTHS_PT.get(month_name)
    if not month: return pt_date
    return f"{int(year):04d}-{month:02d}-{int(day):02d}"

# --- EXTRAÇÃO LISTAGEM (PRODUTOR) ---

def parse_card_resumo_e_encaminhar(card_element: WebElement) -> Optional[Dict]:
    try:
        data = {}
        data['url'] = safe_get_attribute(card_element, 'a', 'href', '')
        if not data['url']: return None

        price_txt = safe_get_text(card_element, 'div[data-cy="rp-cardProperty-price-txt"] p.text-2-25')
        data['valor_R$'] = extract_number(price_txt)
        data['area_m2'] = extract_number(safe_get_text(card_element, 'li[data-cy="rp-cardProperty-propertyArea-txt"] h3'))
        data['quartos'] = extract_number(safe_get_text(card_element, 'li[data-cy="rp-cardProperty-bedroomQuantity-txt"] h3'))
        data['vagas']   = extract_number(safe_get_text(card_element, 'li[data-cy="rp-cardProperty-parkingSpacesQuantity-txt"] h3'))
        data['banheiros'] = extract_number(safe_get_text(card_element, 'li[data-cy="rp-cardProperty-bathroomQuantity-txt"] h3'))
        
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

        data['rua'] = safe_get_text(card_element, 'p[data-cy="rp-cardProperty-street-txt"]', 'Rua Não Informada')

        costs_text = safe_get_text(card_element, 'div[data-cy="rp-cardProperty-price-txt"] p.text-1-75')
        data['condominio_R$'] = 0.0
        data['iptu_R$'] = 0.0
        if costs_text:
            cond_match = re.search(r'Cond\.\s*(R\$\s*[\d\.,]+)', costs_text, re.IGNORECASE)
            if cond_match: data['condominio_R$'] = extract_decimal_number(cond_match.group(1))
            iptu_match = re.search(r'IPTU\s*(R\$\s*[\d\.,]+)', costs_text, re.IGNORECASE)
            if iptu_match: data['iptu_R$'] = extract_decimal_number(iptu_match.group(1))

        data['destaque'] = safe_get_text(card_element, 'li[data-cy="rp-cardProperty-tag-txt"]', 'Sem Destaque')
        data['imagem_url'] = safe_get_attribute(card_element, 'img', 'src', 'Imagem Não Encontrada')

        return data
    except Exception:
        return None

def produtor_listagem(tarefa: Dict):
    label = f"{tarefa['zona']['nome']} [{tarefa['worker_id']}]"
    slug = tarefa['zona']['slug']
    paginas = tarefa['paginas']
    
    logger.info(f"📡 PRODUTOR INICIADO: {label}")
    driver = None
    try:
        time.sleep(random.uniform(1, 4))
        driver = start_driver()
        base_url = f"https://www.zapimoveis.com.br/venda/imoveis/{slug}/"

        for pagina in paginas:
            try:
                driver.get(f"{base_url}?pagina={pagina}")
                
                if "Checking your browser" in driver.title:
                    logger.warning(f"[{label}] Cloudflare check. Aguardando...")
                    time.sleep(8)
                
                scroll_inteligente(driver)
                
                card_elements = driver.find_elements(By.CSS_SELECTOR, 'li[data-cy="rp-property-cd"]')
                
                if len(card_elements) < 5:
                    scroll_inteligente(driver)
                    card_elements = driver.find_elements(By.CSS_SELECTOR, 'li[data-cy="rp-property-cd"]')

                count_pag = 0
                for el in card_elements:
                    resumo = parse_card_resumo_e_encaminhar(el)
                    if resumo:
                        LINK_QUEUE.put(resumo)
                        count_pag += 1
                
                logger.info(f"➡️ [{label}] Pág {pagina}: {count_pag} links encontrados.")
            except Exception as e:
                logger.error(f"[{label}] Erro pág {pagina}: {e}")
                
    except Exception as e:
        logger.error(f"[{label}] Erro fatal: {e}")
    finally:
        if driver: 
            try: driver.quit()
            except: pass
    logger.info(f"🏁 PRODUTOR FINALIZADO: {label}")

# --- FASE 2: CONSUMIDOR (DETALHES COM SAVE-AS-YOU-GO) ---
def enriquecer_detalhes_worker(driver, card_data: Dict):
    url = card_data.get('url')
    if not url: return

    try:
        driver.get(url)
        if "Checking your browser" in driver.title: time.sleep(5)
        time.sleep(1.5) # Tempo de respiro para o React

        # --- DADOS BÁSICOS (Cabeçalho) ---
        card_data['descricao'] = safe_get_text(driver, "p[data-testid='description-content']")
        
        end_raw = safe_get_text(driver, "p[data-testid='address-info-value']")
        if not end_raw:
             end_raw = safe_get_text(driver, "p[data-testid='location-address']")
        
        card_data['endereco_completo'] = end_raw
        card_data['numero'] = extract_number(end_raw)
        card_data["corretora"] = safe_get_text(driver, "a[data-testid='official-store-redirect-link']")
        
        # Numéricos principais (Ficam no topo, fora das abas)
        card_data['suites'] = extract_number(safe_get_text(driver, "li[itemprop='numberOfSuites']"))
        card_data['quartos'] = extract_number(safe_get_text(driver, "li[itemprop='numberOfRooms']"))
        card_data['andar'] = extract_number(safe_get_text(driver, "li[itemprop='floorLevel']"))

        # --- FUNÇÃO DE EXTRAÇÃO POR PAINEL (ISOLAMENTO DE ESCOPO) ---
        def processar_aba(painel_id, feature_map):
            try:
                # 1. Encontra o painel específico (Imóvel ou Condomínio)
                painel = driver.find_element(By.ID, painel_id)
                
                # 2. Tenta expandir a lista APENAS dentro desse painel
                try:
                    btn_ver_mais = painel.find_element(By.CSS_SELECTOR, "button[data-cy='ldp-TextCollapse-btn']")
                    if btn_ver_mais.is_displayed():
                        driver.execute_script("arguments[0].click();", btn_ver_mais)
                        time.sleep(0.5)
                except: 
                    pass # Lista pequena, sem botão

                # 3. Varre os itens APENAS dentro desse painel
                for key, selector in feature_map.items():
                    try:
                        # O segredo: painel.find_element em vez de driver.find_element
                        painel.find_element(By.CSS_SELECTOR, selector)
                        card_data[key] = True
                    except:
                        card_data[key] = False
            except:
                # Se o painel não existir (ex: casa sem condominio), seta tudo False
                for key in feature_map:
                    card_data[key] = False

        # --- MAPEAMENTOS ---
        map_imovel = {
            'aceita_pet': "li[itemprop='PETS_ALLOWED']",
            'tem_vista_pro_mar': "li[itemprop='SEA_VIEW']",
            'tem_janela_grande': "li[itemprop='LARGE_WINDOW']",
            'tem_ar_condicionado': "li[itemprop='AIR_CONDITIONING']",
            'tem_banheira': "li[itemprop='BATHTUB']",
            'tem_banheiro_servico': "li[itemprop='SERVICE_BATHROOM']",
            'tem_armario_cozinha': "li[itemprop='KITCHEN_CABINETS']",
            'tem_armario_banheiro': "li[itemprop='BATHROOM_CABINETS']",
            'tem_piso_madeira': "li[itemprop='WOOD_FLOOR']",
            'tem_box_blindex': "li[itemprop='BLINDEX_BOX']",
            'tem_area_servico': "li[itemprop='SERVICE_AREA']",
            'tem_closet': "li[itemprop='CLOSET']",
            'tem_copa': "li[itemprop='COPA']",
            'tem_varanda': "li[itemprop='BALCONY']",
            'tem_varanda_gourmet': "li[itemprop='GOURMET_BALCONY']",
        }

        map_condominio = {
            'tem_portaria_24h': "li[itemprop='CONCIERGE_24H']",
            'tem_armario_embutido': "li[itemprop='BUILTIN_WARDROBE']",
            'tem_estacionamento': "li[itemprop='PARKING']",
            'tem_academia': "li[itemprop='GYM']",
            'tem_salao_festas': "li[itemprop='PARTY_HALL']",
            'tem_piscina': "li[itemprop='POOL']",
            'tem_interfone': "li[itemprop='INTERCOM']",
            'tem_sala_massagem': "li[itemprop='MASSAGE_ROOM']",
            'tem_churrasqueira': "li[itemprop='BARBECUE_GRILL']",
            'tem_quadra_poliesportiva': "li[itemprop='SPORTS_COURT']",
            'tem_sauna': "li[itemprop='SAUNA']",
            'tem_playground': "li[itemprop='PLAYGROUND']",
            'tem_squash': "li[itemprop='SQUASH']",
            'tem_condominio_fechado': "li[itemprop='GATED_COMMUNITY']",
            'tem_elevador': "li[itemprop='ELEVATOR']",
            'tem_loja': "li[itemprop='STORES']",
            'tem_administracao': "li[itemprop='ADMINISTRATION']",
            'tem_zelador': "li[itemprop='CARETAKER']"
        }

        # --- EXECUÇÃO ---

        # 1. Processa Aba Imóvel (Já vem aberta, ID: panel-unitAmenities)
        processar_aba("panel-unitAmenities", map_imovel)

        # 2. Navega e Processa Aba Condomínio (ID: panel-sectionAmenities)
        try:
            # Busca o botão da aba pelo ID fornecido no seu HTML
            tab_condo = driver.find_element(By.ID, "sectionAmenities")
            
            # Clica apenas se não estiver ativo
            if "olx-tabs__tab--active" not in tab_condo.get_attribute("class"):
                driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", tab_condo)
                time.sleep(0.3)
                driver.execute_script("arguments[0].click();", tab_condo)
                time.sleep(1.0) # Espera o painel carregar/renderizar
            
            # Processa o painel de condomínio
            processar_aba("panel-sectionAmenities", map_condominio)
            
        except Exception:
            # Se falhar ao clicar na aba (ex: não tem condomínio), preenche com False
            for k in map_condominio: card_data[k] = False

        # --- DADOS FINAIS ---
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

        # --- MAPA (COM FALLBACK INTELIGENTE) ---
        lat_val = 0.0
        lon_val = 0.0
        origem_geo = "Nao Encontrado"

        # TENTATIVA 1: Extrair do Iframe (Mais preciso se existir)
        try:
            iframe_preview = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[data-testid='map-iframe']"))
            )
            src_url = iframe_preview.get_attribute("src") or ""
            match = re.search(r'(-?\d{1,2}\.\d+)[,\s]+(-?\d{1,3}\.\d+)', src_url)
            
            if match:
                lat_val = float(match.group(1))
                lon_val = float(match.group(2))
                origem_geo = "Site ZAP"
        except:
            pass # Falhou a extração do site, segue para o plano B

        # TENTATIVA 2: Fallback via API (Se a tentativa 1 falhou)
        if lat_val == 0.0:
            # Usa o endereço que já extraímos lá em cima
            if card_data.get('endereco_completo'):
                # Pequeno delay para respeitar a API pública
                time.sleep(1.0) 
                lat_val, lon_val = geocode_fallback_nominatim(card_data['endereco_completo'])
                if lat_val != 0.0:
                    origem_geo = "API Nominatim"

        # Salva os dados finais
        card_data['latitude'] = lat_val
        card_data['longitude'] = lon_val
        card_data['origem_geo'] = origem_geo
        
        if lat_val != 0.0:
            card_data['coordenadas'] = f"{lat_val},{lon_val}"
        else:
            card_data['coordenadas'] = "Não encontradas"

    except Exception:
        pass
    
def consumidor_detalhes(worker_id: int):
    logger.info(f"🔨 CONSUMIDOR #{worker_id} PRONTO.")
    driver = None
    processed_count = 0
    filename_temp = f"temp_worker_{worker_id}.csv"
    
    file_exists = os.path.isfile(filename_temp)
    csv_file = open(filename_temp, 'a', newline='', encoding='utf-8-sig')
    
    # --- LISTA COMPLETA DE COLUNAS ATUALIZADA ---
    fieldnames = [
        # Identificação e Valores
        'url', 'valor_R$', 'area_m2', 'quartos', 'vagas', 'banheiros', 'suites', 'andar',
        'tipo', 'bairro', 'cidade', 'rua', 'numero', 'endereco_completo',
        'condominio_R$', 'iptu_R$', 
        
        # Detalhes do Anúncio
        'destaque', 'imagem_url', 'descricao', 'corretora',
        'nota_media', 'total_avaliacoes', 'anuncio_criado',
        'latitude', 'longitude', 'coordenadas', 'origem_geo',
        
        # --- Atributos do Imóvel (NOVOS) ---
        'aceita_pet', 'tem_vista_pro_mar', 'tem_varanda', 'tem_varanda_gourmet',
        'tem_janela_grande', 'tem_ar_condicionado', 'tem_banheira',
        'tem_banheiro_servico', 'tem_armario_cozinha', 'tem_armario_banheiro',
        'tem_armario_embutido', 'tem_closet', 'tem_copa',
        'tem_piso_madeira', 'tem_box_blindex', 'tem_area_servico',
        
        # --- Atributos do Condomínio (NOVOS) ---
        'tem_portaria_24h', 'tem_interfone', 'tem_condominio_fechado',
        'tem_elevador', 'tem_estacionamento',
        'tem_zelador', 'tem_administracao', 'tem_loja',
        
        # --- Lazer e Bem-Estar ---
        'tem_piscina', 'tem_sauna', 'tem_churrasqueira', 
        'tem_academia', 'tem_sala_massagem', 'tem_squash',
        'tem_salao_festas', 'tem_playground', 'tem_quadra_poliesportiva'
    ]
    
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter=';', extrasaction='ignore')
    
    if not file_exists:
        writer.writeheader()
        csv_file.flush()

    try:
        time.sleep(random.uniform(1, 10))
        driver = start_driver()
        
        while True:
            try:
                item = LINK_QUEUE.get(timeout=15)
            except queue.Empty:
                if LINK_QUEUE.qsize() == 0:
                    break
                continue
            
            if item is None: break 
                
            enriquecer_detalhes_worker(driver, item)
            writer.writerow(item)
            csv_file.flush()
            LINK_QUEUE.task_done()
            processed_count += 1
            
            # Restart periódico para liberar memória
            if processed_count % 50 == 0:
                driver.quit()
                time.sleep(2)
                driver = start_driver()

    except Exception as e:
        logger.error(f"Consumidor #{worker_id} falhou: {e}")
    finally:
        csv_file.close()
        if driver:
            try: driver.quit()
            except: pass
        logger.info(f"💤 Consumidor #{worker_id} encerrou. {processed_count} salvos em {filename_temp}.")
        
# --- PÓS-PROCESSAMENTO ---
def unificar_e_tratar():
    logger.info("--- UNIFICANDO E PROCESSANDO DADOS ---")
    all_files = glob.glob("temp_worker_*.csv")
    
    if not all_files:
        logger.error("Nenhum arquivo temporário encontrado!")
        return

    try:
        df_list = [pd.read_csv(f, sep=";") for f in all_files]
        df_full = pd.concat(df_list, ignore_index=True)
        
        # 1. Limpeza e Deduplicação
        df_full = aplicar_regras_qualidade(df_full)
        
        # 2. Inteligência de Preço (Media Rua/Bairro)
        df_full = calcular_media_inteligente(df_full, min_amostras_rua=3)
        
        # 3. Geo-Contexto (Distâncias Múltiplas) 
        df_full = enriquecer_geo_contexto_avancado(df_full)
        
        # 4. Análise Financeira (ROI/Payback)
        df_full = analise_financeira_avancada(df_full)
        
        # 5. Indicador de "Risco de Fuga"
        df_full = analisar_saturacao_rua(df_full)
        
        # 6. Segmentação Dinâmica
        df_full = segmentar_mercado_dinamico(df_full)
        
        # 7. Dias no Mercado
        df_full = calcular_dias_mercado(df_full)   
        
        # 8. Gatilhos de Urgência
        df_full = detectar_urgencia_venda(df_full)
        
        # 9. Mineração de Oportunidades
        df_full = extrair_tags_avancadas(df_full)
        
        # --- OTIMIZAÇÃO PARA POWER BI (NOVO) ---
        logger.info("📊 Otimizando tipos de dados para Power BI...")
        
        # A. Tratamento de Nulos em Numéricos (Evita erro em somas no PBI)
        cols_financeiras = [
            'condominio_R$', 'iptu_R$', 'valor_R$', 'area_m2', 'custo_fixo_mensal',
            'entrada_minima', 'valor_financiado', 'primeira_parcela_estimada', 'renda_minima_familiar'
        ]
        for col in cols_financeiras:
            if col in df_full.columns:
                df_full[col] = df_full[col].fillna(0)

        # B. Conversão de Booleanos (True/False) para (1/0)
        # Identifica colunas booleanas pelo prefixo ou conteúdo
        for col in df_full.columns:
            # Verifica se é coluna de flag (tem_, aceita_, tag_)
            if df_full[col].dtype == 'bool' or any(x in col for x in ['tem_', 'aceita_', 'tag_', 'flag_']):
                try:
                    # Passo 1: Força conversão para numérico (erros viram NaN)
                    # Isso resolve casos onde tem strings como "True", "Sim" ou sujeira
                    df_full[col] = pd.to_numeric(df_full[col], errors='coerce')
                    
                    # Passo 2: Preenche vazios com 0
                    df_full[col] = df_full[col].fillna(0)
                    
                    # Passo 3: Converte para Inteiro seguro
                    df_full[col] = df_full[col].astype(int)
                except Exception:
                    # Se mesmo assim falhar, mantemos como está para não quebrar o script
                    pass

        # C. Garante Data no formato ISO (YYYY-MM-DD) para Time Intelligence
        if 'anuncio_criado' in df_full.columns:
            df_full['anuncio_criado'] = pd.to_datetime(df_full['anuncio_criado'], errors='coerce').dt.strftime('%Y-%m-%d')

        # --- FUSÃO DE DADOS (CONSOLIDAÇÃO) ---
        # Cria colunas definitivas para facilitar o gráfico no Power BI
        logger.info("🔗 Fundindo colunas redundantes (Lógica OR)...")

        # 1. VISTA MAR DEFINITIVA
        # Se marcou no site OU escreveu na descrição -> É Vista Mar
        cols_vista = ['tem_vista_pro_mar', 'tag_vista_mar']
        cols_existentes = [c for c in cols_vista if c in df_full.columns]
        if cols_existentes:
            # .max(axis=1) funciona como um OR (se tiver 1 em qualquer uma, vira 1)
            df_full['final_tem_vista_mar'] = df_full[cols_existentes].max(axis=1)

        # 2. VARANDA DEFINITIVA
        # Varanda comum OU Varanda Gourmet -> Tem Varanda
        cols_varanda = ['tem_varanda', 'tem_varanda_gourmet']
        cols_existentes = [c for c in cols_varanda if c in df_full.columns]
        if cols_existentes:
            df_full['final_tem_varanda'] = df_full[cols_existentes].max(axis=1)

        # 3. LAZER RELEVANTE (Flag para filtro rápido)
        # Se tiver Piscina OU Churrasqueira OU Academia -> Tem Lazer
        cols_lazer = ['tem_piscina', 'tem_churrasqueira', 'tem_academia']
        cols_existentes = [c for c in cols_lazer if c in df_full.columns]
        if cols_existentes:
            df_full['final_tem_lazer'] = df_full[cols_existentes].max(axis=1)

        # 10. Índice de Conforto
        df_full = calcular_indice_conforto_plus(df_full)
    
        # ----------------------------------------
        
        # Salva o arquivo Master
        arquivo_final = "DATASET_RIO_BI_READY.csv"
        # Decimal="," é vital para o Power BI em Português
        df_full.to_csv(arquivo_final, sep=";", decimal=",", index=False, encoding="utf-8-sig")
        
        logger.info(f"🏆 SUCESSO! Dataset BI gerado: {arquivo_final}")
        logger.info(f"📊 Total Imóveis: {len(df_full)}")
        
        # Limpa temporários
        for f in all_files:
            try: os.remove(f)
            except: pass
            
    except Exception as e:
        logger.error(f"Erro ao processar final: {e}")

# ==============================================================================
#  MAIN
# ==============================================================================

if __name__ == "__main__":
    limpar_temps()
    
    tarefas_produtor = []
    for zona in ZONAS_CONFIG:
        if zona['split']:
            tarefas_produtor.append({"zona": zona, "paginas": range(1, NUM_PAGINAS+1, 2), "worker_id": "impar"})
            tarefas_produtor.append({"zona": zona, "paginas": range(2, NUM_PAGINAS+1, 2), "worker_id": "par"})
        else:
            tarefas_produtor.append({"zona": zona, "paginas": range(1, NUM_PAGINAS+1), "worker_id": "unico"})

    logger.info(f"--- INICIANDO SCRAPING BLINDADO (TURBO & STEALTH) ---")
    
    executor_consumidores = ThreadPoolExecutor(max_workers=MAX_DETAILS_WORKERS)
    futures_consumidores = [executor_consumidores.submit(consumidor_detalhes, i) for i in range(MAX_DETAILS_WORKERS)]

    with ThreadPoolExecutor(max_workers=MAX_LISTING_WORKERS) as executor_produtores:
        futures_produtores = [executor_produtores.submit(produtor_listagem, t) for t in tarefas_produtor]
        for f in as_completed(futures_produtores):
            f.result()

    logger.info("✅ Produtores finalizaram. Aguardando fila esvaziar...")
    
    for _ in range(MAX_DETAILS_WORKERS):
        LINK_QUEUE.put(None)
    
    for f in as_completed(futures_consumidores):
        try: f.result()
        except: pass

    unificar_e_tratar()