#!/usr/bin/env python3
"""
geocode_feiras.py — Extrai feiras do PDF da Prefeitura e geocodifica via Nominatim.
========================================================================================

Uso:
    python geocode_feiras.py RELACAO_FEIRAS2.pdf

Saída:
    - feiras_geocodificadas.json  (resultado completo com lat/lon)
    - feiras_config_snippet.py    (dict pronto para colar no config.py)
    - feiras_falhas.csv           (endereços que não foram encontrados)

Requer:
    pip install pdfplumber requests
"""
import json
import csv
import re
import sys
import time
import urllib.parse
from pathlib import Path

try:
    import pdfplumber
except ImportError:
    print("ERRO: instale pdfplumber → pip install pdfplumber")
    sys.exit(1)

import requests

# ==============================================================================
#  NORMALIZAÇÃO DE ENDEREÇO
# ==============================================================================

# Mapa de abreviações → nome completo
TIPO_LOGRADOURO = {
    "RUA": "Rua", "RUAS": "Rua",
    "AV": "Avenida", "AVN": "Avenida", "AVENIDA": "Avenida",
    "PCA": "Praça", "PRC": "Praça", "PRÇ": "Praça", "PRA": "Praça",
    "PRACA": "Praça", "PRAÇA": "Praça",
    "ETR": "Estrada", "ESTR": "Estrada", "ESTRADA": "Estrada",
    "VDT": "Viaduto", "VIADUTO": "Viaduto",
    "TVS": "Travessa", "TRAV": "Travessa", "TRAVESSA": "Travessa",
    "LGO": "Largo", "LARGO": "Largo",
    "BEC": "Beco", "BECO": "Beco",
    "ROD": "Rodovia", "RODOVIA": "Rodovia",
    "ALM": "Alameda", "ALAMEDA": "Alameda",
}


def normalizar_endereco(raw: str) -> str:
    """
    Normaliza endereço do formato da Prefeitura para formato de busca.
    
    Exemplos:
        "FELISBELO FREIRE, RUA"                  → "Rua Felisbelo Freire"
        "JOSE RUCAS, ETR"                        → "Estrada Jose Rucas"
        "VICENTE DE CARVALHO, AVN"               → "Avenida Vicente De Carvalho"
        "NICARAGUA, PCA"                         → "Praça Nicaragua"
        "RUA TEREZINA"                           → "Rua Terezina"
        "LARGO DO ANIL"                          → "Largo Do Anil"
        "TADEU KOSCIUSKO / CARLOS SAMPAIO, RUA"  → "Rua Tadeu Kosciusko"
        "EPITÁCIO PESSOA AV, ENTRE..."           → "Avenida Epitácio Pessoa"
        "ARNO KONDER, RUA / GOV..."              → "Rua Arno Konder"
    """
    raw = raw.strip()

    # Pré-limpeza: se tiver "/", pega só o primeiro trecho (intersecção é ruído)
    if "/" in raw:
        # Preserva o tipo se ele está DEPOIS do "/" (ex: "NOME / OUTRO, RUA")
        # Busca tipo após a última vírgula no texto COMPLETO
        tipo_final = ""
        if "," in raw:
            after_last_comma = raw.rsplit(",", 1)[1].strip().upper()
            if after_last_comma in TIPO_LOGRADOURO:
                tipo_final = after_last_comma
        
        primeiro = raw.split("/")[0].strip().rstrip(",").strip()
        
        # Reanexa o tipo se o primeiro trecho não tem um
        if tipo_final and "," not in primeiro:
            raw = f"{primeiro}, {tipo_final}"
        else:
            raw = primeiro

    # Remove sufixos como "ENTRE VISC...", "E NASCIMENTO..."
    raw = re.sub(r",?\s*ENTRE\s+.*$", "", raw, flags=re.IGNORECASE).strip()

    # Caso 1: já começa com tipo reconhecido (ex: "RUA TEREZINA", "LARGO DO ANIL")
    first_word = raw.split()[0].upper().rstrip(",") if raw.split() else ""
    if first_word in TIPO_LOGRADOURO:
        tipo = TIPO_LOGRADOURO[first_word]
        resto = " ".join(raw.split()[1:]).title()
        return f"{tipo} {resto}"

    # Caso 2: formato "NOME, TIPO" ou "NOME TIPO" com tipo no final
    if "," in raw:
        parts = raw.rsplit(",", 1)
        nome_parte = parts[0].strip()
        tipo_parte = parts[1].strip().upper()

        # Verifica se o que está depois da vírgula é realmente um tipo
        if tipo_parte in TIPO_LOGRADOURO:
            tipo = TIPO_LOGRADOURO[tipo_parte]
            return f"{tipo} {nome_parte.title()}"

    # Caso 3: tipo colado no final sem vírgula (ex: "EPITÁCIO PESSOA AV")
    words = raw.split()
    if len(words) >= 2:
        last_word = words[-1].upper()
        if last_word in TIPO_LOGRADOURO:
            tipo = TIPO_LOGRADOURO[last_word]
            nome = " ".join(words[:-1]).title()
            return f"{tipo} {nome}"

    # Caso 4: nenhum tipo reconhecido → retorna título
    return raw.title()


# ==============================================================================
#  EXTRAÇÃO DO PDF
# ==============================================================================

def extrair_feiras_pdf(pdf_path: str) -> list:
    """Extrai dados de feiras do PDF da Prefeitura."""
    feiras = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if not row or not row[1]:
                        continue
                    
                    endereco = str(row[1]).strip().replace("\n", " ")
                    bairro = str(row[4]).strip().replace("\n", " ") if row[4] else ""
                    dia = str(row[7]).strip().replace("\n", " ") if row[7] else ""
                    horario = str(row[10]).strip().replace("\n", " ") if row[10] else ""
                    ra = str(row[13]).strip().replace("\n", " ") if row[13] else ""
                    
                    # Pula cabeçalho
                    if not bairro or "BAIRRO" in bairro.upper() or not dia:
                        continue
                    
                    endereco_norm = normalizar_endereco(endereco)
                    
                    feiras.append({
                        "endereco_raw": endereco,
                        "endereco_normalizado": endereco_norm,
                        "bairro": bairro.title(),
                        "dia": dia,
                        "horario": horario,
                        "ra": ra,
                    })
    
    return feiras


# ==============================================================================
#  GEOCODIFICAÇÃO VIA NOMINATIM
# ==============================================================================

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
HEADERS = {
    "User-Agent": "ScraperImoveisRio_FeirasGeocoder_v1.0",
    "Referer": "https://google.com",
}


def geocode_nominatim(endereco: str, bairro: str, tentativa: int = 0) -> dict:
    """
    Geocodifica endereço via Nominatim com hierarquia:
    1. Rua + Bairro + Rio de Janeiro
    2. Só Bairro + Rio de Janeiro (fallback)
    
    Returns:
        {"lat": float, "lon": float, "display_name": str, "nivel": str}
        ou None se falhar.
    """
    queries = [
        (f"{endereco}, {bairro}, Rio de Janeiro, RJ, Brasil", "Rua"),
        (f"{bairro}, Rio de Janeiro, RJ, Brasil", "Bairro"),
    ]
    
    for query, nivel in queries:
        try:
            params = {
                "q": query,
                "format": "json",
                "limit": 1,
                "countrycodes": "br",
            }
            
            response = requests.get(
                NOMINATIM_URL, params=params, headers=HEADERS, timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data:
                    return {
                        "lat": float(data[0]["lat"]),
                        "lon": float(data[0]["lon"]),
                        "display_name": data[0].get("display_name", ""),
                        "nivel": nivel,
                        "query_usada": query,
                    }
            
            # Rate limiting: Nominatim exige max 1 req/s
            time.sleep(1.1)
            
        except Exception as e:
            print(f"  ⚠ Erro na query '{query}': {e}")
            time.sleep(2)
    
    return None


# ==============================================================================
#  MAIN
# ==============================================================================

def main():
    if len(sys.argv) < 2:
        print("Uso: python geocode_feiras.py RELACAO_FEIRAS2.pdf")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    if not Path(pdf_path).exists():
        print(f"ERRO: arquivo não encontrado: {pdf_path}")
        sys.exit(1)
    
    # 1. Extrai feiras do PDF
    print(f"📄 Extraindo feiras de {pdf_path}...")
    feiras = extrair_feiras_pdf(pdf_path)
    print(f"   ✓ {len(feiras)} feiras encontradas")
    
    # 2. Geocodifica cada uma
    print(f"\n🌍 Geocodificando via Nominatim (1 req/s, ~{len(feiras) * 1.5:.0f}s estimado)...")
    print(f"   Ctrl+C para interromper (progresso é salvo)\n")
    
    resultados = []
    falhas = []
    
    try:
        for i, feira in enumerate(feiras):
            endereco = feira["endereco_normalizado"]
            bairro = feira["bairro"]
            dia_curto = feira["dia"].replace("-Feira", "").strip()
            
            # Nome para o config.py
            nome_config = f"{endereco} - {bairro}"
            
            print(f"  [{i+1:3d}/{len(feiras)}] {nome_config:60s}", end="", flush=True)
            
            geo = geocode_nominatim(endereco, bairro)
            
            if geo:
                feira["lat"] = geo["lat"]
                feira["lon"] = geo["lon"]
                feira["display_name"] = geo["display_name"]
                feira["nivel_geocode"] = geo["nivel"]
                feira["nome_config"] = nome_config
                resultados.append(feira)
                print(f" ✓ ({geo['nivel']}) {geo['lat']:.4f}, {geo['lon']:.4f}")
            else:
                feira["nome_config"] = nome_config
                falhas.append(feira)
                print(f" ✗ NÃO ENCONTRADO")
            
            # Rate limit
            time.sleep(1.1)
    
    except KeyboardInterrupt:
        print("\n\n⚠ Interrompido pelo usuário. Salvando progresso...")
    
    # 3. Salva resultados
    print(f"\n{'='*60}")
    print(f"  Geocodificados: {len(resultados)}/{len(feiras)}")
    print(f"  Falhas:         {len(falhas)}")
    print(f"{'='*60}")
    
    # JSON completo
    with open("feiras_geocodificadas.json", "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)
    print(f"\n✓ Salvo: feiras_geocodificadas.json")
    
    # Snippet para config.py
    with open("feiras_config_snippet.py", "w", encoding="utf-8") as f:
        f.write('    "feiras_alimentacao": {\n')
        f.write('        # Fonte: Prefeitura do RJ - Divisão de Feiras\n')
        f.write(f'        # Geocodificado via Nominatim ({len(resultados)} feiras)\n')
        for r in resultados:
            nome = r["nome_config"]
            dia_curto = r["dia"].replace("-Feira", "").strip()
            chave = f"Feira {r['bairro']} ({dia_curto}/{r['endereco_normalizado']})"
            f.write(f'        "{chave}": ({r["lat"]}, {r["lon"]}),\n')
        f.write('    },\n')
    print(f"✓ Salvo: feiras_config_snippet.py (copiar para config.py)")
    
    # Falhas
    if falhas:
        with open("feiras_falhas.csv", "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=["endereco_raw", "endereco_normalizado", "bairro", "dia"])
            writer.writeheader()
            for fa in falhas:
                writer.writerow({
                    "endereco_raw": fa["endereco_raw"],
                    "endereco_normalizado": fa["endereco_normalizado"],
                    "bairro": fa["bairro"],
                    "dia": fa["dia"],
                })
        print(f"✓ Salvo: feiras_falhas.csv (endereços para revisar manualmente)")


if __name__ == "__main__":
    main()
