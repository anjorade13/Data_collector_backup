import os
import requests
import pandas as pd
import time
from datetime import datetime
from urllib.parse import quote

# Configuración segura desde secretos
TOKEN = os.getenv("API_TOKEN")
BASE_URL = os.getenv("API_BASE_URL")
HEADERS = {"token": TOKEN}

MAX_RETRIES = 0
REQUEST_DELAY = 15
RETRY_DELAY = 10

# ENDPOINTS - Verificar si realmente son todos iguales
ENDPOINTS = {
    "Consulta_1": "System.MaterialTransactions.List.View1",
    "Consulta_2": "System.MaterialTransactions.List.View1",
    "Consulta_3": "System.MaterialTransactions.List.View1",
    "Consulta_4": "System.InventoryItems.List.View4"
}

# Configuración de las consultas corregidas
QUERY_CONFIG = [
    {
        "name": "Consulta_1",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "ctxn_movement_type ilike '261%%' and (ctxn_transaction_date > current_date - 120) and (ctxn_warehouse_code ilike '1145') and (ctxn_primary_uom_code ilike 'Und')"  # Corregido el formato del LIKE
        }
    },
    {
        "name": "Consulta_2",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "ctxn_movement_type ilike '261%%' and (ctxn_transaction_date > current_date - 120) and ctxn_warehouse_code ilike '1145' and not (ctxn_primary_uom_code ilike 'Und')"
        }
    },
    {
        "name": "Consulta_3",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "(ctxn_movement_type ilike '261%%') and (ctxn_transaction_date > current_date - 120) and (ctxn_warehouse_code ilike '1290')"  # Paréntesis añadidos para claridad
        }
    },
        {
        "name": "Consulta_4",
        "params": {
            "take": "20000"
        }
    }
]

def build_url(endpoint, params):
    param_parts = []
    for key, value in params.items():
        # Codificar valores para URL
        encoded_value = quote(str(value))
        param_parts.append(f"{key}={encoded_value}")
    url = f"{BASE_URL}{endpoint}?{'&'.join(param_parts)}"
    return url

def fetch_data(url, name):
    print(f"\n🔗 URL generada para {name}:\n{url}\n")
    for attempt in range(MAX_RETRIES + 1):
        try:
            print(f"🔎 Consultando {name} (Intento {attempt + 1}/{MAX_RETRIES + 1})")
            response = requests.get(url, headers=HEADERS, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            # Mejor manejo de respuestas vacías o inválidas
            if not data:
                print(f"⚠️  {name} no devolvió datos (JSON vacío).")
                return None
                
            if isinstance(data, dict) and data.get("error"):
                print(f"⚠️  Error en {name}: {data.get('error')}")
                return None
                
            df = pd.json_normalize(data)
            df["load_timestamp"] = datetime.now().isoformat()
            return df
        except requests.exceptions.RequestException as e:
            print(f"⚠️  Error en {name}: {e}")
            if attempt < MAX_RETRIES:
                print(f"⏳ Reintentando en {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"❌ La consulta {name} fracasó definitivamente.")
                return None
        except ValueError as e:
            print(f"⚠️  Error al decodificar JSON en {name}: {e}")
            return None

def save_data(df, name):
    os.makedirs("data", exist_ok=True)
    path = f"data/{name}.json"
    df.to_json(path, orient="records", indent=2)
    print(f"💾 Guardado: {path} - {len(df)} registros")

def main():
    print("🚀 Iniciando consultas para Power BI")
    start_time = time.time()

    for query in QUERY_CONFIG:
        name = query["name"]
        url = build_url(ENDPOINTS[name], query["params"])
        print(f"\n🔍 Ejecutando consulta: {name}")
        print(f"📋 Parámetros: {query['params']}")
        df = fetch_data(url, name)
        if df is not None:
            print(f"📊 Datos obtenidos: {len(df)} registros")
            save_data(df, name)
        else:
            print(f"❌ No se obtuvieron datos para {name}")
        time.sleep(REQUEST_DELAY)

    duration = time.time() - start_time
    print(f"✅ Proceso finalizado en {duration:.2f} segundos.")

if __name__ == "__main__":
    main()
