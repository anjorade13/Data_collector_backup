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
    "Transacciones de materiales": "System.MaterialTransactions.List.View1",
    "Conciliacion de inventario": "Ardisa.InventoryReconciliation.List.View2",
    "Inventario de materiales": "System.InventoryItems.List.View4",
    "Entregas de salida": "System.OutboundDeliveries.List.View1",
    "Salida de mercancia": "System.GoodsIssues.List.View1",
    "Entradas de mercancia": "System.GoodsRecipts.List.View1",
    "Envios entrantes": "Ardisa.InboundDeliveries.List.View1",
    "Documentos OV/FR/ST": "Ardisa.SalesOrders.List.View1",
    "Tareas": "System.Tasks.List.View3",
    "Inventario Ciclico": "System.StockCountingItemVars.List.View1"
}

# Configuración de las consultas corregidas
QUERY_CONFIG = [
    {
        "name": "Transacciones de materiales",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "10000",
            "where": "ctxn_transaction_date > current_date - 2"
        }
    },
    {
        "name": "Conciliacion de inventario",
        "params": {
            "orderby": "snap_date desc",
            "take": "10000",
            "where": "snap_date > current_date - 2"
        }
    },
        {
        "name": "Inventario de materiales",
        "params": {
            "take": "20000"
        }
    },
        {
        "name": "Entregas de salida",
        "params": {
            "orderby": "codv_created_on desc",
            "take": "1000",
            "where": "codv_created_on > current_date - 1"
        }
    },
        {
        "name": "Salida de mercancia",
        "params": {
            "orderby": "cgis_created_on desc",
            "take": "1000",
            "where": "cgis_created_on > current_date - 1"
        }
    },
        {
        "name": "Entradas de mercancia",
        "params": {
            "orderby": "cgre_created_on desc",
            "take": "1000",
            "where": "cgre_created_on > current_date - 1"
        }
    },
        {
        "name": "Envios entrantes",
        "params": {
            "orderby": "cdoc_date desc",
            "take": "1000",
            "where": "cdoc_date > current_date - 2"
        }
    },
        {
        "name": "Documentos OV/FR/ST",
        "params": {
            "orderby": "cslo_created_on desc",
            "take": "1000",
            "where": "cslo_created_on > current_date - 2"
        }
    },
        {
        "name": "Tareas",
        "params": {
            "orderby": "ctsk_created_on desc",
            "take": "5000",
            "where": "ctsk_created_on > current_date - 1"
        }
    },
        {
        "name": "Inventario Ciclico",
        "params": {
            "orderby": "DocDate desc",
            "take": "5000",
            "where": "DocDate > current_date - 2 and ItemClosed ilike 'SI'"
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
