import os
import requests
import pandas as pd
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
import re

# Configuración segura desde secretos
TOKEN = os.getenv("API_TOKEN")
BASE_URL = os.getenv("API_BASE_URL")
HEADERS = {"token": TOKEN}

MAX_RETRIES = 0
REQUEST_DELAY = 15
RETRY_DELAY = 10

# ENDPOINTS
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

# Configuración de las consultas
QUERY_CONFIG = [
    {
        "name": "Transacciones de materiales",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "10000",
            "where": "ctxn_transaction_date = current_date - 1"
        }
    },
    {
        "name": "Conciliacion de inventario",
        "params": {
            "orderby": "snap_date desc",
            "take": "10000",
            "where": "snap_date = current_date - 1"
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
            "where": "(codv_created_on > current_date - 1) and (codv_created_on < current_date)"
        }
    },
    {
        "name": "Salida de mercancia",
        "params": {
            "orderby": "cgis_created_on desc",
            "take": "1000",
            "where": "(cgis_created_on > current_date - 1) and (cgis_created_on < current_date)"
        }
    },
    {
        "name": "Entradas de mercancia",
        "params": {
            "orderby": "cgre_created_on desc",
            "take": "1000",
            "where": "(cgre_created_on > current_date - 1) and (cgre_created_on < current_date)"
        }
    },
    {
        "name": "Envios entrantes",
        "params": {
            "orderby": "cdoc_date desc",
            "take": "1000",
            "where": "cdoc_date = current_date - 1"
        }
    },
    {
        "name": "Documentos OV/FR/ST",
        "params": {
            "orderby": "cslo_created_on desc",
            "take": "1000",
            "where": "(cslo_created_on > current_date - 1) and (cslo_created_on< current_date)"
        }
    },
    {
        "name": "Tareas",
        "params": {
            "orderby": "ctsk_created_on desc",
            "take": "5000",
            "where": "(ctsk_created_on > current_date - 1) and (ctsk_created_on < current_date)"
        }
    },
    {
        "name": "Inventario Ciclico",
        "params": {
            "orderby": "DocDate desc",
            "take": "5000",
            "where": "DocDate = current_date - 1 and ItemClosed ilike 'SI'"
        }
    }
]

def get_colombia_time():
    """Obtiene la fecha y hora actual en zona horaria de Colombia (UTC-5)"""
    return datetime.now(timezone(timedelta(hours=-5)))

def build_url(endpoint, params):
    param_parts = []
    for key, value in params.items():
        encoded_value = quote(str(value))
        param_parts.append(f"{key}={encoded_value}")
    url = f"{BASE_URL}{endpoint}?{'&'.join(param_parts)}"
    return url

def sanitize_filename(name):
    """Convierte un nombre a formato seguro para nombres de archivo"""
    return re.sub(r'[\\/*?:"<>|]', "_", name)

def clean_string(value):
    """Limpia strings reemplazando saltos de línea y retornos de carro"""
    if isinstance(value, str):
        # Reemplazar saltos de línea y retornos de carro con espacios
        value = value.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
        # Eliminar espacios múltiples consecutivos
        value = re.sub(r'\s+', ' ', value).strip()
    return value

def clean_dataframe(df):
    """Limpia todos los campos string del DataFrame"""
    for column in df.columns:
        if df[column].dtype == 'object':  # Columnas que contienen strings
            df[column] = df[column].apply(clean_string)
    return df

def fetch_data(url, name):
    print(f"\n🔗 URL generada para {name}:\n{url}\n")
    for attempt in range(MAX_RETRIES + 1):
        try:
            print(f"🔎 Consultando {name} (Intento {attempt + 1}/{MAX_RETRIES + 1})")
            response = requests.get(url, headers=HEADERS, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                print(f"⚠️  {name} no devolvió datos (JSON vacío).")
                return None
                
            if isinstance(data, dict) and data.get("error"):
                print(f"⚠️  Error en {name}: {data.get('error')}")
                return None
            
            # Extraer solo el array "message" de la respuesta
            if isinstance(data, dict) and "message" in data:
                message_data = data["message"]
                if not message_data:
                    print(f"⚠️  {name} no tiene datos en el array 'message'.")
                    return None
                df = pd.DataFrame(message_data)
            else:
                print(f"⚠️  La respuesta de {name} no contiene el array 'message'.")
                return None
                
            # Limpiar los datos (remover saltos de línea)
            df = clean_dataframe(df)
                
            # Agregar metadata con fecha y hora colombiana
            colombia_time = get_colombia_time()
            df["metadata_fecha_consulta"] = colombia_time.strftime("%Y-%m-%d %H:%M:%S")
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
        except KeyError as e:
            print(f"⚠️  Error al procesar la estructura JSON en {name}: {e}")
            return None

def save_data(df, query_name):
    os.makedirs("data", exist_ok=True)
    
    # Obtener fecha y hora colombiana para el nombre del archivo
    colombia_time = get_colombia_time()
    timestamp = colombia_time.strftime("%Y%m%d_%H%M%S")
    
    # Crear nombre de archivo seguro usando el nombre de la consulta
    safe_query_name = sanitize_filename(query_name)
    filename = f"{safe_query_name}_{timestamp}.csv"
    path = os.path.join("data", filename)
    
    # Guardar como CSV con encoding UTF-8
    df.to_csv(path, index=False, encoding='utf-8')
    print(f"💾 Guardado: {path} - {len(df)} registros")
    
    return path

def main():
    print("🚀 Iniciando consultas para Power BI")
    start_time = time.time()

    for query in QUERY_CONFIG:
        name = query["name"]
        endpoint = ENDPOINTS[name]
        url = build_url(endpoint, query["params"])
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
