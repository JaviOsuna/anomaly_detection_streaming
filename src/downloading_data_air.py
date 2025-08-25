import os
import requests
import pandas as pd
from datetime import datetime

# Configuración
DATA_DIR = "data/air_quality"
AIR_URL = "https://datos.madrid.es/egob/catalogo/212531-10515086-calidad-aire-tiempo-real.csv"


def ensure_dir(path):
    """Crea el directorio si no existe"""
    if not os.path.exists(path):
        os.makedirs(path)

def load_estaciones_coords():
    """Carga las coordenadas de las estaciones desde un CSV local"""
    coords_file = "data/informacion_estaciones_red_calidad_aire.csv"
    if not os.path.exists(coords_file):
        print(f"✗ No se encuentra el archivo de coordenadas: {coords_file}")
        return None
    try:
        df_coords = pd.read_csv(coords_file, sep=";")
        return df_coords[["CODIGO_CORTO", "st_x", "st_y"]]
    except Exception as e:
        print(f"✗ Error procesando coordenadas: {e}")
        return None

def preprocess_air_quality(raw_file, processed_file, df_coords):
    """Preprocesa los datos de calidad del aire"""
    try:
        # Cargar datos crudos
        df = pd.read_csv(raw_file, sep=";")

        # Filtrar solo NO2 (MAGNITUD = 8)
        df_no2 = df[df["MAGNITUD"] == 8].copy()

        if df_no2.empty:
            print("No hay datos de NO2 en el archivo")
            return False
        
        # Añadir coordenadas
        df_no2 = df_no2.merge(df_coords, left_on="ESTACION", right_on="CODIGO_CORTO", how="left")
        df_no2 = df_no2.dropna(subset=["st_x", "st_y"])
        
        if df_no2.empty:
            print(" No hay estaciones con coordenadas válidas")
            return False
        
        # Convertir a formato largo (una fila por hora)
        horas = [f"H{str(i).zfill(2)}" for i in range(1, 25)]
        df_long = df_no2.melt(
            id_vars=["ESTACION", "ANO", "MES", "DIA", "st_x", "st_y"],
            value_vars=horas,
            var_name="HORA",
            value_name="NO2"
        )
        
        # Crear fecha_hora
        df_long["HORA_NUM"] = df_long["HORA"].str.extract(r"(\d+)").astype(int)
        df_long["fecha_hora"] = pd.to_datetime(
            df_long["ANO"].astype(str) + "-" +
            df_long["MES"].astype(str).str.zfill(2) + "-" +
            df_long["DIA"].astype(str).str.zfill(2) + " " +
            (df_long["HORA_NUM"] - 1).astype(str).str.zfill(2) + ":00:00"
        )

        ahora = datetime.now()
        df_long = df_long[df_long["fecha_hora"] <= ahora]
        
        # Limpiar valores negativos (ausencia de datos)
        df_long = df_long[df_long["NO2"] >= 0]
        
        # Seleccionar solo las columnas necesarias
        df_final = df_long[["ESTACION", "fecha_hora", "st_x", "st_y", "NO2"]]
        
        # Guardar procesado
        df_final.to_csv(processed_file, index=False)
        print(f" Procesado y guardado: {processed_file} ({len(df_final)} registros)")
        return True
        
    except Exception as e:
        print(f" Error procesando calidad del aire: {e}")
        return False

def download_air_quality():
    """Descarga y preprocesa los datos de calidad del aire"""
    ensure_dir(DATA_DIR)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_filename = f"{DATA_DIR}/raw_air_{timestamp}.csv"
    processed_filename = f"{DATA_DIR}/air_quality_{timestamp}.csv"
    
    # Descargar coordenadas si es necesario
    df_coords = load_estaciones_coords()
    if df_coords is None:
        print(" No se pudieron obtener las coordenadas de las estaciones")
        return False
    
    try:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Descargando calidad del aire...")
        response = requests.get(AIR_URL, timeout=30)
        response.raise_for_status()
        
        with open(raw_filename, "wb") as f:
            f.write(response.content)
        
        print(f" Descargado: {raw_filename} ({os.path.getsize(raw_filename)} bytes)")
        
        # Preprocesar
        if preprocess_air_quality(raw_filename, processed_filename, df_coords):
            # Eliminar archivo crudo
            os.remove(raw_filename)
            print(f" Eliminado archivo crudo: {raw_filename}")
            return True
        else:
            print(f" No se eliminó el archivo crudo por error en procesamiento")
            return False
        
    except requests.exceptions.RequestException as e:
        print(f" Error de conexión: {e}")
        return False
    except Exception as e:
        print(f" Error inesperado: {e}")
        return False