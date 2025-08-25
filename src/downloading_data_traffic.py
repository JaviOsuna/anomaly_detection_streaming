import os
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

# Configuración
DATA_DIR = "data/traffic"
TRAFFIC_URL = "https://datos.madrid.es/egob/catalogo/202087-0-trafico-intensidad.xml"

def ensure_dir(path):
    """Crea el directorio si no existe"""
    if not os.path.exists(path):
        os.makedirs(path)

def preprocess_traffic_xml(xml_path, csv_path, fecha_hora_descarga):

    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        puntos = root.findall(".//pm")

        if not puntos:
            print("✗ No se encontraron nodos <pm> en el XML.")
            return False

        data = []
        for punto in puntos:
            row = {"fecha_hora_descarga": fecha_hora_descarga}
            for elem in punto:
                text = elem.text.strip() if elem.text else ""
                if elem.tag in ["st_x", "st_y"]:
                    text = text.replace(",", ".")
                row[elem.tag] = text
            data.append(row)

        df = pd.DataFrame(data)

        # Convertir columnas numéricas
        numeric_cols = ["intensidad", "ocupacion", "carga", "nivelServicio", "intensidadSat", "st_x", "st_y"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                if col != "nivelServicio":
                    df[col] = df[col].where(df[col] >= 0)

        # Convertir fecha_hora_descarga a datetime
        df["fecha_hora_descarga"] = pd.to_datetime(df["fecha_hora_descarga"])

        # Crear columna fecha_hora_trunc truncando a la hora
        df["fecha_hora_trunc"] = df["fecha_hora_descarga"].dt.floor("h")

        # Eliminar filas sin coordenadas válidas
        df = df.dropna(subset=["st_x", "st_y"])

        if df.empty:
            print("✗ No hay datos válidos después del preprocesado")
            return False

        # Agrupar por estación (idelem) y hora truncada
        df_agg = df.groupby(["idelem", "fecha_hora_trunc"]).agg({
            "st_x": "first",
            "st_y": "first",
            "intensidad": "mean",
            "ocupacion": "mean",
            "carga": "mean",
            "nivelServicio": "max",
            "intensidadSat": "mean"
        }).reset_index()

        # Guardar CSV procesado
        df_agg.to_csv(csv_path, index=False)
        print(f"✓ Procesado y guardado: {csv_path} ({len(df_agg)} registros)")
        return True

    except Exception as e:
        print(f"✗ Error al procesar XML de tráfico: {e}")
        return False

def download_traffic():
    """Descarga y preprocesa los datos de tráfico"""
    ensure_dir(DATA_DIR)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fecha_hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    xml_filename = f"{DATA_DIR}/raw_traffic_{timestamp}.xml"
    csv_filename = f"{DATA_DIR}/traffic_{timestamp}.csv"
    
    try:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Descargando datos de tráfico...")
        response = requests.get(TRAFFIC_URL, timeout=30)
        response.raise_for_status()
        
        with open(xml_filename, "wb") as f:
            f.write(response.content)
        print(f" Descargado: {xml_filename} ({os.path.getsize(xml_filename)} bytes)")

        # Preprocesar
        if preprocess_traffic_xml(xml_filename, csv_filename, fecha_hora):
            # Eliminar archivo XML crudo
            os.remove(xml_filename)
            print(f" Eliminado archivo XML: {xml_filename}")
            return True
        else:
            print(f" No se eliminó el XML por error en procesamiento")
            return False
        
    except requests.exceptions.RequestException as e:
        print(f" Error de conexión: {e}")
        return False
    except Exception as e:
        print(f" Error inesperado: {e}")
        return False