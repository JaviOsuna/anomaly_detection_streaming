import os
import time
import requests
from datetime import datetime
import xml.etree.ElementTree as ET
import csv

# Configuración
DATA_DIR = "data/traffic"
TRAFFIC_URL = "https://datos.madrid.es/egob/catalogo/202087-0-trafico-intensidad.xml"
DOWNLOAD_FREQ = 5 * 60  # cada 5 minutos

def ensure_dir(path):
    """Crea el directorio si no existe"""
    if not os.path.exists(path):
        os.makedirs(path)

def download_traffic():
    """Descarga los datos de tráfico"""
    ensure_dir(DATA_DIR)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fecha_hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    xml_filename = f"{DATA_DIR}/traffic_{timestamp}.xml"
    csv_filename = f"{DATA_DIR}/traffic_{timestamp}.csv"
    
    try:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Descargando datos de tráfico...")
        response = requests.get(TRAFFIC_URL, timeout=30)
        response.raise_for_status()
        
        with open(xml_filename, "wb") as f:
            f.write(response.content)
        print(f"✓ Descargado: {xml_filename} ({os.path.getsize(xml_filename)} bytes)")

        # Convertir a CSV
        conversion_exitosa = convert_xml_to_csv(xml_filename, csv_filename, fecha_hora)
        if conversion_exitosa:
            os.remove(xml_filename)
            print(f"✓ Eliminado archivo XML: {xml_filename}")
        else:
            print(f"✗ No se eliminó el XML porque la conversión falló.")
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error de conexión: {e}")
    except Exception as e:
        print(f"✗ Error inesperado: {e}")

def convert_xml_to_csv(xml_path, csv_path, fecha_hora):
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        puntos = root.findall(".//pm")  # Ajusta si la estructura cambia

        if not puntos:
            print("✗ No se encontraron nodos <pm> en el XML.")
            return False

        campos = [elem.tag for elem in puntos[0]]
        campos = ["fecha_hora"] + campos

        with open(csv_path, "w", newline='', encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(campos)
            for punto in puntos:
                row = [fecha_hora]
                row += [punto.find(campo).text if punto.find(campo) is not None else "" for campo in campos]
                writer.writerow(row)
        print(f"✓ Convertido a CSV: {csv_path}")
        return True

    except Exception as e:
        print(f"✗ Error al convertir XML a CSV: {e}")
        return False

def main():
    """Función principal - descarga periódica"""
    print("=== DESCARGA PERIÓDICA DE DATOS DE TRÁFICO ===")
    print(f"URL: {TRAFFIC_URL}")
    print(f"Frecuencia: cada {DOWNLOAD_FREQ//60} minutos")
    print(f"Carpeta destino: {DATA_DIR}")
    print("Pulsa Ctrl+C para parar.\n")
    
    try:
        while True:
            download_traffic()
            print(f"Esperando {DOWNLOAD_FREQ//60} minutos hasta la próxima descarga...\n")
            time.sleep(DOWNLOAD_FREQ)
            
    except KeyboardInterrupt:
        print("\n🛑 Descarga detenida por el usuario.")
    except Exception as e:
        print(f"\n💥 Error crítico: {e}")

if __name__ == "__main__":
    main()