import os
import time
import requests
from datetime import datetime

# Configuración
DATA_DIR = "data/air_quality"
AIR_URL = "https://datos.madrid.es/egob/catalogo/212531-10515086-calidad-aire-tiempo-real.csv"
DOWNLOAD_FREQ = 20 * 60  # cada 20 minutos (según la web: minutos 15, 35, 55)

def ensure_dir(path):
    """Crea el directorio si no existe"""
    if not os.path.exists(path):
        os.makedirs(path)

def download_air_quality():
    """Descarga los datos de calidad del aire"""
    ensure_dir(DATA_DIR)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{DATA_DIR}/air_quality_{timestamp}.csv"
    
    try:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Descargando calidad del aire...")
        response = requests.get(AIR_URL, timeout=30)
        response.raise_for_status()
        
        with open(filename, "wb") as f:
            f.write(response.content)
        
        file_size = os.path.getsize(filename)
        print(f"✓ Descargado: {filename} ({file_size} bytes)")
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error de conexión: {e}")
    except Exception as e:
        print(f"✗ Error inesperado: {e}")

def main():
    """Función principal - descarga periódica"""
    print("=== DESCARGA PERIÓDICA DE CALIDAD DEL AIRE ===")
    print(f"URL: {AIR_URL}")
    print(f"Frecuencia: cada {DOWNLOAD_FREQ//60} minutos")
    print(f"Carpeta destino: {DATA_DIR}")
    print("Pulsa Ctrl+C para parar.\n")
    
    try:
        while True:
            download_air_quality()
            print(f"Esperando {DOWNLOAD_FREQ//60} minutos hasta la próxima descarga...\n")
            time.sleep(DOWNLOAD_FREQ)
            
    except KeyboardInterrupt:
        print("\n🛑 Descarga detenida por el usuario.")
    except Exception as e:
        print(f"\n💥 Error crítico: {e}")

if __name__ == "__main__":
    main()