from downloading_data_air import download_air_quality
from downloading_data_traffic import download_traffic
from fusion_data import fusionar_y_eliminar
import time
from datetime import datetime

def main():
    print("=== PIPELINE DE DATOS TRÁFICO + CALIDAD DEL AIRE ===")
    print("Ctrl+C para parar.\n")
    
    try:
        while True:
            start_time = time.time()
            print(f"\n{'='*50}")
            print(f"CICLO INICIADO: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*50}")
        
            
            print("\n--- DESCARGA TRÁFICO (12 veces) ---")
            traffic_successes = 0
            for i in range(12):
                print(f"\nDescarga de tráfico {i+1}/12:")
                if download_traffic():
                    traffic_successes += 1
                
                if i < 11:
                    print(f"Esperando 5 minutos hasta la siguiente descarga de tráfico...")
                    time.sleep(5 * 60)

            time.sleep(60)

            print("\n--- DESCARGA CALIDAD DEL AIRE ---")
            air_success = download_air_quality()
            
            print(f"\n--- FUSIÓN DE DATOS ---")
            if air_success and traffic_successes >= 12:
                print("Intentando fusionar datos...")
                if fusionar_y_eliminar():
                    print(" Fusión completada exitosamente")
                else:
                    print(" Error en la fusión")
            else:
                print(f" No se puede fusionar: aire={air_success}, tráfico={traffic_successes}/12")

            print(f"\n{'='*50}")
            print("CICLO COMPLETADO. Esperando hasta el siguiente ciclo de 60 minutos...")
            print(f"{'='*50}")

            elapsed = time.time() - start_time
            sleep_time = max(0, 3600 - elapsed)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n🛑 Pipeline detenido por el usuario.")
    except Exception as e:
        print(f"\n💥 Error crítico en el pipeline: {e}")

if __name__ == "__main__":
    main()