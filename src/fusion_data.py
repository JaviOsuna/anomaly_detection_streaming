import pandas as pd
from scipy.spatial import cKDTree
import glob
import os
from datetime import datetime

def horas_validas_por_no2(df_aire_filtrado, umbral_min_no2_count=5):
    """
    Devuelve las horas donde hay suficiente NO2 > 0 para considerar la hora 'consolidada'.
    umbral_min_no2_count: nº mínimo de estaciones con NO2 > 0.
    """
    g = df_aire_filtrado.groupby("fecha_hora")["NO2"]
    stats = g.agg(count="count",
                  nonzero=lambda s: (s > 0).sum())
    # criterio: al menos X valores > 0
    mask = stats["nonzero"] >= umbral_min_no2_count
    return set(stats.index[mask])

def join_espacial_temporal(df_trafico, df_aire):
    try:
        # Convertir coordenadas a float y corregir coma decimal
        for df in [df_trafico, df_aire]:
            df["st_x"] = df["st_x"].astype(str).str.replace(",", ".").astype(float)
            df["st_y"] = df["st_y"].astype(str).str.replace(",", ".").astype(float)

        # Crear árbol KDTree con estaciones de aire
        estaciones_coords = df_aire[["st_x", "st_y"]].drop_duplicates().values
        estaciones_info = df_aire[["ESTACION", "st_x", "st_y"]].drop_duplicates()
        tree = cKDTree(estaciones_coords)

        # Encontrar estación más cercana para cada punto de tráfico
        trafico_coords = df_trafico[["st_x", "st_y"]].values
        distancias, indices = tree.query(trafico_coords)

        df_trafico = df_trafico.copy()
        df_trafico["estacion_cercana"] = estaciones_info.iloc[indices]["ESTACION"].values
        df_trafico["distancia_estacion"] = distancias

        # Agregar datos de tráfico por estación y hora truncada
        df_trafico_agg = df_trafico.groupby(["estacion_cercana", "fecha_hora_trunc"]).agg({
            "intensidad": "mean",
            "ocupacion": "mean",
            "carga": "mean",
            "nivelServicio": "max",
            "intensidadSat": "mean",
            "distancia_estacion": "mean",
            "idelem": "count"
        }).reset_index().rename(columns={"idelem": "num_puntos_trafico"})

        # Merge con datos de calidad del aire por estación y hora
        df_fusion = df_aire.merge(
            df_trafico_agg,
            left_on=["ESTACION", "fecha_hora"],
            right_on=["estacion_cercana", "fecha_hora_trunc"],
            how="inner"
        )

        columnas_finales = [
            "ESTACION", "fecha_hora", "st_x", "st_y", "NO2",
            "intensidad", "ocupacion", "carga", "nivelServicio", "intensidadSat",
            "distancia_estacion", "num_puntos_trafico"
        ]

        df_fusion = df_fusion[columnas_finales]
        return df_fusion

    except Exception as e:
        print(f"✗ Error en join espacial/temporal: {e}")
        return pd.DataFrame()

def fusionar_y_eliminar():
    """Fusiona datos de aire con los CSV de tráfico y elimina archivos usados"""
    air_files = sorted(glob.glob("data/air_quality/air_quality_*.csv"))
    
    if not air_files:
        print("No hay archivos de calidad de aire para fusionar.")
        return False
    
    fusion_exitosa = False
    
    for air_file in air_files:
        try:
            # Extraer timestamp del archivo de aire
            timestamp = air_file.split("_")[-1].replace(".csv", "")
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Procesando fusión para {timestamp}")
            
            # Buscar archivos de tráfico
            traffic_files = sorted(glob.glob("data/traffic/traffic_*.csv"))
            if len(traffic_files) < 1:
                print(" No hay archivos de tráfico para fusionar")
                continue
            
            # Cargar datos de aire
            df_aire = pd.read_csv(air_file)
            df_aire["fecha_hora"] = pd.to_datetime(df_aire["fecha_hora"])
            
            # Asegurar nombre y tipo de NO2
            cols_lower = {c: c.strip() for c in df_aire.columns}
            df_aire.rename(columns=cols_lower, inplace=True)

            if "NO2" not in df_aire.columns:
                posibles = [c for c in df_aire.columns if c.lower() in ["no2", "no_2", "nitrogeno_dioxido", "nitrogeno_dióxido"]]
                if posibles:
                    df_aire.rename(columns={posibles[0]: "NO2"}, inplace=True)
                else:
                    raise ValueError(f"No encuentro columna NO2 en {air_file}. Columnas: {df_aire.columns.tolist()}")

            # Convertir NO2 a float con seguridad
            df_aire["NO2"] = (
                df_aire["NO2"]
                .astype(str)
                .str.replace(",", ".", regex=False)
                .str.replace(r"[^\d\.\-]", "", regex=True)
            )
            df_aire["NO2"] = pd.to_numeric(df_aire["NO2"], errors="coerce")
            
            # Cargar y concatenar datos de tráfico
            dfs_trafico = []
            for traffic_file in traffic_files:
                df_t = pd.read_csv(traffic_file)
                df_t["fecha_hora_trunc"] = pd.to_datetime(df_t["fecha_hora_trunc"])
                
                if "NO2" in df_t.columns:
                    print("[WARN] Tráfico trae columna NO2 inesperada; se renombra para evitar colisiones.")
                    df_t = df_t.rename(columns={"NO2": "NO2_trafico_invalido"})
                
                dfs_trafico.append(df_t)
            df_trafico = pd.concat(dfs_trafico, ignore_index=True)
            
            # Filtrar horas comunes
            horas_aire = set(df_aire["fecha_hora"].unique())
            horas_trafico = set(df_trafico["fecha_hora_trunc"].unique())
            print(f" Horas aire (min..max): {min(horas_aire) if horas_aire else None} .. {max(horas_aire) if horas_aire else None}")
            print(f" Horas tráfico (min..max): {min(horas_trafico) if horas_trafico else None} .. {max(horas_trafico) if horas_trafico else None}")
            horas_comunes = horas_aire.intersection(horas_trafico)
            if not horas_comunes:
                print(" No hay horas comunes entre aire y tráfico para fusionar.")
                continue

            # Diagnóstico de NO2 por hora en AIRE solo para horas comunes
            aire_comun = df_aire[df_aire["fecha_hora"].isin(horas_comunes)].copy()
            diag = aire_comun.groupby("fecha_hora")["NO2"].agg(
                count="count",
                nonzero=lambda s: (s > 0).sum(),
                zeros=lambda s: (s == 0).sum()
            ).sort_index()
            print(" Diagnóstico NO2 por hora (aire, horas comunes):")
            print(diag.tail(8))
            
            # Filtrar por horas válidas (con NO2 > 0)
            horas_validas = horas_validas_por_no2(df_aire, umbral_min_no2_count=5)
            horas_a_fusionar = horas_comunes.intersection(horas_validas)
            if not horas_a_fusionar:
                print(" No hay horas comunes con NO2 consolidado. Conservando archivos para próxima ejecución.")
                continue

            # Mantener solo horas a fusionar
            df_aire_filtrado = df_aire[df_aire["fecha_hora"].isin(horas_a_fusionar)]
            df_trafico_filtrado = df_trafico[df_trafico["fecha_hora_trunc"].isin(horas_a_fusionar)]
            
            print(f" Horas a fusionar (consolidadas): {sorted(horas_a_fusionar)}")
            print(f" Filas aire tras filtro: {len(df_aire_filtrado)}")
            print(f" Filas tráfico tras filtro: {len(df_trafico_filtrado)}")
            
            # Hacer join espacial y temporal
            df_fusion = join_espacial_temporal(df_trafico_filtrado, df_aire_filtrado)
            
            if not df_fusion.empty:
                # Crear directorio de fusión si no existe
                os.makedirs("data/fusion", exist_ok=True)
                
                # Guardar fusionado (usar timestamp del archivo de aire)
                tt = datetime.now().strftime("%Y%m%d_%H%M%S")
                fusion_file = f"data/fusion/fusion_{tt}.csv"
                df_fusion.to_csv(fusion_file, index=False)
                print(f" Fusionado y guardado: {fusion_file} ({len(df_fusion)} registros)")
                
                # Eliminar archivos usados
                horas_fusionadas = set(horas_a_fusionar)
                archivos_eliminados = 0
                
                for traffic_file in traffic_files:
                    try:
                        df_t = pd.read_csv(traffic_file, usecols=["fecha_hora_trunc"])
                        df_t["fecha_hora_trunc"] = pd.to_datetime(df_t["fecha_hora_trunc"])
                        horas_en_file = set(df_t["fecha_hora_trunc"].unique())
                        
                        # Si todas las horas del archivo están fusionadas, se puede borrar
                        if horas_en_file.issubset(horas_fusionadas):
                            os.remove(traffic_file)
                            archivos_eliminados += 1
                    except Exception as e:
                        print(f"[WARN] No se pudo inspeccionar {traffic_file}: {e}")

                # Eliminar archivo de aire
                os.remove(air_file)
                
                print(f" Eliminados: {air_file} y {archivos_eliminados} archivos de tráfico")
                if archivos_eliminados < len(traffic_files):
                    print(f" Conservados {len(traffic_files) - archivos_eliminados} archivos de tráfico con horas pendientes")
                
                fusion_exitosa = True
                
            else:
                print(f" No se pudieron fusionar datos para {timestamp}")
                
        except Exception as e:
            print(f" Error procesando {air_file}: {e}")
            continue
    
    return fusion_exitosa