import os
from math import pi
import pandas as pd
from pyspark.sql import SparkSession, functions as F, types as T
from sklearn.neighbors import NearestNeighbors

MASTER_FEATURES_PATH = "data/store/master_features_delta"
NEIGHBORS_CSV = "data/store/neighbors_k3.csv"
STATS_CSV = "data/store/stats_by_station.csv"

def spark_with_delta(app="eda"):
    return (SparkSession.builder
            .appName(app)
            .master("local[4]")  # 2 hilos; sube a 4 si tu máquina lo soporta
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.shuffle.partitions", "4")  # baja shuffles
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.adaptive.enabled", "false")  # Streaming lo desactiva igual, evitamos overhead
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
            .getOrCreate())

def compute_neighbors(coords_pdf):
    X = coords_pdf[['st_x','st_y']].values
    nbrs = NearestNeighbors(n_neighbors=4, algorithm='kd_tree').fit(X)
    distances, indices = nbrs.kneighbors(X)
    rows = []
    for i, est in enumerate(coords_pdf['ESTACION'].tolist()):
        neigh_idxs = indices[i][1:4]
        for j, ni in enumerate(neigh_idxs):
            rows.append({
                "ESTACION": int(est),
                "neighbor": int(coords_pdf.iloc[ni]['ESTACION']),
                "rank": j+1,
                "dist": float(distances[i][1:4][j])
            })
    return pd.DataFrame(rows)

def main():
    spark = spark_with_delta("eda_fusion")
    spark.sparkContext.setLogLevel("WARN")

    if not os.path.exists(MASTER_FEATURES_PATH):
        raise RuntimeError("No existe master_features_delta. Ejecuta primero features_spark.py")

    df = spark.read.format("delta").load(MASTER_FEATURES_PATH)

    # Estadísticas por estación
    quantiles = df.groupBy("ESTACION").agg(
        F.expr("percentile_approx(NO2, 0.5)").alias("median_NO2"),
        F.expr("percentile_approx(NO2, 0.9)").alias("p90_NO2"),
        F.expr("percentile_approx(NO2, 0.95)").alias("p95_NO2"),
        F.count("*").alias("count")
    )

    df_med = df.join(quantiles.select("ESTACION","median_NO2"), "ESTACION", "left") \
               .withColumn("abs_dev", F.abs(F.col("NO2") - F.col("median_NO2")))
    mad = df_med.groupBy("ESTACION").agg(F.expr("percentile_approx(abs_dev, 0.5)").alias("MAD_NO2"))
    stats = (quantiles.join(mad, "ESTACION", "left")).toPandas()
    stats.to_csv(STATS_CSV, index=False)
    print(f"Estadísticas guardadas en {STATS_CSV}")

    # Tabla de vecinos k=3
    coords_pdf = df.select("ESTACION","st_x","st_y").distinct().toPandas()
    neighbors_pdf = compute_neighbors(coords_pdf)
    neighbors_pdf.to_csv(NEIGHBORS_CSV, index=False)
    print(f"Tabla de vecinos guardada en {NEIGHBORS_CSV}")

    # EDA básico: resumen y distribución NO2
    print("Resumen general:")
    df.describe(["NO2"]).show()

    print("Distribución NO2 por estación (primeras 5):")
    for est in stats["ESTACION"].head(5):
        print(f"Estación {est}:")
        df.filter(F.col("ESTACION") == est).select("NO2").describe().show()

    spark.stop()

if __name__ == "__main__":
    main()