import os
from math import pi
import numpy as np
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from sklearn.neighbors import NearestNeighbors
import holidays

MASTER_PATH = "data/store/master_delta"
FEATURES_PATH = "data/store/master_features_delta"
NEIGHBORS_CSV = "data/store/neighbors_k3.csv"
STATS_CSV = "data/store/stats_by_station.csv"

def spark_with_delta(app="features"):
    return (SparkSession.builder
        .appName(app)
        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
    import pandas as pd
    return pd.DataFrame(rows)

def main():
    spark = spark_with_delta("features_fusion")
    spark.sparkContext.setLogLevel("WARN")

    if not os.path.exists(MASTER_PATH):
        raise RuntimeError("No existe master_delta. Ejecuta primero ingest_spark.py")

    df_raw = spark.read.format("delta").load(MASTER_PATH)

    # Si existe maestro features, obtener fechas ya procesadas
    if os.path.exists(FEATURES_PATH):
        df_features = spark.read.format("delta").load(FEATURES_PATH)
        processed_dates = [row['date'] for row in df_features.select(F.to_date("fecha_hora").alias("date")).distinct().collect()]
        df_to_process = df_raw.filter(~F.to_date("fecha_hora").isin(processed_dates))
        if df_to_process.rdd.isEmpty():
            print("No hay datos nuevos para procesar en features.")
            spark.stop()
            return
    else:
        df_to_process = df_raw

    # Calculamos features solo para datos nuevos
    df = df_to_process.withColumn("hour", F.hour("fecha_hora")) \
           .withColumn("hour_sin", F.sin(F.lit(2*pi) * F.col("hour")/F.lit(24))) \
           .withColumn("hour_cos", F.cos(F.lit(2*pi) * F.col("hour")/F.lit(24))) \
           .withColumn("dayofweek", F.dayofweek("fecha_hora")) \
           .withColumn("is_weekend", F.when(F.col("dayofweek").isin(1,7), F.lit(1)).otherwise(F.lit(0)))

    @F.udf(T.IntegerType())
    def is_es_holiday(ts):
        if ts is None: return 0
        try:
            d = ts.date()
            return 1 if d in holidays.CountryHoliday('ES') else 0
        except Exception:
            return 0
    df = df.withColumn("is_holiday", is_es_holiday(F.col("fecha_hora")))

    w_order = Window.partitionBy("ESTACION").orderBy("fecha_hora")
    df = df.withColumn("NO2_lag1", F.lag("NO2", 1).over(w_order)) \
           .withColumn("NO2_lag2", F.lag("NO2", 2).over(w_order)) \
           .withColumn("NO2_lag24", F.lag("NO2", 24).over(w_order)) \
           .withColumn("diff_1", F.col("NO2") - F.col("NO2_lag1")) \
           .withColumn("pct_change_1", F.col("diff_1") / (F.col("NO2_lag1") + F.lit(1e-9)) * 100.0)

    df = df.withColumn("ts_long", F.col("fecha_hora").cast("long"))
    w8  = Window.partitionBy("ESTACION").orderBy("ts_long").rangeBetween(-8*3600, 0)
    w24 = Window.partitionBy("ESTACION").orderBy("ts_long").rangeBetween(-24*3600, 0)
    w168= Window.partitionBy("ESTACION").orderBy("ts_long").rangeBetween(-168*3600, 0)

    df = df.withColumn("rolling_mean_8h",  F.avg("NO2").over(w8)) \
           .withColumn("rolling_std_8h",   F.stddev_pop("NO2").over(w8)) \
           .withColumn("rolling_mean_24h", F.avg("NO2").over(w24)) \
           .withColumn("rolling_std_24h",  F.stddev_pop("NO2").over(w24)) \
           .withColumn("rolling_mean_168h",F.avg("NO2").over(w168)) \
           .withColumn("rolling_std_168h", F.stddev_pop("NO2").over(w168))

    df = df.withColumn("rolling_std_8h", F.coalesce("rolling_std_8h", F.lit(0.0))) \
           .withColumn("rolling_z_8h", (F.col("NO2") - F.col("rolling_mean_8h")) / (F.col("rolling_std_8h") + F.lit(1e-9))) \
           .withColumn("spike_flag_8h", (F.abs(F.col("rolling_z_8h")) > F.lit(3.5)).cast("int"))

    cnt_per_station = df.groupBy("ESTACION").agg(F.count("*").alias("n_rows"))
    df = df.join(cnt_per_station, "ESTACION", "left") \
           .withColumn("small_history_flag", (F.col("n_rows") < F.lit(48)).cast("boolean")) \
           .withColumn("is_missing_flag", F.col("NO2").isNull()) \
           .drop("n_rows")

    coords_pdf = df.select("ESTACION","st_x","st_y").distinct().toPandas()
    neighbors_pdf = compute_neighbors(coords_pdf)
    neighbors_pdf.to_csv(NEIGHBORS_CSV, index=False)
    neighbors_sdf = spark.createDataFrame(neighbors_pdf)

    a = df.alias("a")
    b = df.alias("b")
    neigh_expanded = neighbors_sdf.select("ESTACION", F.col("neighbor").alias("NEIGH"))
    joined = (a.join(neigh_expanded, on="ESTACION", how="left")
               .join(b, (F.col("NEIGH")==F.col("b.ESTACION")) & (F.col("a.fecha_hora")==F.col("b.fecha_hora")), how="left")
               .groupBy("a.ESTACION","a.fecha_hora")
               .agg(F.avg("b.NO2").alias("mean_NO2_neighbors_k3")))

    df = (df.join(joined, on=["ESTACION","fecha_hora"], how="left")
            .withColumn("neighbor_diff", F.col("NO2") - F.col("mean_NO2_neighbors_k3")))

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

    # Unir con features existentes si hay
    if os.path.exists(FEATURES_PATH):
        df_features = spark.read.format("delta").load(FEATURES_PATH)
        combined = df_features.unionByName(df)
    else:
        combined = df

    combined = combined.withColumn("date", F.to_date("fecha_hora"))
    (combined.write.format("delta")
     .mode("overwrite")
     .partitionBy("date")
     .save(FEATURES_PATH))

    print("Features OK.")
    print(f"- Delta features: {FEATURES_PATH}")
    print(f"- Vecinos CSV: {NEIGHBORS_CSV}")
    print(f"- Stats CSV: {STATS_CSV}")

    spark.stop()

if __name__ == "__main__":
    main()