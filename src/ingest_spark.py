import os
import shutil
from glob import glob
from pyspark.sql import SparkSession, functions as F, types as T

FUSION_PATH = "/app/data/fusion/"
FUSION_PROCESSED_PATH = "/app/data/fusion_processed/"
MASTER_PATH = "/app/data/store/master_delta"

def get_new_files():
    all_files = glob(FUSION_PATH + "*.csv")
    processed_files = set(os.listdir(FUSION_PROCESSED_PATH)) if os.path.exists(FUSION_PROCESSED_PATH) else set()
    new_files = [f for f in all_files if os.path.basename(f) not in processed_files]
    return new_files

def move_files_to_processed(files):
    os.makedirs(FUSION_PROCESSED_PATH, exist_ok=True)
    for f in files:
        dest = os.path.join(FUSION_PROCESSED_PATH, os.path.basename(f))
        shutil.move(f, dest)

def spark_with_delta(app="ingest"):
    return (SparkSession.builder
        .appName(app)
        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .getOrCreate())

def main():
    new_files = get_new_files()
    if not new_files:
        print("No hay archivos nuevos para procesar.")
        return

    spark = spark_with_delta("fusion_ingest")
    spark.sparkContext.setLogLevel("WARN")

    schema = T.StructType([
        T.StructField("ESTACION", T.IntegerType(), True),
        T.StructField("fecha_hora", T.StringType(), True),
        T.StructField("st_x", T.DoubleType(), True),
        T.StructField("st_y", T.DoubleType(), True),
        T.StructField("NO2", T.DoubleType(), True),
        T.StructField("intensidad", T.DoubleType(), True),
        T.StructField("ocupacion", T.DoubleType(), True),
        T.StructField("carga", T.DoubleType(), True),
        T.StructField("nivelServicio", T.IntegerType(), True),
        T.StructField("intensidadSat", T.DoubleType(), True),
        T.StructField("distancia_estacion", T.DoubleType(), True),
        T.StructField("num_puntos_trafico", T.IntegerType(), True),
    ])

    # Leer solo los archivos nuevos
    df_new = spark.read.option("header", True).schema(schema).csv(new_files)
    df_new = df_new.withColumn("fecha_hora", F.to_timestamp("fecha_hora", "yyyy-MM-dd HH:mm:ss")) \
                   .dropna(subset=["ESTACION","fecha_hora"])

    if df_new.rdd.isEmpty():
        print("No hay datos nuevos en los archivos.")
        spark.stop()
        return

    df_new = df_new.withColumn("date", F.to_date("fecha_hora"))

    if os.path.exists(MASTER_PATH):
        df_master = spark.read.format("delta").load(MASTER_PATH)
        combined = df_master.unionByName(df_new)
    else:
        combined = df_new

    dedup = combined.dropDuplicates(["ESTACION","fecha_hora"])

    (dedup.write.format("delta").mode("overwrite")
     .partitionBy("date").save(MASTER_PATH))

    row_cnt = dedup.count()
    dates = dedup.agg(F.min("fecha_hora").alias("min_ts"), F.max("fecha_hora").alias("max_ts")).collect()[0]
    est_cnt = dedup.select("ESTACION").distinct().count()
    print(f"Ingesta OK. Filas: {row_cnt}, Estaciones: {est_cnt}, Rango: {dates['min_ts']} -> {dates['max_ts']}")

    spark.stop()

    # Mover archivos procesados para evitar reprocesos
    move_files_to_processed(new_files)
    print(f"Archivos procesados movidos a {FUSION_PROCESSED_PATH}")

if __name__ == "__main__":
    main()