import os, time
from typing import Tuple, Dict, Any
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark import StorageLevel
import mlflow
import mlflow.spark  # ✅ Import fijo

print(">>> DEBUG: src/model.py FINAL CLEAN <<<", flush=True)

# Configuración
MASTER_FEATURES_PATH = "data/store/master_features_delta"
RESULTS_PATH = "data/store/predictions_delta"
MODEL_NAME = "GBT_NO2_AnomalyDetection"
RMSE_THRESHOLD = 20.0

FEATURE_COLS = [
    "hour_sin","hour_cos","is_weekend","is_holiday",
    "NO2_lag1","NO2_lag2","NO2_lag24",
    "diff_1","pct_change_1",
    "rolling_mean_8h","rolling_std_8h",
    "rolling_mean_24h","rolling_std_24h",
    "rolling_mean_168h","rolling_std_168h",
    "mean_NO2_neighbors_k3","small_history_flag","is_missing_flag"
]

DEBUG_LIMIT = int(os.getenv("DEBUG_LIMIT", "0"))

def spark_with_delta(app="gbt_training") -> SparkSession:
    return (SparkSession.builder
        .appName(app)
        .master("local[8]")  # usar 8 cores
        .config("spark.sql.shuffle.partitions", "16")  # particiones suficientes
        .config("spark.driver.memory", "6g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .getOrCreate())

def setup_mlflow():
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(MODEL_NAME)
    print(f">>> MLflow ready at {tracking_uri}", flush=True)

def load_data(spark: SparkSession) -> DataFrame:
    print(">>> loading delta...", flush=True)
    df = spark.read.format("delta").load(MASTER_FEATURES_PATH)
    _ = df.take(1)  # validación mínima
    print(">>> take(1) ok", flush=True)

    if DEBUG_LIMIT > 0:
        df = df.limit(DEBUG_LIMIT)
        print(f">>> DEBUG_LIMIT active: {DEBUG_LIMIT}", flush=True)

    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    sample_dates = df.select("fecha_hora").limit(5).collect()
    print(f">>> sample dates: {[r['fecha_hora'] for r in sample_dates]}", flush=True)
    return df

def prepare_features(df: DataFrame) -> DataFrame:
    print(">>> preparing features...", flush=True)
    df = (df
        .withColumn("small_history_flag", F.col("small_history_flag").cast("int"))
        .withColumn("is_missing_flag", F.col("is_missing_flag").cast("int"))
        .withColumn("is_weekend", F.col("is_weekend").cast("int"))
        .withColumn("is_holiday", F.col("is_holiday").cast("int"))
        .withColumn("NO2", F.col("NO2").cast("double"))
    )
    num_to_fill = [c for c in FEATURE_COLS if c not in ["is_weekend","is_holiday","small_history_flag","is_missing_flag"]]
    df = df.fillna({c: 0.0 for c in num_to_fill})

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features", handleInvalid="skip")
    df = assembler.transform(df).dropna(subset=["NO2"])

    # Persistir y repartir para performance, sin checkpoint eager
    df = df.repartition(16).persist(StorageLevel.MEMORY_AND_DISK)
    print(">>> features ready (cached, no eager checkpoint)", flush=True)
    return df

def temporal_split_light(df: DataFrame, train_ratio=0.8) -> Tuple[DataFrame, DataFrame, Dict[str, Any]]:
    print(">>> temporal split (approxQuantile)...", flush=True)
    df = df.filter(F.col("fecha_hora").isNotNull()).withColumn("ts", F.unix_timestamp("fecha_hora").cast("long"))

    q = df.approxQuantile("ts", [0.0, 1.0], 0.01)
    if not q or len(q) < 2 or q[0] is None or q[1] is None or q[0] == q[1]:
        print(">>> WARN: rango temporal inválido; usando randomSplit", flush=True)
        train_df, test_df = df.randomSplit([train_ratio, 1-train_ratio], seed=42)
        return train_df, test_df, {"mode": "random"}

    min_ts, max_ts = int(q[0]), int(q[1])
    cutoff = int(min_ts + train_ratio * (max_ts - min_ts))

    train_df = df.filter(F.col("ts") <= cutoff).drop("ts")
    test_df  = df.filter(F.col("ts") >  cutoff).drop("ts")
    print(f">>> split done (temporal approx). cutoff_ts={cutoff}", flush=True)
    return train_df, test_df, {"mode": "temporal_approx", "cutoff_ts": cutoff}

def train_and_eval(train_df, test_df, name="gbt_d3_i30"):
    print(">>> fitting model...", flush=True)

    with mlflow.start_run(run_name=name):
        mlflow.log_param("algo", "GBTRegressor")
        mlflow.log_param("maxDepth", 3)
        mlflow.log_param("maxIter", 30)
        mlflow.log_param("subsamplingRate", 0.8)

        gbt = GBTRegressor(
            featuresCol="features", labelCol="NO2",
            maxIter=30, maxDepth=3, subsamplingRate=0.8, seed=42
        )

        tfit = time.time()
        model = gbt.fit(train_df)
        print(f">>> fit() took {time.time()-tfit:.1f}s", flush=True)

        print(">>> predicting...", flush=True)
        tpred = time.time()
        pred_df = model.transform(test_df)
        print(f">>> transform() took {time.time()-tpred:.1f}s", flush=True)

        evaluator = RegressionEvaluator(labelCol="NO2", predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(pred_df)
        mlflow.log_metric("rmse", rmse)
        print(f">>> RMSE: {rmse:.4f}", flush=True)

        mlflow.spark.log_model(model, "model", registered_model_name=MODEL_NAME)
        print(">>> model registered in MLflow", flush=True)
        return model, pred_df, rmse

def main():
    spark = spark_with_delta()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        setup_mlflow()
        df = load_data(spark)
        df = prepare_features(df)

        train_df, test_df, split_info = temporal_split_light(df)
        model, pred_df, rmse = train_and_eval(train_df, test_df, name="gbt_d3_i30")

        if DEBUG_LIMIT == 0:
            print(">>> saving predictions (delta overwrite)...", flush=True)
            pred_df.write.format("delta").mode("overwrite").save(RESULTS_PATH)

        if rmse < RMSE_THRESHOLD:
            print(f">>> Modelo supera umbral (RMSE<{RMSE_THRESHOLD})", flush=True)
        else:
            print(f">>> Modelo no supera umbral (RMSE>={RMSE_THRESHOLD})", flush=True)

        print(">>> DONE", flush=True)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()