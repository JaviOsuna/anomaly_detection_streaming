from pyspark.sql import SparkSession
import mlflow

def main():
    print("¡El entorno está funcionando correctamente!")

    # Iniciar Spark
    spark = SparkSession.builder.appName("Test").getOrCreate()
    print("SparkSession creada con éxito")

    # Crear un DataFrame de prueba
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "valor"])
    df.show()

    # Probar MLflow
    with mlflow.start_run(run_name="prueba_spark_mlflow") as run:
        mlflow.log_param("param_prueba", 123)
        mlflow.log_metric("metric_prueba", 0.99)
        print("MLflow run ID:", run.info.run_id)

    spark.stop()

if __name__ == "__main__":
    main()