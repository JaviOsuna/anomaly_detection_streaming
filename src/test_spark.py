import mlflow
import mlflow.spark

from pyspark.sql import SparkSession

def main():
    """
    Crea una sesión de Spark, muestra un mensaje de prueba y registra información con MLflow.
    """
    with mlflow.start_run() as run:
        spark = SparkSession.builder.appName("TestSpark").getOrCreate()

        print("¡PySpark está funcionando correctamente!")

        # Crea un DataFrame de ejemplo
        data = [("Alice", 30), ("Bob", 40), ("Charlie", 50)]
        df = spark.createDataFrame(data, ["Name", "Age"])

        # Muestra el DataFrame
        df.show()

        # Registra un parámetro con MLflow
        mlflow.log_param("data_size", len(data))

        # Registra una métrica con MLflow
        mlflow.log_metric("example_metric", 123.45)

        spark.stop()
        
if __name__ == "__main__":
    main()