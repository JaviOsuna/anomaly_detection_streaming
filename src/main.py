from pyspark.sql import SparkSession
import mlflow

def main():
    print("¡El entorno está funcionando correctamente!")
    spark = SparkSession.builder.appName("Test").getOrCreate()
    print("SparkSession creada con éxito")
    print("MLflow versión:", mlflow.__version__)

if __name__ == "__main__":
    main()