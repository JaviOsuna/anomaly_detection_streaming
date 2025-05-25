from pyspark.sql import SparkSession

def main():
    """
    Crea una sesión de Spark y muestra un mensaje de prueba.
    """
    spark = SparkSession.builder.appName("TestSpark").getOrCreate()

    print("¡PySpark está funcionando correctamente!")

    # Crea un DataFrame de ejemplo
    data = [("Alice", 30), ("Bob", 40), ("Charlie", 50)]
    df = spark.createDataFrame(data, ["Name", "Age"])

    # Muestra el DataFrame
    df.show()

    spark.stop()

if __name__ == "__main__":
    main()