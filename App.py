import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import lit
from pyspark.sql.types import StructField, StringType, StructType, IntegerType


def main(directory) -> None:
    """ Program that reads parking data in streaming from a directory adding a new column.

    It is assumed that an external entity is writing files in that directory, and every file contains a
    parking data values

    :param directory: streaming directory
    """
    spark_session = SparkSession \
        .builder \
        .master("local[4]") \
        .appName("Updating the Parking Data") \
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    fields = [StructField("dato", StringType(), True),
              StructField("id", StringType(), True),
              StructField("libres", IntegerType(), True)]

    # Create DataFrame representing the stream of input lines
    lines = spark_session \
        .readStream \
        .format("csv") \
        .option("header", "true") \
        .schema(StructType(fields)) \
        .load(directory)

    lines.printSchema()

    values = lines.withColumn("capacity", lit(950))
    values.printSchema()

    # Start running the query that prints the output in the screen
    query = values \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python App.py <directory>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])