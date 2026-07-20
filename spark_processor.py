from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


spark = SparkSession.builder \
    .appName("RetailStream Processor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "price_events") \
    .load()

json_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("retailer_name", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock", IntegerType(), True)
])

silver_df = df.select(
    from_json(col("value").cast("string"), json_schema).alias("data")
).select("data.*")

query = silver_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()