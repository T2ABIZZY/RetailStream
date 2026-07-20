import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Dynamically grab your PySpark 4.x version
spark_version = pyspark.__version__
kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}"

spark = SparkSession.builder \
    .appName("RetailStream Processor") \
    .config("spark.jars.packages", kafka_package) \
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

# 1. Unpack the JSON (Bronze to Silver)
silver_df = df.select(
    from_json(col("value").cast("string"), json_schema).alias("data")
).select("data.*")

# 2. Cast the string timestamp into a true PySpark TimestampType
silver_df = silver_df.withColumn("timestamp", to_timestamp(col("timestamp")))

# 3. Print the micro-batches to the console
query = silver_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()