from dotenv import load_dotenv
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, max, min, avg, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os


spark_version = pyspark.__version__
kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
bigquery_package = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.44.2"
inject_package = "javax.inject:javax.inject:1"
load_dotenv()
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
spark = SparkSession.builder \
    .appName("RetailStream Processor") \
    .config("spark.jars.packages", f"{kafka_package},{bigquery_package},{inject_package}") \
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

silver_df = silver_df.withColumn("timestamp", to_timestamp(col("timestamp")))
gold_df = silver_df.groupBy(
    window(col("timestamp"), "5 minutes"),
    col("product_id")
).agg(
    max("price").alias("max_price"),
    min("price").alias("min_price"),
    round(avg("price"), 2).alias("avg_price"),
    max("stock").alias("max_stock"),
    min("stock").alias("min_stock"),
    round(avg("stock"), 2).alias("avg_stock")
)

def write_to_bigquery(batch_df, batch_id):
    batch_df.write \
        .format("bigquery") \
        .option("table", "retailstream-pipeline.gold_layer.market_metrics") \
        .option("writeMethod", "direct") \
        .option("credentialsFile", credentials_path) \
        .mode("append") \
        .save()

query = gold_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_bigquery) \
    .option("checkpointLocation", "./checkpoints") \
    .start()

query.awaitTermination()
