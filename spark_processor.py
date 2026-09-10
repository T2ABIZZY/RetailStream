import logging
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, max, min, avg, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# PySpark 3.5 requires Java 8, 11, or 17. Java 23+ removes javax.security.auth.Subject.getSubject().
java17_path = "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"
if os.path.exists(java17_path):
    os.environ["JAVA_HOME"] = java17_path

kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
bigquery_package = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.44.2"
inject_package = "javax.inject:javax.inject:1"

spark = SparkSession.builder \
    .appName("RetailStream Processor") \
    .config("spark.jars.packages", f"{kafka_package},{bigquery_package},{inject_package}") \
    .getOrCreate()


logger.info(f"Connecting to Kafka bootstrap servers: {config.KAFKA_BOOTSTRAP_SERVERS}")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", config.KAFKA_TOPIC) \
    .option("failOnDataLoss", "false") \
    .load()


json_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("retailer_name", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock", IntegerType(), True)
])

# Silver Layer: JSON Parsing & Timestamp Typing
silver_df = df.select(
    from_json(col("value").cast("string"), json_schema).alias("data")
).select("data.*")

silver_df = silver_df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Gold Layer: Stateful Streaming Window Aggregation with Watermarking
gold_df = silver_df \
    .withWatermark("timestamp", config.WATERMARK_DURATION) \
    .groupBy(
        window(col("timestamp"), config.WINDOW_DURATION),
        col("product_id")
    ).agg(
        max("price").alias("max_price"),
        min("price").alias("min_price"),
        round(avg("price"), 2).alias("avg_price"),
        max("stock").alias("max_stock"),
        min("stock").alias("min_stock"),
        round(avg("stock"), 2).alias("avg_stock")
    )

# Flatten window struct for direct Looker Studio & BigQuery timestamp indexing
gold_df = gold_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("product_id"),
    col("max_price"),
    col("min_price"),
    col("avg_price"),
    col("max_stock"),
    col("min_stock"),
    col("avg_stock")
)

def write_to_bigquery(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        logger.info(f"Micro-batch {batch_id} is empty. Skipping BigQuery write.")
        return
        
    logger.info(f"Writing micro-batch {batch_id} ({count} rows) to BigQuery table: {config.BIGQUERY_TABLE}")
    batch_df.write \
        .format("bigquery") \
        .option("table", config.BIGQUERY_TABLE) \
        .option("writeMethod", "direct") \
        .option("credentialsFile", config.GOOGLE_APPLICATION_CREDENTIALS) \
        .mode("append") \
        .save()

query = gold_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_bigquery) \
    .option("checkpointLocation", config.CHECKPOINT_LOCATION) \
    .start()

logger.info("RetailStream PySpark Structured Streaming pipeline actively processing...")
query.awaitTermination()

