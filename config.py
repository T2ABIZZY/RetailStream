import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "price_events")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "price_event_consumers")

# BigQuery Settings
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE", "retailstream-pipeline.gold_layer.market_metrics")

# Spark Streaming Window Settings
WATERMARK_DURATION = "10 minutes"
WINDOW_DURATION = "5 minutes"
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "./checkpoints")
