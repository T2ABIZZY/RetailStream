import random
from datetime import datetime, timezone
from typing import Any
from confluent_kafka import Producer
import json
import time

def generate_ecommerce_payload() -> dict[str, Any]:
    retailers = [
        "ShopSphere",
        "UrbanCart",
        "MegaRetail",
        "QuickBuy",
        "PrimeOutlet",
    ]
    product_id = f"SKU-{random.randint(10000, 99999)}"
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "retailer_name": random.choice(retailers),
        "product_id": product_id,
        "price": round(random.uniform(5.0, 500.0), 2),
        "stock": random.randint(0, 1000),
    }
    return payload, product_id


if __name__ == "__main__":
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    topic = "price_events"
    try:
        while True:
            payload, product_id = generate_ecommerce_payload()
            producer.produce(topic, value=json.dumps(payload).encode("utf-8"), key=product_id.encode("utf-8"))
            print(f"Produced payload: {payload}")
            time.sleep(1)
    except KeyboardInterrupt:
        producer.flush()
        print("Stopping producer...")