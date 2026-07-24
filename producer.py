import random
from datetime import datetime, timezone
from typing import Any, Tuple
from confluent_kafka import Producer
import json
import time


CATALOG = {
    "SKU-84920": {"base_price": 120.00, "base_stock": 80},   # 65% Mechanical Keyboard (Linear)
    "SKU-19384": {"base_price": 135.00, "base_stock": 45},   # 65% Mechanical Keyboard (Tactile)
    "SKU-57201": {"base_price": 250.00, "base_stock": 110},  # 144Hz Gaming Monitor
    "SKU-99213": {"base_price": 110.00, "base_stock": 200},  # Ultralight Wireless Mouse
    "SKU-48192": {"base_price": 25.00,  "base_stock": 450},  # Ergonomic Wrist Rest
    "SKU-22349": {"base_price": 30.00,  "base_stock": 300},  # XXL Desk Mat
    "SKU-75830": {"base_price": 320.00, "base_stock": 60},   # Noise Cancelling Headphones
    "SKU-31094": {"base_price": 85.00,  "base_stock": 150},  # 1TB NVMe SSD
    "SKU-66482": {"base_price": 45.00,  "base_stock": 250},  # USB-C Docking Hub
    "SKU-80127": {"base_price": 60.00,  "base_stock": 90}    # PBT Keycap Set
}


def generate_ecommerce_payload() -> Tuple[dict[str, Any], str]:
    retailers = [
        "ShopSphere",
        "UrbanCart",
        "MegaRetail",
        "QuickBuy",
        "PrimeOutlet",
    ]
    
    product_id = random.choice(list(CATALOG.keys()))
    product_data = CATALOG[product_id]
    
    price_volatility = random.uniform(-0.05, 0.05)
    current_price = round(product_data["base_price"] * (1 + price_volatility), 2)
    
    current_stock = max(0, product_data["base_stock"] + random.randint(-5, 2))
    
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "retailer_name": random.choice(retailers),
        "product_id": product_id,
        "price": current_price,
        "stock": current_stock,
    }
    
    return payload, product_id


if __name__ == "__main__":
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    topic = "price_events"
    
    print("Starting realistic data stream...")
    try:
        while True:
            payload, product_id = generate_ecommerce_payload()
            
            producer.produce(
                topic, 
                value=json.dumps(payload).encode("utf-8"), 
                key=product_id.encode("utf-8")
            )
            print(f"Produced payload: {payload}")
            
            time.sleep(0.5) 
            
    except KeyboardInterrupt:
        print("\nFlushing records...")
        producer.flush()
        print("Stopping producer...")
