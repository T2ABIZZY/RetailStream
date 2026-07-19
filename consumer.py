from confluent_kafka import Consumer

def main():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'price_event_consumers',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe(['price_events'])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            print(f"Consumed message: {msg.value().decode('utf-8')} using key {msg.key().decode('utf-8')}")

    except KeyboardInterrupt:
        consumer.close()
        
if __name__ == "__main__":
    main()
