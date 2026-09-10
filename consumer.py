from confluent_kafka import Consumer
import config

def main():
    consumer = Consumer({
        'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': config.KAFKA_CONSUMER_GROUP,
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([config.KAFKA_TOPIC])

    print(f"Starting CLI consumer on topic '{config.KAFKA_TOPIC}'...")
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
        print("Closing consumer...")
        consumer.close()

if __name__ == "__main__":
    main()

