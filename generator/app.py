# generator/app.py
import os
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from transactions import create_random_transaction

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TRANSACTIONS_TOPIC = os.environ.get('TRANSACTIONS_TOPIC')
TRANSACTIONS_PER_SECOND = float(os.environ.get('TRANSACTIONS_PER_SECOND', 1))
SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND

print(f"KAFKA_BROKER_URL: {KAFKA_BROKER_URL}")
print(f"TRANSACTIONS_TOPIC: {TRANSACTIONS_TOPIC}")
print(f"TRANSACTIONS_PER_SECOND: {TRANSACTIONS_PER_SECOND}")

def create_kafka_producer(retries=10, delay=10):
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER_URL,
                value_serializer=lambda value: json.dumps(value).encode('utf-8'),
            )
            print("Connected to Kafka successfully!")
            return producer
        except NoBrokersAvailable as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(delay)
    print("Failed to connect to Kafka after multiple attempts")
    raise NoBrokersAvailable("Failed to connect to Kafka after multiple attempts")

if __name__ == '__main__':
    try:
        producer = create_kafka_producer()
        while True:
            transaction = create_random_transaction()
            producer.send(TRANSACTIONS_TOPIC, value=transaction)
            print(transaction)  # DEBUG
            time.sleep(SLEEP_TIME)
    except NoBrokersAvailable:
        print("Exiting script due to repeated failures to connect to Kafka")
    except Exception as e:
        print(f"Unexpected error: {e}")
