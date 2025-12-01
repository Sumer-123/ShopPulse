import time
import json
import random
from faker import Faker
from datetime import datetime
from kafka import KafkaProducer  # New Import

fake = Faker()

# Kafka Configuration
KAFKA_TOPIC = "shop_pulse_events"
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'  # Using the Docker service name "kafka"

# Initialize Kafka Producer
# We add a retry loop because Kafka might take a few seconds to start up
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connected to Kafka!")
    except Exception as e:
        print(f"Waiting for Kafka to start... Error: {e}")
        time.sleep(5)

USER_IDS = [i for i in range(1, 101)]
PRODUCT_IDS = [i for i in range(100, 200)]


def generate_click_data():
    return {
        "event_id": fake.uuid4(),
        "user_id": random.choice(USER_IDS),
        "product_id": random.choice(PRODUCT_IDS),
        "event_type": random.choice(["view", "add_to_cart", "purchase"]),
        "timestamp": datetime.now().isoformat(),
        "device_type": random.choice(["android", "ios", "web"]),
        "location": fake.city()
    }


if __name__ == "__main__":
    print("Starting Data Stream Simulation...")
    while True:
        dummy_data = generate_click_data()

        # Send data to Kafka Topic
        producer.send(KAFKA_TOPIC, dummy_data)
        producer.flush()  # Force send immediately

        print(f"Sent to Kafka: {dummy_data}")  # Just so we can see it working
        time.sleep(random.uniform(0.5, 2.0))