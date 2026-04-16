import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

TOPIC = os.getenv("KAFKA_TOPIC", "click-events")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
SLEEP_SECONDS = float(os.getenv("PRODUCER_SLEEP_SECONDS", "0.5"))

USERS = [f"U{1000+i}" for i in range(1, 101)]
PRODUCTS = [
    {"product_id": "P1001", "category": "Mobile Phones"},
    {"product_id": "P1002", "category": "Mobile Phones"},
    {"product_id": "P1003", "category": "Audio"},
    {"product_id": "P1004", "category": "Laptops"},
    {"product_id": "P1005", "category": "Laptops"},
    {"product_id": "P1006", "category": "Accessories"},
]
STANDARD_EVENT_TYPES = ["view", "add_to_cart", "purchase"]
STANDARD_EVENT_WEIGHTS = [0.68, 0.20, 0.12]
LOW_CONVERSION_PRODUCT_IDS = {"P1001", "P1002"}
LOW_CONVERSION_EVENT_WEIGHTS = [0.985, 0.014, 0.001]
LOW_CONVERSION_SPIKE_PROBABILITY = 0.25

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def generate_event() -> dict:
    product = random.choice(PRODUCTS)
    if random.random() < LOW_CONVERSION_SPIKE_PROBABILITY:
        # Force occasional bursts of views for a small set of products so the
        # flash-sale condition can be demonstrated reliably during the demo.
        product = random.choice([p for p in PRODUCTS if p["product_id"] in LOW_CONVERSION_PRODUCT_IDS])
        event_type = "view" if random.random() < 0.995 else "add_to_cart"
    else:
        event_weights = (
            LOW_CONVERSION_EVENT_WEIGHTS
            if product["product_id"] in LOW_CONVERSION_PRODUCT_IDS
            else STANDARD_EVENT_WEIGHTS
        )
        event_type = random.choices(STANDARD_EVENT_TYPES, weights=event_weights, k=1)[0]

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(USERS),
        "product_id": product["product_id"],
        "category": product["category"],
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


if __name__ == "__main__":
    print("Starting clickstream producer...")
    try:
        while True:
            event = generate_event()
            producer.send(TOPIC, value=event)
            print(f"Sent: {event}")
            time.sleep(SLEEP_SECONDS)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.flush()
        producer.close()
