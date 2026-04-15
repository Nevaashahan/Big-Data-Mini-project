import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

TOPIC = "click-events"
BOOTSTRAP_SERVERS = "localhost:29092"

USERS = [f"U{1000+i}" for i in range(1, 101)]
PRODUCTS = [
    {"product_id": "P1001", "category": "Mobile Phones"},
    {"product_id": "P1002", "category": "Mobile Phones"},
    {"product_id": "P1003", "category": "Audio"},
    {"product_id": "P1004", "category": "Laptops"},
    {"product_id": "P1005", "category": "Laptops"},
    {"product_id": "P1006", "category": "Accessories"},
]
EVENT_TYPES = ["view", "add_to_cart", "purchase"]
EVENT_WEIGHTS = [0.78, 0.17, 0.05]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def generate_event() -> dict:
    product = random.choice(PRODUCTS)
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

    # Controlled spike for high-interest low-conversion behavior.
    if random.random() < 0.15:
        product = random.choice(PRODUCTS[:2])
        event_type = "view" if random.random() < 0.95 else "purchase"

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
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.flush()
        producer.close()
