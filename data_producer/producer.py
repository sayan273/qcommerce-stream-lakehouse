import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker('en_IN')

producer = KafkaProducer(
    bootstrap_servers=['localhost:19092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

CITIES = ['Bengaluru', 'Mumbai', 'Delhi-NCR', 'Hyderabad', 'Chennai']
CATEGORIES = ['Groceries', 'Dairy & Eggs', 'Snacks', 'Beverages', 'Personal Care']
PAYMENT_MODES = ['UPI', 'Credit Card', 'NetBanking', 'COD']

def generate_order():
    # 5% chance to simulate a malformed/corrupted event
    if random.random() < 0.05:
        return {
            "order_id": fake.uuid4(),
            "user_id": None,
            "city": random.choice(CITIES),
            "amount": -150.00,  # Negative anomaly
            "payment_mode": "UNKNOWN_GATEWAY",
            "status": "CORRUPTED",
            "timestamp": datetime.utcnow().isoformat()
        }

    return {
        "order_id": fake.uuid4(),
        "user_id": f"USR_{random.randint(1000, 9999)}",
        "city": random.choice(CITIES),
        "category": random.choice(CATEGORIES),
        "amount": round(random.uniform(50.0, 2500.0), 2),
        "payment_mode": random.choice(PAYMENT_MODES),
        "status": random.choices(["SUCCESS", "FAILED", "PENDING"], weights=[0.88, 0.08, 0.04])[0],
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    print("Starting Order Event Producer with Anomaly Simulation...")
    try:
        while True:
            event = generate_order()
            producer.send('order-events', value=event)
            print(f"Emitted: {event['order_id']} | ₹{event.get('amount')} | Status: {event.get('status')}")
            time.sleep(random.uniform(0.2, 0.8))
    except KeyboardInterrupt:
        print("Producer stopped.")