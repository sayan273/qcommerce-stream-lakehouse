import json
import time
import random
from datetime import datetime, timezone
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
    prep_time = random.randint(3, 12)       # Minutes to pack
    transit_time = random.randint(5, 25)    # Minutes to deliver
    total_delivery_time = prep_time + transit_time

    return {
        "order_id": fake.uuid4(),
        "user_id": f"USR_{random.randint(1000, 9999)}",
        "rider_id": f"RIDER_{random.randint(100, 999)}",
        "city": random.choice(CITIES),
        "category": random.choice(CATEGORIES),
        "amount": round(random.uniform(50.0, 2500.0), 2),
        "payment_mode": random.choice(PAYMENT_MODES),
        "status": random.choices(["SUCCESS", "FAILED", "PENDING"], weights=[0.88, 0.08, 0.04])[0],
        "prep_time_minutes": prep_time,
        "transit_time_minutes": transit_time,
        "total_delivery_minutes": total_delivery_time,
        "is_sla_breached": total_delivery_time > 20,  # 20-minute Quick-Commerce SLA
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

if __name__ == "__main__":
    print("Starting Enhanced Order & Rider Telemetry Producer...")
    try:
        while True:
            event = generate_order()
            producer.send('order-events', value=event)
            print(f"Emitted: Order {event['order_id']} | Rider: {event['rider_id']} | Time: {event['total_delivery_minutes']}m | SLA Breach: {event['is_sla_breached']}")
            time.sleep(random.uniform(0.2, 0.8))
    except KeyboardInterrupt:
        print("Producer stopped.")