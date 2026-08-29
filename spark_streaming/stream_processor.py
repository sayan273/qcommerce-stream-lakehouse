import json
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer, KafkaProducer

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "warehouse",
    "user": "de_user",
    "password": "de_password"
}

REQUIRED_KEYS = {"order_id", "user_id", "city", "category", "amount", "payment_mode", "status", "timestamp"}

def validate_event(event):
    if not isinstance(event, dict):
        return False, "Payload is not a JSON object"
    if not REQUIRED_KEYS.issubset(event.keys()):
        return False, f"Missing required keys: {REQUIRED_KEYS - set(event.keys())}"
    if event.get("amount") is None or event.get("amount") <= 0:
        return False, f"Invalid amount: {event.get('amount')}"
    if event.get("status") not in {"SUCCESS", "FAILED", "PENDING"}:
        return False, f"Invalid status: {event.get('status')}"
    return True, "Valid"

def consume_stream():
    consumer = KafkaConsumer(
        'order-events',
        bootstrap_servers=['localhost:19092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='order_processor_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    dlq_producer = KafkaProducer(
        bootstrap_servers=['localhost:19092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    batch = []
    BATCH_SIZE = 10
    
    print("Listening for messages with DLQ validation enabled...")

    insert_query = """
        INSERT INTO raw_orders (
            order_id, user_id, city, category, amount, payment_mode, status, event_timestamp
        )
        VALUES %s
        ON CONFLICT (order_id) DO NOTHING;
    """

    try:
        for message in consumer:
            event = message.value
            is_valid, reason = validate_event(event)

            if not is_valid:
                dlq_payload = {
                    "raw_record": event,
                    "error_reason": reason,
                    "quarantined_at": message.timestamp
                }
                dlq_producer.send('order-events-dlq', value=dlq_payload)
                print(f"⚠️ DLQ Routed: {event.get('order_id')} | Reason: {reason}")
                continue

            batch.append((
                event["order_id"],
                event["user_id"],
                event["city"],
                event["category"],
                event["amount"],
                event["payment_mode"],
                event["status"],
                event["timestamp"]
            ))

            if len(batch) >= BATCH_SIZE:
                execute_values(cursor, insert_query, batch)
                conn.commit()
                print(f"Committed batch of {len(batch)} clean records to PostgreSQL.")
                batch.clear()

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        if batch:
            execute_values(cursor, insert_query, batch)
            conn.commit()
        cursor.close()
        conn.close()
        dlq_producer.close()
        consumer.close()

if __name__ == "__main__":
    consume_stream()