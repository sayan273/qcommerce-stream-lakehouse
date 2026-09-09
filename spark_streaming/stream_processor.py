import json
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer, KafkaProducer
from pydantic import ValidationError
import sys
import os

# Ensure models directory is in pythonpath
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from data_producer.schemas.order_models import OrderEventModel

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "warehouse",
    "user": "de_user",
    "password": "de_password"
}

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
    
    print("Listening with Pydantic contract validation active...")

    insert_query = """
        INSERT INTO raw_orders (
            order_id, user_id, city, category, amount, payment_mode, status, event_timestamp
        )
        VALUES %s
        ON CONFLICT (order_id) DO NOTHING;
    """

    try:
        for message in consumer:
            raw_event = message.value

            # Strict Pydantic Schema Validation
            try:
                validated_order = OrderEventModel(**raw_event)
            except ValidationError as err:
                dlq_payload = {
                    "raw_record": raw_event,
                    "validation_errors": err.errors(),
                    "quarantined_at": message.timestamp
                }
                dlq_producer.send('order-events-dlq', value=dlq_payload)
                print(f"⚠️ Contract Violation -> DLQ: {raw_event.get('order_id')} | {err.error_count()} errors")
                continue

            batch.append((
                validated_order.order_id,
                validated_order.user_id,
                validated_order.city,
                validated_order.category,
                validated_order.amount,
                validated_order.payment_mode,
                validated_order.status,
                validated_order.timestamp.isoformat()
            ))

            if len(batch) >= BATCH_SIZE:
                execute_values(cursor, insert_query, batch)
                conn.commit()
                print(f"Committed batch of {len(batch)} validated records to PostgreSQL.")
                batch.clear()

    except KeyboardInterrupt:
        print("Stopping processor...")
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