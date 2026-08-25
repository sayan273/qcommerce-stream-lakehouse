import json
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "warehouse",
    "user": "de_user",
    "password": "de_password"
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def consume_stream():
    consumer = KafkaConsumer(
        'order-events',
        bootstrap_servers=['localhost:19092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='order_processor_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    conn = get_db_connection()
    cursor = conn.cursor()
    
    batch = []
    BATCH_SIZE = 10
    
    print("Listening for messages on 'order-events'...")

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
                print(f"Committed batch of {len(batch)} records to PostgreSQL.")
                batch.clear()

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        if batch:
            execute_values(cursor, insert_query, batch)
            conn.commit()
        cursor.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    consume_stream()