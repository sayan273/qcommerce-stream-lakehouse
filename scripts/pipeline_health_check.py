import psycopg2
import sys
from kafka import KafkaAdminClient

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "warehouse",
    "user": "de_user",
    "password": "de_password"
}

def verify_kafka():
    try:
        admin_client = KafkaAdminClient(bootstrap_servers="localhost:19092", request_timeout_ms=3000)
        topics = admin_client.list_topics()
        print(f"✅ Kafka Connection: Active | Topics Found: {topics}")
        admin_client.close()
    except Exception as e:
        print(f"❌ Kafka Check Failed: {e}")

def verify_database_layers():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Verify Raw Bronze Table
        cursor.execute("SELECT COUNT(*) FROM raw_orders;")
        raw_count = cursor.fetchone()[0]
        print(f"✅ Bronze Layer (raw_orders): {raw_count} total records ingested.")
        
        # Verify Success Rate Breakdown
        cursor.execute("""
            SELECT status, COUNT(*) 
            FROM raw_orders 
            GROUP BY status;
        """)
        breakdown = cursor.fetchall()
        print(f"📊 Telemetry Distribution: {dict(breakdown)}")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Database Verification Failed: {e}")

if __name__ == "__main__":
    print("--- Starting Pipeline Health & Observability Check ---")
    verify_kafka()
    verify_database_layers()
    print("-----------------------------------------------------")