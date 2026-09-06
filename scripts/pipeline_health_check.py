import psycopg2
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "warehouse",
    "user": "de_user",
    "password": "de_password"
}

def check_kafka_health():
    try:
        admin_client = KafkaAdminClient(bootstrap_servers="localhost:19092", request_timeout_ms=3000)
        topics = admin_client.list_topics()
        print(f"✅ Kafka Connection: Active | Topics: {topics}")
        admin_client.close()
    except Exception as e:
        print(f"❌ Kafka Check Failed: {e}")

def check_database_health():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM raw_orders;")
        count = cursor.fetchone()[0]
        print(f"✅ Database Storage: {count} events stored in Bronze raw_orders.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Database Check Failed: {e}")

if __name__ == "__main__":
    print("--- Full Pipeline Diagnostic ---")
    check_kafka_health()
    check_database_health()
    print("--------------------------------")