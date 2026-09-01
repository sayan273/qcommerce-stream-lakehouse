import os
import psycopg2
import pandas as pd
from datetime import datetime

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "warehouse",
    "user": "de_user",
    "password": "de_password"
}

LAKEHOUSE_DIR = "lakehouse_storage/bronze_orders"

def archive_to_lakehouse():
    os.makedirs(LAKEHOUSE_DIR, exist_ok=True)
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = """
            SELECT 
                order_id, 
                user_id, 
                city, 
                category, 
                amount, 
                payment_mode, 
                status, 
                event_timestamp,
                TO_CHAR(event_timestamp, 'YYYY-MM-DD') AS event_date
            FROM raw_orders;
        """
        
        print("📥 Fetching raw stream records from PostgreSQL for Lakehouse export...")
        df = pd.read_sql(query, conn)
        conn.close()

        if df.empty:
            print("⚠️ No records found to archive.")
            return

        print(f"📦 Writing {len(df)} records partitioned by 'event_date' and 'city' to Parquet format...")
        
        # Partitioned Parquet dump simulating an S3/GCS Bronze Lake tier
        df.to_parquet(
            LAKEHOUSE_DIR,
            partition_cols=["event_date", "city"],
            engine="pyarrow",
            compression="snappy",
            index=False
        )

        print(f"✅ Lakehouse Export Complete! Columnar files saved to: {LAKEHOUSE_DIR}/")

    except Exception as e:
        print(f"❌ Lakehouse Archival Failed: {e}")

if __name__ == "__main__":
    archive_to_lakehouse()