import psycopg2
from datetime import datetime

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "warehouse",
    "user": "de_user",
    "password": "de_password"
}

def mutate_user_dimension():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Simulate an upgrade event for an existing user
    upgrade_query = """
        UPDATE raw_users
        SET membership_tier = 'Platinum',
            updated_at = NOW()
        WHERE user_id = 'USR_1001';
    """
    cursor.execute(upgrade_query)
    conn.commit()
    print("✅ Mutated USR_1001: Upgraded 'Silver' -> 'Platinum' to test dbt snapshot SCD2.")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    mutate_user_dimension()