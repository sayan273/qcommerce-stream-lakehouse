import os
import duckdb

LAKEHOUSE_PATH = "lakehouse_storage/bronze_orders/*/*/*.parquet"

def run_duckdb_lakehouse_queries():
    print("--- Initializing DuckDB In-Memory Lakehouse Engine ---\n")
    
    con = duckdb.connect(database=':memory:')

    # Verify if parquet data exists
    if not os.path.exists("lakehouse_storage"):
        print("⚠️ No lakehouse data found. Run 'make archive-lake' first.")
        return

    # Create an in-memory view pointing to partitioned Parquet files
    con.execute(f"""
        CREATE VIEW lakehouse_orders AS 
        SELECT * FROM read_parquet('{LAKEHOUSE_PATH}', hive_partitioning=true);
    """)

    print("📊 [Query 1] GMV, Order Volume & Failure Rate by Regional Hub:")
    query_1 = """
        SELECT 
            city,
            COUNT(*) AS total_orders,
            ROUND(SUM(amount), 2) AS total_gmv_inr,
            ROUND(AVG(amount), 2) AS avg_ticket_size,
            ROUND(COUNT(CASE WHEN status = 'FAILED' THEN 1 END) * 100.0 / COUNT(*), 2) AS failure_rate_pct
        FROM lakehouse_orders
        GROUP BY city
        ORDER BY total_gmv_inr DESC;
    """
    print(con.execute(query_1).df().to_string(index=False))
    print("\n" + "-"*60 + "\n")

    print("💳 [Query 2] Payment Channel Distribution & Realized Conversion:")
    query_2 = """
        SELECT 
            payment_mode,
            COUNT(*) AS transaction_volume,
            ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END), 2) AS settled_volume_inr
        FROM lakehouse_orders
        GROUP BY payment_mode
        ORDER BY transaction_volume DESC;
    """
    print(con.execute(query_2).df().to_string(index=False))
    print("\n------------------------------------------------------------")

if __name__ == "__main__":
    run_duckdb_lakehouse_queries()