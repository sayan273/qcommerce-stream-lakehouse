import psycopg2
import pandas as pd

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "warehouse",
    "user": "de_user",
    "password": "de_password"
}

def generate_business_report():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        
        query = """
            SELECT 
                city,
                payment_mode,
                total_transactions,
                success_rate_pct,
                realized_revenue_inr,
                lost_revenue_inr
            FROM analytics.fct_city_payment_performance
            ORDER BY realized_revenue_inr DESC;
        """
        
        df = pd.read_sql(query, conn)
        
        print("\n=======================================================")
        print("          ⚡ QUICK-COMMERCE EXECUTIVE REPORT ⚡         ")
        print("=======================================================\n")
        
        if df.empty:
            print("No aggregated data found. Ensure dbt run has executed.")
        else:
            print(df.to_string(index=False))
            
            top_city = df.groupby('city')['realized_revenue_inr'].sum().idxmax()
            total_gmv = df['realized_revenue_inr'].sum()
            total_lost = df['lost_revenue_inr'].sum()
            
            print("\n------------------- Key Takeaways ---------------------")
            print(f"🏆 Top Performing Hub: {top_city}")
            print(f"💰 Total Realized GMV: ₹{total_gmv:,.2f}")
            print(f"⚠️ Revenue Attrition (Failed Txns): ₹{total_lost:,.2f}")
            print("-------------------------------------------------------\n")
            
        conn.close()
    except Exception as e:
        print(f"Report Generation Failed: {e}")

if __name__ == "__main__":
    generate_business_report()