import streamlit as st
import pandas as pd
import psycopg2
import duckdb
import os

st.set_page_config(
    page_title="Quick-Commerce Telemetry Dashboard",
    page_icon="⚡",
    layout="wide"
)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "warehouse",
    "user": "de_user",
    "password": "de_password"
}

@st.cache_data(ttl=5)
def fetch_live_metrics():
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
            FROM analytics.fct_city_payment_performance;
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error connecting to PostgreSQL warehouse: {e}")
        return pd.DataFrame()

st.title("⚡ Quick-Commerce Real-Time Operations & Telemetry")
st.markdown("Live streaming and dimensional analytics powered by **Kafka, PostgreSQL, dbt, and DuckDB**.")

df_metrics = fetch_live_metrics()

if not df_metrics.empty:
    total_revenue = df_metrics['realized_revenue_inr'].sum()
    total_txns = df_metrics['total_transactions'].sum()
    total_lost = df_metrics['lost_revenue_inr'].sum()
    avg_success_rate = df_metrics['success_rate_pct'].mean()

    # Top KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Realized GMV", f"₹{total_revenue:,.2f}")
    col2.metric("Total Transactions", f"{total_txns:,}")
    col3.metric("Avg Success Rate", f"{avg_success_rate:.1f}%")
    col4.metric("Revenue Attrition", f"₹{total_lost:,.2f}", delta_color="inverse")

    st.divider()

    # Visualizations
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Regional GMV Contribution")
        city_rev = df_metrics.groupby("city")["realized_revenue_inr"].sum().reset_index()
        st.bar_chart(city_rev.set_index("city"))

    with col_right:
        st.subheader("Transaction Volume by Payment Mode")
        pay_vol = df_metrics.groupby("payment_mode")["total_transactions"].sum().reset_index()
        st.bar_chart(pay_vol.set_index("payment_mode"))

    st.subheader("Detailed Conversion & Settlement Table")
    st.dataframe(df_metrics, use_container_width=True)

else:
    st.info("No aggregated records found in the analytics schema. Ensure `make dbt-run` has executed.")

# DuckDB Parquet Cold Storage Query Section
st.divider()
st.subheader("🔍 Cold Lakehouse Parquet Inspection (DuckDB)")
lake_path = "lakehouse_storage/bronze_orders/*/*/*.parquet"

if os.path.exists("lakehouse_storage"):
    if st.button("Run Ad-Hoc DuckDB Query on Parquet Tier"):
        con = duckdb.connect(database=':memory:')
        res = con.execute(f"SELECT city, category, COUNT(*) as volume, AVG(amount) as avg_price FROM read_parquet('{lake_path}', hive_partitioning=true) GROUP BY city, category LIMIT 10;").df()
        st.dataframe(res, use_container_width=True)
else:
    st.caption("Lakehouse archive not initialized yet. Execute `make archive-lake` to generate partitioned Parquet datasets.")