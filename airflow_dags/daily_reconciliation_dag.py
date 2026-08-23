from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import psycopg2

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def audit_raw_ingestion():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="warehouse",
        user="de_user",
        password="de_password"
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) 
        FROM raw_orders 
        WHERE ingested_at >= NOW() - INTERVAL '24 HOURS';
    """)
    record_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    
    print(f"Ingestion Audit: {record_count} events ingested in the last 24 hours.")
    if record_count == 0:
        print("Warning: Zero records ingested in the lookback window.")

with DAG(
    dag_id='qcommerce_daily_reconciliation',
    default_args=default_args,
    description='Automated batch reconciliation and dbt transformation mart execution',
    schedule_interval='@daily',
    catchup=False,
    tags=['qcommerce', 'etl', 'dbt']
) as dag:

    task_audit_ingestion = PythonOperator(
        task_id='audit_raw_ingestion',
        python_callable=audit_raw_ingestion
    )

    task_dbt_run = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt_transforms && dbt run --profiles-dir .'
    )

    task_dbt_test = BashOperator(
        task_id='test_dbt_data_quality',
        bash_command='cd /opt/airflow/dbt_transforms && dbt test --profiles-dir .'
    )

    task_audit_ingestion >> task_dbt_run >> task_dbt_test