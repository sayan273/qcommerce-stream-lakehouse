# Real-Time Quick-Commerce Event Streaming & Analytics Lakehouse

An end-to-end event-driven data pipeline designed to ingest, process, and model high-throughput e-commerce transaction events in real time.

## Architecture
1. **Producer**: Python synthetic event generator simulating real-time order streams with Indian demographic distributions.
2. **Message Broker**: Redpanda / Apache Kafka for distributed event streaming.
3. **Stream Processing**: PySpark Structured Streaming for real-time window aggregations and SLA tracking.
4. **Data Warehouse & Modeling**: PostgreSQL + dbt for building dimensional star-schema models (Fact & Dimension marts).
5. **Orchestration**: Apache Airflow DAGs for automated daily reconciliations and data quality assertions.

## Setup & Execution
```bash
docker-compose up -d
pip install -r requirements.txt
python data_producer/producer.py