.PHONY: help build up down stream consume dbt-run dbt-test check clean

help:
	@echo "Available commands:"
	@echo "  make up          - Start all Docker infrastructure (Kafka, Postgres, Airflow)"
	@echo "  make down        - Stop and remove Docker containers"
	@echo "  make stream      - Launch the real-time event producer"
	@echo "  make consume     - Launch the real-time stream consumer processor"
	@echo "  make dbt-run     - Execute dbt staging and mart transformations"
	@echo "  make dbt-test    - Execute dbt data quality test suites"
	@echo "  make check       - Run pipeline health and observability diagnostics"

up:
	docker-compose up -d

down:
	docker-compose down

stream:
	python data_producer/producer.py

consume:
	python spark_streaming/stream_processor.py

dbt-run:
	cd dbt_transforms && dbt run --profiles-dir .

dbt-test:
	cd dbt_transforms && dbt test --profiles-dir .

check:
	python scripts/pipeline_health_check.py

clean:
	docker-compose down -v
	
report:
	python scripts/analytics_reporting.py

lint:
	flake8 . --count --max-line-length=127 --exclude=venv,.venv

alert-test:
	python scripts/pipeline_notifier.py

db-clean:
	docker exec -i qcommerce-stream-lakehouse-postgres-1 psql -U de_user -d warehouse < scripts/maintenance_cleanup.sql

archive-lake:
	python scripts/lakehouse_archiver.py

duckdb-query:
	python scripts/duckdb_lakehouse_analytics.py

monitor:
	python scripts/kafka_stream_monitor.py

dashboard:
	streamlit run src/dashboard/app.py