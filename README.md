# E-Commerce Clickstream & Inventory Watch

Starter project for the Applied Big Data Engineering mini project.

## Stack
- Kafka for ingestion
- Spark Structured Streaming for real-time processing
- Airflow for daily orchestration
- PostgreSQL for storage
- Spring Boot for APIs and report access

## High-level flow
Python Producer -> Kafka -> Spark -> PostgreSQL -> Airflow -> Spring Boot API

## PostgreSQL tables used
- `products`
- `clickstream_events`
- `product_window_metrics`
- `flash_sale_alerts`
- `daily_user_segments`
- `daily_top_products`
- `daily_conversion_report`

## Folders
- `producer/` synthetic clickstream generator
- `spark-jobs/` streaming jobs that write raw events, window metrics, and flash-sale alerts to PostgreSQL
- `airflow/dags/` daily batch and report orchestration from PostgreSQL
- `springboot-app/` REST API for products, alerts, and reports
- `infra/` PostgreSQL initialization scripts
- `reports/` generated reports and screenshots

## Run order
1. Start Docker Compose
2. Run Spring Boot
3. Run the clickstream producer
4. Run the raw-events Spark job inside the Spark container
5. Run the metrics-and-alerts Spark job inside the Spark container
6. Trigger the Airflow DAG

## Important note
This starter now uses **PostgreSQL end-to-end**:
- Spring Boot reads from PostgreSQL
- Spark writes raw clickstream events, sliding-window product metrics, and flash-sale alerts into PostgreSQL
- Airflow reads PostgreSQL, computes daily reports, and writes results back into PostgreSQL
