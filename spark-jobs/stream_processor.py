import os
from typing import Iterable

import psycopg2
from psycopg2.extras import execute_batch
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, to_timestamp, window, when, lit
from pyspark.sql.types import StructField, StructType, StringType

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "ecommerce_analytics")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/ecommerce-checkpoints")

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
])


def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def write_events_batch(batch_df, epoch_id):
    rows = batch_df.select(
        "event_id", "user_id", "product_id", "category", "event_type", "event_time"
    ).collect()
    if not rows:
        return

    payload = [
        (
            row["event_id"],
            row["user_id"],
            row["product_id"],
            row["category"],
            row["event_type"],
            row["event_time"],
        )
        for row in rows
    ]

    sql = """
        INSERT INTO clickstream_events (event_id, user_id, product_id, category, event_type, event_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
    """

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            execute_batch(cursor, sql, payload, page_size=500)
        conn.commit()
        print(f"Epoch {epoch_id}: wrote {len(payload)} clickstream events to PostgreSQL")
    finally:
        conn.close()



def write_metrics_batch(batch_df, epoch_id):
    rows = batch_df.select(
        "product_id", "window_start", "window_end", "views", "add_to_cart_count", "purchases"
    ).collect()
    if not rows:
        return

    payload = [
        (
            row["product_id"],
            row["window_start"],
            row["window_end"],
            int(row["views"] or 0),
            int(row["add_to_cart_count"] or 0),
            int(row["purchases"] or 0),
        )
        for row in rows
    ]

    sql = """
        INSERT INTO product_window_metrics (
            product_id, window_start, window_end, views, add_to_cart_count, purchases
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id, window_start, window_end)
        DO UPDATE SET
            views = EXCLUDED.views,
            add_to_cart_count = EXCLUDED.add_to_cart_count,
            purchases = EXCLUDED.purchases
    """

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            execute_batch(cursor, sql, payload, page_size=500)
        conn.commit()
        print(f"Epoch {epoch_id}: upserted {len(payload)} window metrics into PostgreSQL")
    finally:
        conn.close()



def write_alerts_batch(batch_df, epoch_id):
    rows = batch_df.select(
        "product_id", "window_start", "window_end", "views", "purchases", "recommendation"
    ).collect()
    if not rows:
        return

    payload = [
        (
            row["product_id"],
            row["window_start"],
            row["window_end"],
            int(row["views"] or 0),
            int(row["purchases"] or 0),
            row["recommendation"],
        )
        for row in rows
    ]

    sql = """
        INSERT INTO flash_sale_alerts (
            product_id, window_start, window_end, views, purchases, recommendation
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id, window_start, window_end, recommendation)
        DO UPDATE SET
            views = EXCLUDED.views,
            purchases = EXCLUDED.purchases
    """

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            execute_batch(cursor, sql, payload, page_size=500)
        conn.commit()
        print(f"Epoch {epoch_id}: upserted {len(payload)} flash-sale alerts into PostgreSQL")
    finally:
        conn.close()


spark = (
    SparkSession.builder
    .appName("EcommerceClickstreamProcessor")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", "click-events")
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
    .filter(col("event_id").isNotNull() & col("product_id").isNotNull() & col("event_time").isNotNull())
)

windowed_metrics = (
    parsed_df
    .withWatermark("event_time", "15 minutes")
    .groupBy(
        window(col("event_time"), "10 minutes", "5 minutes"),
        col("product_id")
    )
    .agg(
        count(when(col("event_type") == "view", True)).alias("views"),
        count(when(col("event_type") == "add_to_cart", True)).alias("add_to_cart_count"),
        count(when(col("event_type") == "purchase", True)).alias("purchases")
    )
    .select(
        col("product_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("views"),
        col("add_to_cart_count"),
        col("purchases")
    )
)

alerts_df = (
    windowed_metrics
    .filter((col("views") > 100) & (col("purchases") < 5))
    .withColumn("recommendation", lit("Flash Sale Suggested"))
)

# Raw events are stored in PostgreSQL for Airflow batch analytics.
events_query = (
    parsed_df.writeStream
    .foreachBatch(write_events_batch)
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/events")
    .start()
)

# Window metrics are continuously upserted into PostgreSQL.
metrics_query = (
    windowed_metrics.writeStream
    .foreachBatch(write_metrics_batch)
    .outputMode("update")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/window-metrics")
    .start()
)

# Alerts are continuously upserted into PostgreSQL.
alerts_query = (
    alerts_df.writeStream
    .foreachBatch(write_alerts_batch)
    .outputMode("update")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/flash-sale-alerts")
    .start()
)

spark.streams.awaitAnyTermination()
