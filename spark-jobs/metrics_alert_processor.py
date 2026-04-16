import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, to_timestamp, when, window
from pyspark.sql.types import StructField, StructType, StringType

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
])

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "click-events")
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5433")),
    "dbname": os.getenv("POSTGRES_DB", "ecommerce_analytics"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}
CHECKPOINT_LOCATION = os.getenv("METRICS_ALERTS_CHECKPOINT_DIR", "./checkpoints/metrics-alerts")
FLASH_SALE_VIEW_THRESHOLD = int(os.getenv("FLASH_SALE_VIEW_THRESHOLD", "100"))
FLASH_SALE_PURCHASE_THRESHOLD = int(os.getenv("FLASH_SALE_PURCHASE_THRESHOLD", "5"))
FLASH_SALE_RECOMMENDATION = os.getenv("FLASH_SALE_RECOMMENDATION", "Flash Sale Suggested")

spark = (
    SparkSession.builder
    .appName("EcommerceMetricsProcessor")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
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


def get_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)


def write_metrics_and_alerts(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"Metrics batch {batch_id}: no records")
        return

    rows = list(batch_df.collect())
    metric_keys = [(row.product_id, row.window_start, row.window_end) for row in rows]
    metric_values = [
        (
            row.product_id,
            row.window_start,
            row.window_end,
            row.views,
            row.add_to_cart_count,
            row.purchases,
        )
        for row in rows
    ]
    alert_values = [
        (
            row.product_id,
            row.window_start,
            row.window_end,
            row.views,
            row.purchases,
            FLASH_SALE_RECOMMENDATION,
        )
        for row in rows
        if row.views > FLASH_SALE_VIEW_THRESHOLD and row.purchases < FLASH_SALE_PURCHASE_THRESHOLD
    ]

    print(
        f"Metrics batch {batch_id}: upserting {len(metric_values)} windows and {len(alert_values)} flash-sale alerts"
    )

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            execute_values(
                cursor,
                """
                DELETE FROM product_window_metrics AS metrics
                USING (VALUES %s) AS incoming(product_id, window_start, window_end)
                WHERE metrics.product_id = incoming.product_id
                  AND metrics.window_start = incoming.window_start
                  AND metrics.window_end = incoming.window_end
                """,
                metric_keys,
            )
            execute_values(
                cursor,
                """
                INSERT INTO product_window_metrics (
                    product_id, window_start, window_end, views, add_to_cart_count, purchases
                )
                VALUES %s
                """,
                metric_values,
            )
            execute_values(
                cursor,
                """
                DELETE FROM flash_sale_alerts AS alerts
                USING (VALUES %s) AS incoming(product_id, window_start, window_end)
                WHERE alerts.product_id = incoming.product_id
                  AND alerts.window_start = incoming.window_start
                  AND alerts.window_end = incoming.window_end
                """,
                metric_keys,
            )
            if alert_values:
                execute_values(
                    cursor,
                    """
                    INSERT INTO flash_sale_alerts (
                        product_id, window_start, window_end, views, purchases, recommendation
                    )
                    VALUES %s
                    """,
                    alert_values,
                )
        conn.commit()
    finally:
        conn.close()


query = (
    windowed_metrics.writeStream
    .foreachBatch(write_metrics_and_alerts)
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .start()
)

query.awaitTermination()
