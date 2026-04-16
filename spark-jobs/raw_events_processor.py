import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
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
CHECKPOINT_LOCATION = os.getenv("RAW_EVENTS_CHECKPOINT_DIR", "./checkpoints/raw-events")

spark = (
    SparkSession.builder
    .appName("EcommerceClickstreamRawWriter")
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

events_to_store = (
    parsed_df.select(
        col("event_id"),
        col("user_id"),
        col("product_id"),
        col("event_type"),
        col("event_time")
    )
    .dropna(subset=["event_id", "user_id", "product_id", "event_type", "event_time"])
    .dropDuplicates(["event_id"])
)


def get_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)


def write_raw_events(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id}: no records")
        return

    rows = [
        (
            row.event_id,
            row.user_id,
            row.product_id,
            row.event_type,
            row.event_time,
        )
        for row in batch_df.collect()
    ]
    print(f"Batch {batch_id}: writing {len(rows)} records to clickstream_events")

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO clickstream_events (
                    event_id, user_id, product_id, event_type, event_time
                )
                VALUES %s
                ON CONFLICT (event_id) DO NOTHING
                """,
                rows,
            )
        conn.commit()
    finally:
        conn.close()


query = (
    events_to_store.writeStream
    .foreachBatch(write_raw_events)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .start()
)

query.awaitTermination()
