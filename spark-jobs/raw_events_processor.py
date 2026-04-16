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

JDBC_URL = "jdbc:postgresql://postgres:5432/ecommerce_analytics"
JDBC_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

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
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "click-events")
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

def write_raw_events(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id}: no records")
        return

    count = batch_df.count()
    print(f"Batch {batch_id}: writing {count} records to clickstream_events")

    (
        batch_df.write
        .mode("append")
        .jdbc(JDBC_URL, "clickstream_events", properties=JDBC_PROPERTIES)
    )

query = (
    events_to_store.writeStream
    .foreachBatch(write_raw_events)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/raw-events")
    .start()
)

query.awaitTermination()