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

JDBC_URL = "jdbc:postgresql://postgres:5432/ecommerce_analytics"
JDBC_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

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

def write_metrics(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Metrics batch {batch_id}: no records")
        return

    print(f"Metrics batch {batch_id}: writing batch")

    (
        batch_df.write
        .mode("append")
        .jdbc(JDBC_URL, "product_window_metrics", properties=JDBC_PROPERTIES)
    )

query = (
    windowed_metrics.writeStream
    .foreachBatch(write_metrics)
    .outputMode("update")
    .option("checkpointLocation", "/tmp/checkpoints/product-window-metrics")
    .start()
)

query.awaitTermination()