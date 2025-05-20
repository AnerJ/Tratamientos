from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, TimestampType

# Define schema of incoming Kafka message
schema = StructType() \
    .add("user_id", StringType()) \
    .add("article_id", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("category", StringType()) \
    .add("location", StringType()) \
    .add("device_type", StringType()) \
    .add("session_id", StringType())

# Start Spark session
spark = SparkSession.builder \
    .appName("ActiveSessionsByCountry") \
    .getOrCreate()

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \  # change to event-generator-private-ip:9094 for AWS
    .option("subscribe", "news_events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON payload
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Use watermark to allow late data up to 5 minutes
# Group by window and location, count distinct session_ids
df_sessions = df_parsed \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("location")
    ).agg(
        # Approx count distinct for better performance on large scale
        # .agg(countDistinct("session_id").alias("active_sessions"))
        {"session_id": "countDistinct"}
    ).withColumnRenamed("count(DISTINCT session_id)", "active_sessions")

# Output to console
query = df_sessions.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
