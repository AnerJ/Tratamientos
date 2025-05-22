from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType

# Spark session
spark = SparkSession.builder.appName("Query2 - Device Usage Trends").getOrCreate()

# Read from kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.31.7.172:9094") \
    .option("subscribe", "news_events") \
    .load()

# Schema updated
schema = StructType() \
    .add("user_id", StringType()) \
    .add("article_id", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("category", StringType()) \
    .add("location", StringType()) \
    .add("device_type", StringType()) \
    .add("session_id", StringType())

# Parse JSON and choose the wanted values 
events = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Filter only valid devices
valid_devices = ["mobile", "desktop", "tablet"]
filtered = events.filter(col("device_type").isin(valid_devices))

# Grupo the devices
device_counts = filtered.groupBy("device_type").count().orderBy(col("count").desc())

# Show the results
query = device_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()


