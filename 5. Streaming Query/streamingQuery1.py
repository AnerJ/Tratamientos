from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql import SparkSession

# Spark sesion
spark = SparkSession.builder.appName("Query1 - Top Finance Countries").getOrCreate()

# Read data from kafka
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

# Parse JSOn fields and choose the values that we want
events = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Filter only events of the catgory "finance" 
finance = events.filter(col("category") == "finance")

# GRouo by country and filter
result = finance \
    .withWatermark("timestamp", "15 minutes") \
    .groupBy(window(col("timestamp"), "15 minutes"), col("location")) \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(10)

# Show result
query = result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
