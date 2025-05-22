from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, approx_count_distinct
from pyspark.sql.types import StructType, StringType, TimestampType

spark = SparkSession.builder.appName("StreamingQuery3 - Active Sessions by Country").getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.31.7.172:9094") \
    .option("subscribe", "news_events") \
    .load()

schema = StructType() \
    .add("user_id", StringType()) \
    .add("article_id", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("category", StringType()) \
    .add("location", StringType()) \
    .add("device_type", StringType()) \
    .add("session_id", StringType())

events = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

result = events \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(window(col("timestamp"), "5 minutes"), col("location")) \
    .agg(approx_count_distinct("session_id").alias("active_sessions")) \
    .orderBy(col("active_sessions").desc())

query = result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
