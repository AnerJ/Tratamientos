from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StringType, TimestampType

# Crear sesión de Spark
spark = SparkSession.builder.appName("StreamingQuery5 - Suspicious Behavior Detection").getOrCreate()

# Leer desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.31.7.172:9094") \
    .option("subscribe", "news_events") \
    .load()

# Definir esquema del JSON
schema = StructType() \
    .add("user_id", StringType()) \
    .add("article_id", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("category", StringType()) \
    .add("location", StringType()) \
    .add("device_type", StringType()) \
    .add("session_id", StringType())

# Parsear los eventos
events = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Detección de comportamiento sospechoso: > 20 clics por minuto por usuario
suspicious_users = events \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window(col("timestamp"), "1 minute"), col("user_id")) \
    .agg(count("article_id").alias("click_count")) \
    .filter(col("click_count") > 20) \
    .orderBy(col("click_count").desc())

# Mostrar en consola
query = suspicious_users.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
