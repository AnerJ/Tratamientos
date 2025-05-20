from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, TimestampType

# Leer datos desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.31.10.118:9094") \
    .option("subscribe", "news_events") \
    .load()

# Definir el esquema del JSON
schema = StructType() \
    .add("user_id", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("url", StringType()) \
    .add("device", StringType()) \
    .add("country", StringType())

# Parsear JSON y seleccionar campos
events = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Query 1: Top países accediendo a /finance en los últimos 15 minutos
finance = events.filter(col("url").contains("finance"))

result = finance \
    .withWatermark("timestamp", "15 minutes") \
    .groupBy(window(col("timestamp"), "15 minutes"), col("country")) \
    .count() \
    .orderBy(col("count").desc())

# Mostrar resultados en consola
query = result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
