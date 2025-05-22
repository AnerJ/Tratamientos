from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StringType, TimestampType

# Crear la sesión de Spark
spark = SparkSession.builder.appName("StreamingQuery4 - Breaking News Spike Detection").getOrCreate()

# Leer desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.31.7.172:9094") \
    .option("subscribe", "news_events") \
    .load()

# Esquema del JSON
schema = StructType() \
    .add("user_id", StringType()) \
    .add("article_id", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("category", StringType()) \
    .add("location", StringType()) \
    .add("device_type", StringType()) \
    .add("session_id", StringType())

# Parsear el JSON
events = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Agrupar por artículo y ventana de 10 minutos, contar vistas y filtrar picos (>250)
result = events \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window(col("timestamp"), "10 minutes"), col("article_id")) \
    .agg(count("*").alias("view_count")) \
    .filter(col("view_count") > 1) \
    .orderBy(col("view_count").desc())

# Mostrar los resultados por consola
query = result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
