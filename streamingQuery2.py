from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Query2 - Device Usage Trends").getOrCreate()

# Leer desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.31.7.172:9094") \
    .option("subscribe", "news_events") \
    .load()

# Esquema actualizado
schema = StructType() \
    .add("user_id", StringType()) \
    .add("article_id", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("category", StringType()) \
    .add("location", StringType()) \
    .add("device_type", StringType()) \
    .add("session_id", StringType())

# Parsear JSON y seleccionar campos
events = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Filtrar solo los dispositivos válidos
valid_devices = ["mobile", "desktop", "tablet"]
filtered = events.filter(col("device_type").isin(valid_devices))

# Agrupar por tipo de dispositivo
device_counts = filtered.groupBy("device_type").count().orderBy(col("count").desc())

# Mostrar resultados en consola
query = device_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()


#FUNCIONA JODER