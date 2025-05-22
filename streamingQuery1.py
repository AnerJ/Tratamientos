from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Query1 - Top Finance Countries").getOrCreate()

# Leer datos desde Kafka
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

# Filtrar solo los eventos de categoría "finance"
finance = events.filter(col("category") == "finance")

# Agrupar por país y ventana de tiempo
result = finance \
    .withWatermark("timestamp", "15 minutes") \
    .groupBy(window(col("timestamp"), "15 minutes"), col("location")) \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(10)

# Mostrar resultados en consola
query = result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
