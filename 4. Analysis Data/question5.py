from pyspark.sql.functions import explode, split, col, monotonically_increasing_id

# Tokenizar todas las palabras con posición
tokenized = df.select("chapter_id", explode(split(col("text"), r"\s+")).alias("word")) \
              .withColumn("pos", monotonically_increasing_id())

# Filtrar solo nombres importantes
named_tokens = tokenized.join(
    important_characters.withColumnRenamed("word", "name"),
    tokenized.word == col("name"),
    "inner"
).select("chapter_id", "word", "pos")

# Self-join en ventana de ±50 posiciones dentro del mismo capítulo
pairs = named_tokens.alias("a").join(
    named_tokens.alias("b"),
    (col("a.chapter_id") == col("b.chapter_id")) &
    (col("a.pos") < col("b.pos")) &
    ((col("b.pos") - col("a.pos")) <= 50)
)

# Crear pares ordenados
character_pairs_window = pairs.select(
    col("a.word").alias("char1"),
    col("b.word").alias("char2")
).groupBy("char1", "char2").count().orderBy("count", ascending=False)

# Mostrar
character_pairs_window.show()

# Guardar en HDFS
character_pairs_window.write.mode("overwrite").option("header", True).csv("hdfs:///user/ec2-user/character_pairs_window")
