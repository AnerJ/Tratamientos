from pyspark.sql.functions import lower, regexp_replace, col, explode, split

# Lista de personajes importantes de la pregunta 1
important_names = important_characters.select("word").withColumnRenamed("word", "name")

# Capítulos que contienen a Frodo
tokens = df.filter(lower(col("text")).contains("frodo")) \
    .select("chapter_id", explode(split(col("text"), r"\s+")).alias("word"))

# Limpiar puntuación de palabras
tokens = tokens.withColumn("word", regexp_replace("word", r"[^\w]", ""))

# Filtrar solo nombres importantes, y excluir Frodo del resultado
character_near_frodo = tokens.join(
    important_names, tokens.word == important_names.name, "inner"
).filter(col("word") != "Frodo") \
 .groupBy("word").count().orderBy("count", ascending=False)

# Mostrar resultados
character_near_frodo.show()

# Guardar en HDFS
character_near_frodo.write.mode("overwrite").option("header", True).csv("hdfs:///user/ec2-user/frodo_near_characters")
