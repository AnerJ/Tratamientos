from pyspark.sql.functions import explode, split, length

# Tokenizar palabras por capítulo
tokens = df.select("chapter_id", "chapter_title", explode(split(col("text"), r"\s+")).alias("word"))

# Filtrar palabras largas (más de 13 caracteres)
long_words = tokens.filter(length(col("word")) > 13)

# Contar por capítulo
long_word_counts = long_words.groupBy("chapter_id", "chapter_title").count().orderBy("count", ascending=False)

# Mostrar resultados
long_word_counts.show()

# Guardar en HDFS
long_word_counts.write.mode("overwrite").option("header", True).csv("hdfs:///user/ec2-user/long_words_by_chapter")
