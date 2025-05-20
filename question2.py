from pyspark.sql.functions import size, split

# Calcular número de palabras por capítulo
chapter_word_count = df.withColumn("num_words", size(split(col("text"), r"\s+")))

# Mostrar capítulos más largos
chapter_word_count.select("chapter_id", "chapter_title", "num_words") \
    .orderBy("num_words", ascending=False).show()

# Guardar en HDFS en una línea separada
output_df = chapter_word_count.select("chapter_id", "chapter_title", "num_words")

output_df.write.option("header", True).csv("hdfs:///user/ec2-user/chapter_word_counts")
