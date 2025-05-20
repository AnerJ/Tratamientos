from pyspark.sql.functions import explode, split, col

# Lista de palabras comunes (stopwords capitalizadas)
stopwords = ["The", "He", "His", "She", "It", "They", "You", "Your", "I", "We", "A", "An", "In", "Of", "On", "With", "As", "At", "To", "From", "And", "But", "That", "This", "For", "By", "Not", "So", "Then", "There"]

# Tokenizar texto en palabras
tokens = df.select(explode(split(col("text"), r"\s+")).alias("word"))

# Filtrar palabras capitalizadas simples que NO estén en stopwords
characters = tokens.filter(
    (col("word").rlike("^[A-Z][a-z]+$")) &
    (~col("word").isin(stopwords))
)

# Contar apariciones y filtrar por >250
important_characters = characters.groupBy("word").count().filter("count > 100").orderBy("count", ascending=False)
important_characters.show()
important_characters.write.mode("overwrite").option("header", True).csv("hdfs:///user/ec2-user/important_characters")



#Cambios a priori
#Limite de 100 y una lsita de palabras que seguro que no queremos
