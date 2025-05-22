from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, regexp_replace, col, explode, split

spark = SparkSession.builder.appName("Question3").getOrCreate()

df = spark.read.option("header", True).csv("file:///home/ec2-user/two_towers_chapters_unique.csv")


# List of imporant characters of the question numer 1
important_names = important_characters.select("word").withColumnRenamed("word", "name")

# Chapters that contain the work Frodo
tokens = df.filter(lower(col("text")).contains("frodo")) \
    .select("chapter_id", explode(split(col("text"), r"\s+")).alias("word"))

# Clean punctuation
tokens = tokens.withColumn("word", regexp_replace("word", r"[^\w]", ""))

# Filter only important names
character_near_frodo = tokens.join(
    important_names, tokens.word == important_names.name, "inner"
).filter(col("word") != "Frodo") \
 .groupBy("word").count().orderBy("count", ascending=False)

# Show result
character_near_frodo.show()

# Save in HDFS
character_near_frodo.write.mode("overwrite").option("header", True).csv("hdfs:///user/ec2-user/frodo_near_characters")
