from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, monotonically_increasing_id


spark = SparkSession.builder.appName("Question5").getOrCreate()

df = spark.read.option("header", True).csv("file:///home/ec2-user/two_towers_chapters_unique.csv")


# Tokenize each word for posistion
tokenized = df.select("chapter_id", explode(split(col("text"), r"\s+")).alias("word")) \
              .withColumn("pos", monotonically_increasing_id())

# Filter only important names
named_tokens = tokenized.join(
    important_characters.withColumnRenamed("word", "name"),
    tokenized.word == col("name"),
    "inner"
).select("chapter_id", "word", "pos")

# Self-join in range of ±50 positions inside of the same chapter
pairs = named_tokens.alias("a").join(
    named_tokens.alias("b"),
    (col("a.chapter_id") == col("b.chapter_id")) &
    (col("a.pos") < col("b.pos")) &
    ((col("b.pos") - col("a.pos")) <= 50)
)

# Create pairs
character_pairs_window = pairs.select(
    col("a.word").alias("char1"),
    col("b.word").alias("char2")
).groupBy("char1", "char2").count().orderBy("count", ascending=False)

# Show 
character_pairs_window.show()

# Save in HDFS
character_pairs_window.write.mode("overwrite").option("header", True).csv("hdfs:///user/ec2-user/character_pairs_window")
