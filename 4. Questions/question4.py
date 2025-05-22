from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, length


spark = SparkSession.builder.appName("Question4").getOrCreate()

df = spark.read.option("header", True).csv("file:///home/ec2-user/two_towers_chapters_unique.csv")


# Tokenize the words
tokens = df.select("chapter_id", "chapter_title", explode(split(col("text"), r"\s+")).alias("word"))

# Filters longest words (13 characters)
long_words = tokens.filter(length(col("word")) > 13)

# Count per chapter
long_word_counts = long_words.groupBy("chapter_id", "chapter_title").count().orderBy("count", ascending=False)

# Show result
long_word_counts.show()

# Save in hdfs
long_word_counts.write.mode("overwrite").option("header", True).csv("hdfs:///user/ec2-user/long_words_by_chapter")
