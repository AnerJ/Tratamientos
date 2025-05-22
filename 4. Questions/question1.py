from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

spark = SparkSession.builder.appName("Question1").getOrCreate()


df = spark.read.option("header", True).csv("file:///home/ec2-user/two_towers_chapters_unique.csv")



# List of useless word that could have capital letter
stopwords = ["The", "He", "His", "She", "It", "They", "You", "Your", "I", "We", "A", "An", "In", "Of", "On", "With", "As", "At", "To", "From", "And", "But", "That", "This", "For", "By", "Not", "So", "Then", "There"]

# Tokenize the works
tokens = df.select(explode(split(col("text"), r"\s+")).alias("word"))

# Filter the words that are not in the stopwords
characters = tokens.filter(
    (col("word").rlike("^[A-Z][a-z]+$")) &
    (~col("word").isin(stopwords))
)

# Words appear more that 100 times
important_characters = characters.groupBy("word").count().filter("count > 100").orderBy("count", ascending=False)
important_characters.show()
important_characters.write.mode("overwrite").option("header", True).csv("hdfs:///user/ec2-user/important_characters")

