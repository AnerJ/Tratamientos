from pyspark.sql import SparkSession
from pyspark.sql.functions import size, split, col, avg


spark = SparkSession.builder.appName("Question2").getOrCreate()

# Read file
df = spark.read.option("header", True).csv("file:///home/ec2-user/two_towers_chapters_unique.csv")

#Calulate words per chapter
df = df.withColumn("num_words", size(split(col("text"), r"\s+")))

# calculate avergae length for chapters
avg_length = df.select(avg(col("num_words")).alias("avg_chapter_length"))

# Show result
avg_length.show()
#Save in hdfs
output_df.write.option("header", True).csv("hdfs:///user/ec2-user/chapter_word_counts")
