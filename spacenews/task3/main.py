from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, to_date, round as spark_round, lit

HDFS_URI = "hdfs://192.168.0.12:9000"

spark = SparkSession.builder.appName("SpaceNews - Task 3").master(
    "spark://192.168.0.2:7077").getOrCreate()

df = spark.read.csv(f"{HDFS_URI}/data/spacenews.csv",
                    header=True,
                    sep=',',
                    inferSchema=True,
                    nullValue="",
                    quote='"',
                    escape='"',
                    multiLine=True,
                    ignoreLeadingWhiteSpace=False,
                    ignoreTrailingWhiteSpace=False
                    )

df = df.withColumn("date", to_date(col("date"), "MMMM d, yyyy")) \
    .select("date", "author") \
    .cache()

total_articles = df.count()

articles_percent_by_day = df.groupBy("date") \
    .agg(count("*").alias("article_count")) \
    .withColumn("% of articles", spark_round((col("article_count") / lit(total_articles)) * 100, 5)) \
    .orderBy(col("% of articles").desc()) \
    .drop("article_count")

articles_percent_by_day.show(50, truncate=False)
articles_percent_by_day.coalesce(1).write.mode("overwrite") \
    .csv(f"{HDFS_URI}/output/spacenews/articles_percent_by_day", header=False, sep=" ")

articles_percent_by_author = df.groupBy("author") \
    .agg(count("*").alias("article_count")) \
    .withColumn("% of articles", spark_round((col("article_count") / lit(total_articles)) * 100, 5)) \
    .orderBy(col("% of articles").desc()) \
    .drop("article_count")

articles_percent_by_day.show(50, truncate=False)
articles_percent_by_author.coalesce(1).write.mode("overwrite") \
    .csv(f"{HDFS_URI}/output/spacenews/articles_percent_by_author", header=False, sep=" ")

df.unpersist()
spark.stop()
