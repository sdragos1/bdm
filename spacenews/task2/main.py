from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, regexp_replace, lower, col, count, desc, to_date

CLEAN_WORD_REGEX = "[^a-z0-9\\s']"
HDFS_URI = "hdfs://192.168.0.12:9000"

spark = SparkSession.builder.appName("SpaceNews - Task 2").master(
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

words_df = df.select(
    to_date(col("date"), "MMMM d, yyyy").alias("date"),
    explode(
        split(
            regexp_replace(lower(col("content")), CLEAN_WORD_REGEX, " "),
            "\\s+"
        )
    ).alias("word")
).filter((col("word") != "") & (col("word") != "'"))

word_counts_total = words_df.drop("date").groupBy("word") \
    .agg(count("*").alias("count")) \
    .orderBy(desc("count"))
word_counts_total.show(50, truncate=False)
word_counts_total.coalesce(1).write.mode("overwrite") \
    .csv(f"{HDFS_URI}/output/spacenews/content_word_count_total", header=False, sep=" ")

word_counts_per_day = words_df.groupBy("date", "word") \
    .agg(count("*").alias("count")) \
    .orderBy(col("date").desc(), desc("count"))
word_counts_per_day.show(50, truncate=False)
word_counts_per_day.coalesce(1).write.mode("overwrite") \
    .csv(f"{HDFS_URI}/output/spacenews/content_word_count_per_day", header=False, sep=" ")

spark.stop()