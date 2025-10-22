from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower

HDFS_URI = "hdfs://192.168.0.12:9000"

spark = SparkSession.builder.appName("SpaceNews - Task 4").master(
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

df = df.select("url", "title", "postexcerpt").dropna() \
    .cache()

urls_with_keywords = df.filter(
    (lower(col("title")).contains("nasa") | lower(col("title")).contains("satellite"))
    &
    (lower(col("postexcerpt")).contains("nasa") | lower(col("postexcerpt")).contains("satellite"))
).select("url")

urls_with_keywords.coalesce(1).show(50, truncate=False)

urls_with_keywords.write.mode("overwrite") \
    .csv(f"{HDFS_URI}/output/spacenews_urls_nasa_satellite", header=False)


df.unpersist()
spark.stop()
