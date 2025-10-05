from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Household Power Consumption").master("spark://192.168.0.2:7077").getOrCreate()

df = spark.read.csv("hdfs://192.168.0.12:9000/data/household_power_consumption.txt", header=True, sep=';', inferSchema=True)

df.show(5)

spark.stop()
