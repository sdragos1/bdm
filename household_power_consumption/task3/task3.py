from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, col


class Columns:
    DATE = "Date"
    TIME = "Time"
    GLOBAL_ACTIVE_POWER = "Global_active_power"
    GLOBAL_REACTIVE_POWER = "Global_reactive_power"
    VOLTAGE = "Voltage"
    GLOBAL_INTENSITY = "Global_intensity"
    SUB_METERING_1 = "Sub_metering_1"
    SUB_METERING_2 = "Sub_metering_2"
    SUB_METERING_3 = "Sub_metering_3"


NUMERIC_COLUMNS = [
    Columns.GLOBAL_ACTIVE_POWER,
    Columns.GLOBAL_REACTIVE_POWER,
    Columns.VOLTAGE,
    Columns.GLOBAL_INTENSITY,
    Columns.SUB_METERING_1,
    Columns.SUB_METERING_2,
    Columns.SUB_METERING_3,
]
spark = SparkSession.builder.appName("House Power Consumption - Task 3").master(
    "spark://192.168.0.2:7077").getOrCreate()

df = spark.read.csv("hdfs://192.168.0.12:9000/data/household_power_consumption.txt",
                    header=True,
                    sep=';',
                    inferSchema=True,
                    nullValue="?"
                    )

df_clean = df.dropna(subset=NUMERIC_COLUMNS)

min_max_dict = {}
for col_name in NUMERIC_COLUMNS:
    min_val, max_val = df_clean.agg(min(col_name), max(col_name)).first()
    min_max_dict[col_name] = (min_val, max_val)

result_df = df_clean
for col_name in NUMERIC_COLUMNS:
    min_val, max_val = min_max_dict[col_name]
    result_df = result_df.withColumn(
        col_name,
        (col(col_name) - min_val) / (max_val - min_val)
    )

RESULT_PATH = "hdfs://192.168.0.12:9000/output/task3"

result_df.coalesce(1).write.csv(
    RESULT_PATH,
    header=True,
    mode="overwrite"
)

spark.read.csv(RESULT_PATH, header=True).show(50, truncate=False)

spark.stop()
