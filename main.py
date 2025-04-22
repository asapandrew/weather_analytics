from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, avg, year, month, sum


spark = SparkSession.builder.appName("Read CSV Weather data").getOrCreate()

df = spark.read.csv("weather_data.csv", header=True, inferSchema=True, sep=",")

# Finding missing values
print("Missing values:")
for column in df.columns:
    df.select(col(column)).filter(col(column).isNull()).count()
    print(f"{column}: {df.filter(col(column).isNull()).count()}")

# Top-5 most hot days
df_highest_temp = df["date", "temperature"].orderBy(df.temperature.desc()).limit(5)

print("Топ-5 самых жарких дней за все время наблюдений:")

df_highest_temp.show()

# Meteostation with most precipitation in last year
max_date = df.agg(max("date").alias("max_date")).first()[0]
last_year = max_date.year

df_meteostation = df.filter(year("date") == last_year)

df_meteostation = df_meteostation.groupBy("station_id").agg(
    sum("precipitation").alias("precipitation_sum")
)

df_meteostation = df_meteostation.orderBy(
    df_meteostation.precipitation_sum.desc()
).limit(1)

df_meteostation.show()

# Average monthly temperature

df_month_temp = df.withColumn("month", month("date"))

df_month_temp = (
    df_month_temp["month", "temperature"]
    .groupBy("month")
    .agg(avg("temperature").alias("average_temp"))
)

df_month_temp = df_month_temp.orderBy(df_month_temp.month.asc())
df_month_temp.show(12)

spark.stop()
