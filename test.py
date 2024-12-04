from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, to_date, dayofweek


spark = SparkSession.builder \
	.appName("Baseball Wizard") \
	.getOrCreate()

df = spark.read.csv("scripts/statcast/data/barebones.csv", header=True, inferSchema=True)

df = df.withColumn(
	"home_win",
	when(col("home_score") > col("away_score"), 1).otherwise(0)
)

df = df.withColumn("game_date", to_date(col("game_date"), "yyyy-MM-dd"))

df = df.withColumn("day_of_week", dayofweek(col("game_date")))
df.select("game_date", "day_of_week").show(5)


