# preprocess.py
#
# The second part of using Statcast data for BaseballWizard.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a Spark session
spark = SparkSession.builder \
	.appName("Statcast Model") \
	.config("spark.sql.shuffle.partitions", "200") \
	.getOrCreate()

spark.conf.set("spark.sql.debug.maxToStringFields", 150)


# Load data
df = spark.read.parquet("statcast_data_2023.parquet").withColumn("game_date", col("game_date").cast("string"))
df.createOrReplaceTempView("statcast")


# Aggregate the data for each game. Statcast is done on a pitch-by-pitch basis, so this should make the data closer to "game-level".
game_stats = spark.sql("""
SELECT
	game_date,
	home_team,
	away_team,
	SUM(CASE WHEN inning_topbot = 'Bot' THEN 1 ELSE 0 END) AS home_hits,
	SUM(CASE WHEN inning_topbot = 'Top' THEN 1 ELSE 0 END) AS away_hits,
	MAX(home_score) AS home_score,
	MAX(away_score) AS away_score,
	COUNT(*) AS total_pitches,
	SUM(CASE WHEN events = 'strikeout' AND inning_topbot = 'Bot' THEN 1 ELSE 0 END) AS home_strikeouts,
	SUM(CASE WHEN events = 'strikeout' AND inning_topbot = 'Top' THEN 1 ELSE 0 END) AS away_strikeouts
FROM statcast
GROUP BY game_date, home_team, away_team
""")


# Create a label column for use in supervised learning. The column will be called 'home_win'.
game_stats = game_stats.withColumn("home_win", (game_stats["home_score"] > game_stats["away_score"]).cast("int"))


# May add rolling averages, so that the previous five games (for example) would affect the weight of each game.



# Train the model

from pyspark.ml.feature import StringIndexer, VectorAssembler

# Encoding for team names
indexer = StringIndexer(inputCols=["home_team", "away_team"],
			outputCols=["home_team_index", "away_team_index"])

game_stats = indexer.fit(game_stats).transform(game_stats)


# Assemble features
feature_cols = ["home_team_index", "away_team_index", "home_avg_hits",
		"away_avg_hits", "home_avg_strikeouts", "away_avg_strikeouts"]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

dataset = assembler.transform(game_stats).select("features", "home_win")


# Split the dataset into two sets, for training and testing, respectively.
train, test = dataset.randomSplit([0.8, 0.2], seed=42)

# Train a parallelized classifer
from pyspark.ml.classification import RandomForestClassifier

rf = RandomForestClassifier(featuresCol="features", labelCol="home_win", numTrees=100)
model = rf.fit(train)


# Evaluate model
from pyspark.sql.evaluation import MulticlassClassificationEvaluator

predictions = model.transform(test)
evaluator = MulticlassClassificationEvaluator(labelCol="home_win", metricName="accuracy")

accuracy = evaluator.evaluate(predictions)
print(f"Model Accuracy: {accuracy}")

# Describe what influenced the accuracy the most
print(f"Feature Importances: {model.featureImportances}")
