# train.py
#
# This script will process data, gathered by the scraping scripts, and train a model based on said data.
# The model splits the data in half, trains on the first half, and tests itself with the second half.
# Following the tests, you should see how accurate the model was, and how each feature affected its predictions.


from pyspark.sql import SparkSession, functions
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator



# Initialize a Spark session
spark = SparkSession.builder \
	.appName("BaseballWizard") \
	.config("spark.sql.shuffle.partitions", "200") \
	.getOrCreate()


# Load data

# Cast the "game_date" column to string, so that PySpark can handle it
df = spark.read.parquet("statcast/data/statcast_data_2024.parquet").withColumn("game_date", functions.col("game_date").cast("string"))
df.createOrReplaceTempView("statcast")


# Calculate home team hits
home_team_stats = spark.sql("""
SELECT
	game_date,
	home_team,
	SUM(CASE WHEN inning_topbot = 'Bot' THEN 1 ELSE 0 END) AS home_hits,
	AVG(release_speed) AS home_avg_pitch_speed,
	AVG(release_spin_rate) AS home_avg_spin_rate
FROM STATCAST
GROUP BY game_date, home_team
""").alias("home_team_stats")


# Calculate away team hits
away_team_stats = spark.sql("""
SELECT
	game_date,
	away_team,
	SUM(CASE WHEN inning_topbot = 'Top' THEN 1 ELSE 0 END) as away_hits,
	AVG(release_speed) AS away_avg_pitch_speed,
	AVG(release_spin_rate) AS away_avg_spin_rate
FROM statcast
GROUP BY game_date, away_team
""").alias("away_team_stats")


# Rename columns, so that there aren't duplicate column names/ambiguous column names
home_team_stats = home_team_stats.withColumnRenamed("home_team", "home_team_stat") \
	.withColumnRenamed("home_hits", "home_hits_stat")

away_team_stats = away_team_stats.withColumnRenamed("away_team", "away_team_stat") \
	.withColumnRenamed("away_hits", "away_hits_stat")

# Aggregate game-level statistics
game_stats = spark.sql("""
SELECT
	game_date,
	home_team AS game_home_team,
	away_team AS game_away_team,
	MAX(home_score) AS home_score,
	MAX(away_score) AS away_score,
	COUNT(*) AS total_pitches,
	SUM(CASE WHEN events = 'strikeout' AND inning_topbot = 'Bot' THEN 1 ELSE 0 END) AS home_strikeouts,
	SUM(CASE WHEN events = 'strikeout' AND inning_topbot = 'Top' THEN 1 ELSE 0 END) AS away_strikeouts,
	AVG(launch_speed) AS avg_exit_velocity,
	AVG(launch_angle) AS avg_launch_angle,
	SUM(CASE WHEN events = 'home_run' THEN 1 ELSE 0 END) as home_runs
FROM statcast
GROUP BY game_date, home_team, away_team
""").alias("game_stats")


# Join home/away statistics with game-level statistics
game_stats = game_stats \
	.join(home_team_stats, on="game_date", how="left") \
	.join(away_team_stats, on="game_date", how="left")


# Rename columns, so there is no ambigiuity after joining columns
game_stats = game_stats \
	.withColumnRenamed("game_home_team", "game_home_team_stat") \
	.withColumnRenamed("game_away_team", "game_away_team_stat") \
	.withColumnRenamed("home_hits_stat", "home_hits_stat") \
	.withColumnRenamed("away_hits_stat", "away_hits_stat")


# Add additional feature columns
home_window = Window.partitionBy("game_home_team_stat")
away_window = Window.partitionBy("game_away_team_stat")


game_stats = game_stats.withColumn(
	"home_avg_hits", functions.col("home_hits_stat") / functions.count("game_date").over(home_window)
)

game_stats = game_stats.withColumn(
	"away_avg_hits", functions.column("away_hits_stat") / functions.count("game_date").over(away_window)
)


# Calculate average pitching statistics
game_stats = game_stats.withColumn(
	"home_avg_pitch_speed", functions.avg("home_avg_pitch_speed").over(home_window)
)

game_stats = game_stats.withColumn(
	"away_avg_pitch_speed", functions.avg("away_avg_pitch_speed").over(away_window)
)


# Create a binary target column for "home_win"
game_stats = game_stats.withColumn("home_win", (game_stats["home_score"] > game_stats["away_score"]).cast("int"))


# Encode categorical features
indexer = StringIndexer(inputCols=["game_home_team_stat", "game_away_team_stat"], outputCols=["home_team_index", "away_team_index"])

game_stats = indexer.fit(game_stats).transform(game_stats)


# Feature columns
feature_cols = [
	"home_team_index",
	"away_team_index",
	"home_avg_hits",
	"away_avg_hits",
	"home_strikeouts",
	"away_strikeouts",
	"avg_exit_velocity",
	"avg_launch_angle",
	"home_runs",
	"home_avg_pitch_speed",
	"away_avg_pitch_speed"]



# Assemble features into a feature vector, skip invalid values in feature columns
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")

# Tranform game_stats into feature vectors, skip rows with invalid values
dataset = assembler.transform(game_stats).select("features", "home_win")

# Split the dataset into two halves
train, test = dataset.randomSplit([0.8, 0.2], seed=42)

# Train a Random Forest classifier
rf = RandomForestClassifier(featuresCol="features", labelCol="home_win", numTrees=100) # TODO: Test numTrees and the effects on accuracy
model = rf.fit(train)


# Evaluate model
predictions = model.transform(test)
evaluator = MulticlassClassificationEvaluator(labelCol="home_win", metricName="accuracy")

accuracy = evaluator.evaluate(predictions)
print(f"Accuracy: {accuracy}")

# Display feature-importances
print(f"Feature Importances: {model.featureImportances}")
