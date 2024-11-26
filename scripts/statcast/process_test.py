from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Statcast Model") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Load data
df = spark.read.parquet("data/*.parquet").withColumn("game_date", F.col("game_date").cast("string"))
df.createOrReplaceTempView("statcast")

# Calculate the home and away hits as before
home_team_stats = spark.sql("""
SELECT
    game_date,
    home_team,
    SUM(CASE WHEN inning_topbot = 'Bot' THEN 1 ELSE 0 END) AS home_hits
FROM statcast
GROUP BY game_date, home_team
""").alias("home_team_stats")

away_team_stats = spark.sql("""
SELECT
    game_date,
    away_team,
    SUM(CASE WHEN inning_topbot = 'Top' THEN 1 ELSE 0 END) AS away_hits
FROM statcast
GROUP BY game_date, away_team
""").alias("away_team_stats")

# Rename the home_team and away_team columns in home_team_stats and away_team_stats to avoid ambiguity
home_team_stats = home_team_stats.withColumnRenamed("home_team", "home_team_stat")
away_team_stats = away_team_stats.withColumnRenamed("away_team", "away_team_stat")

# Now, join the game stats with home and away hits stats
game_stats = spark.sql("""
SELECT
    game_date,
    home_team AS game_home_team,
    away_team AS game_away_team,
    MAX(home_score) AS home_score,
    MAX(away_score) AS away_score,
    COUNT(*) AS total_pitches,
    SUM(CASE WHEN events = 'strikeout' AND inning_topbot = 'Bot' THEN 1 ELSE 0 END) AS home_strikeouts,
    SUM(CASE WHEN events = 'strikeout' AND inning_topbot = 'Top' THEN 1 ELSE 0 END) AS away_strikeouts
FROM statcast
GROUP BY game_date, home_team, away_team
""").alias("game_stats")

# Join the game stats with home and away hits stats
game_stats = game_stats \
    .join(home_team_stats, on="game_date", how="left") \
    .join(away_team_stats, on="game_date", how="left")

# Explicitly rename columns to avoid ambiguity after the join
game_stats = game_stats.withColumnRenamed("game_home_team", "game_home_team_stat") \
    .withColumnRenamed("game_away_team", "game_away_team_stat") \
    .withColumnRenamed("home_hits", "home_hits_stat") \
    .withColumnRenamed("away_hits", "away_hits_stat")

# Now calculate average hits after the join, ensuring the correct partitioning
home_window = Window.partitionBy("game_home_team_stat")
away_window = Window.partitionBy("game_away_team_stat")

# Add home_avg_hits and away_avg_hits by partitioning over the correct window
game_stats = game_stats.withColumn(
    "home_avg_hits", F.col("home_hits_stat") / F.count("game_date").over(home_window)
)

game_stats = game_stats.withColumn(
    "away_avg_hits", F.col("away_hits_stat") / F.count("game_date").over(away_window)
)

# Create the label column for home_win
game_stats = game_stats.withColumn("home_win", (game_stats["home_score"] > game_stats["away_score"]).cast("int"))

# Encoding for team names
indexer = StringIndexer(inputCols=["game_home_team_stat", "game_away_team_stat"],
                        outputCols=["home_team_index", "away_team_index"])

game_stats = indexer.fit(game_stats).transform(game_stats)

# Feature columns
feature_cols = ["home_team_index", "away_team_index", "home_avg_hits",
                "away_avg_hits", "home_strikeouts", "away_strikeouts"]

# Assemble features
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

dataset = assembler.transform(game_stats).select("features", "home_win")

# Split the dataset into training and test sets
train, test = dataset.randomSplit([0.8, 0.2], seed=42)

# Train a Random Forest model
rf = RandomForestClassifier(featuresCol="features", labelCol="home_win", numTrees=100)
model = rf.fit(train)

# Evaluate the model
predictions = model.transform(test)
evaluator = MulticlassClassificationEvaluator(labelCol="home_win", metricName="accuracy")

accuracy = evaluator.evaluate(predictions)
print(f"Model Accuracy: {accuracy}")

# Feature importances
print(f"Feature Importances: {model.featureImportances}")
