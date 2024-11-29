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

# Load the statcast dataset
df = spark.read.parquet("statcast/data/statcast_data_2024.parquet").withColumn("game_date", functions.col("game_date").cast("string"))
df.createOrReplaceTempView("statcast")

# Step 1: Calculate Home Team Stats (Batting & Pitching)
home_team_stats = spark.sql("""
SELECT
    game_date,
    home_team,
    SUM(CASE WHEN inning_topbot = 'Bot' THEN 1 ELSE 0 END) AS home_hits,
    AVG(release_speed) AS home_avg_pitch_speed,
    AVG(release_spin_rate) AS home_avg_spin_rate,
    AVG(launch_speed) AS home_avg_exit_velocity,
    AVG(launch_angle) AS home_avg_launch_angle,
    SUM(CASE WHEN events = 'home_run' THEN 1 ELSE 0 END) AS home_runs
FROM statcast
WHERE home_team IS NOT NULL
GROUP BY game_date, home_team
""")

# Step 2: Calculate Away Team Stats (Batting & Pitching)
away_team_stats = spark.sql("""
SELECT
    game_date,
    away_team,
    SUM(CASE WHEN inning_topbot = 'Top' THEN 1 ELSE 0 END) AS away_hits,
    AVG(release_speed) AS away_avg_pitch_speed,
    AVG(release_spin_rate) AS away_avg_spin_rate,
    AVG(launch_speed) AS away_avg_exit_velocity,
    AVG(launch_angle) AS away_avg_launch_angle,
    SUM(CASE WHEN events = 'home_run' THEN 1 ELSE 0 END) AS away_runs
FROM statcast
WHERE away_team IS NOT NULL
GROUP BY game_date, away_team
""")

# Step 3: Aggregate Game-Level Statistics
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
    AVG(launch_angle) AS avg_launch_angle
FROM statcast
GROUP BY game_date, home_team, away_team
""")

# Step 4: Join Game Stats with Home and Away Team Stats
game_stats = game_stats \
    .join(home_team_stats, on="game_date", how="left") \
    .join(away_team_stats, on="game_date", how="left")

# Step 5: Rename columns to avoid ambiguity
game_stats = game_stats \
    .withColumnRenamed("home_runs", "home_team_home_runs") \
    .withColumnRenamed("away_runs", "away_team_home_runs")

# Step 6: Add Additional Features for Home and Away Performance
home_window = Window.partitionBy("game_home_team")
away_window = Window.partitionBy("game_away_team")

game_stats = game_stats.withColumn(
    "home_avg_hits", functions.col("home_hits") / functions.count("game_date").over(home_window)
)

game_stats = game_stats.withColumn(
    "away_avg_hits", functions.col("away_hits") / functions.count("game_date").over(away_window)
)

game_stats = game_stats.withColumn(
    "home_avg_pitch_speed", functions.avg("home_avg_pitch_speed").over(home_window)
)

game_stats = game_stats.withColumn(
    "away_avg_pitch_speed", functions.avg("away_avg_pitch_speed").over(away_window)
)

# Step 7: Create Binary Target Column for Home Win (1 = home team wins, 0 = away team wins)
game_stats = game_stats.withColumn("home_win", (game_stats["home_score"] > game_stats["away_score"]).cast("int"))

# Step 8: Encode Categorical Features (Teams)
indexer = StringIndexer(inputCols=["game_home_team", "game_away_team"], outputCols=["home_team_index", "away_team_index"])
game_stats = indexer.fit(game_stats).transform(game_stats)

# Step 9: Define Feature Columns
feature_cols = [
    "home_team_index",
    "away_team_index",
    "home_avg_hits",
    "away_avg_hits",
    "home_strikeouts",
    "away_strikeouts",
    "avg_exit_velocity",
    "avg_launch_angle",
    "home_team_home_runs",  # Renamed column
    "home_avg_pitch_speed",
    "away_avg_pitch_speed",
    "home_avg_spin_rate",
    "away_avg_spin_rate"
]

# Step 10: Assemble Features into a Feature Vector
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")

# Transform the data into feature vectors
dataset = assembler.transform(game_stats).select("features", "home_win")

# Step 11: Split the Dataset into Training and Test Sets (80% Train, 20% Test)
train, test = dataset.randomSplit([0.8, 0.2], seed=42)

# Step 12: Train the Random Forest Model
rf = RandomForestClassifier(featuresCol="features", labelCol="home_win", numTrees=100)
model = rf.fit(train)

# Step 13: Evaluate the Model on the Test Set
predictions = model.transform(test)

# Step 14: Evaluate Accuracy
evaluator = MulticlassClassificationEvaluator(labelCol="home_win", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

print(f"Accuracy: {accuracy}")

# Step 15: Display Feature Importances
print(f"Feature Importances: {model.featureImportances}")

# Step 16: Display Some Predictions with Actual Results
predictions.select("game_date", "game_home_team", "game_away_team", "home_score", "away_score", "prediction").show(10)
