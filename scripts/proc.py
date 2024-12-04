# proc.py
#
# This should gather data, clean it, and index the columns that contain non-numeric values.

from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from pybaseball import statcast
import pybaseball
import pandas as pd
import os


if os.path.exists("statcast_2024_data.csv"):
	print("Data found. Continuing.")
else:
	# Enable caching, in case there is an error during the data retrieval.
	pybaseball.cache.enable()

	# Ensure that data is gathered, from the opening day, to the final game of the season.
	data = statcast(start_dt="2024-03-20", end_dt="2024-09-29")

	# Convert "game_date" to string values
	data['game_date'] = data['game_date'].astype(str)

	data.to_csv("statcast_2024_data.csv")

	# Create a dataframe from the data
	df = pd.read_csv("statcast_2024_data.csv")

	cols_to_rm = [
		'age_bat_legacy',
		'age_pit_legacy',
		'game_type',
		'spin_rate_deprecated',
		'arm_angle',
		'api_break_x_batter_in',
		'api_break_z_with_gravity',
		'batter_days_until_next_game',
		'pitcher_days_until_next_game',
		'batter_days_since_prev_game',
		'pitcher_days_since_prev_game',
		'n_priorpa_thisgame_player_at_bat',
		'age_bat',
		'hyper_speed',
		'age_pit',
		'of_fielding_alignment',
		'if_fielding_alignment',
		'pitch_name',
		'iso_value',
		'woba_value',
		'babip_value',
		'woba_denom',
		'tfs_zulu_deprecated',
		'tfs_deprecated',
		'game_year',
		'game_pk',
		'des',
		'break_angle_deprecated',
		'break_length_deprecated']

	df = df.drop(cols_to_rm, axis=1, errors='ignore')

	# Save cleaned dataset to CSV
	df.to_csv("statcast_2024_data.csv")

# Reload the dataset
df = pd.read_csv("statcast_2024_data.csv")
