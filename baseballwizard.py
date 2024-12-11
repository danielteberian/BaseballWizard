#  ____                 _           _ ___        ___                  _
# | __ )  __ _ ___  ___| |__   __ _| | \ \      / (_)______ _ _ __ __| |
# |  _ \ / _` / __|/ _ \ '_ \ / _` | | |\ \ /\ / /| |_  / _` | '__/ _` |
# | |_) | (_| \__ \  __/ |_) | (_| | | | \ V  V / | |/ / (_| | | | (_| |
# |____/ \__,_|___/\___|_.__/ \__,_|_|_|  \_/\_/  |_/___\__,_|_|  \__,_|
#
# This is a simple, easy-to-use (kind of) way of predicting baseball games, based on tons of data.


# IMPORTS
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from pybaseball import statcast
from pyfiglet import Figlet
import pybaseball
import pandas as pd
import os
import time
import sys
from rich.console import Console
from rich.prompt import Prompt
from rich.progress import Progress
import warnings
import logging


# Hide the warnings from the user. Ain't nobody got time for that.
warnings.filterwarnings("ignore")

# Start the console
console = Console()


# Retrieve data
def retrieve_data(progress):

	# Create a progress bar
	task = progress.add_task("[cyan]Retrieving data...[/cyan]", total=4)

	console.print("[blink yellow]Enabling caching...[/blink yellow]")
	 # Enable caching
	pybaseball.cache.enable()
	progress.update(task, advance=1)

	console.print("[blink yellow]Downloading data...[/blink yellow]")
	 # Gather data from opening day, until the final game of the regular season
	data = statcast(start_dt="2024-03-20", end_dt="2024-09-29")
	progress.update(task, advance=1)

	console.print(
	    "[blink yellow]Ensuring that data is formatted correctly...[/blink yellow]")
	# Convert "game_date" to string values
	data['game_date'] = data['game_date'].astype(str)
	progress.update(task, advance=1)

	console.print("[blink yellow]Writing data to file...[/blink yellow]")
	# Write data to file
	data.to_csv("bw_data.csv")
	progress.update(task, advance=1)

	# Stop the current progress bar
	progress.stop()

	# Tell the user that the data has been retrieved successfully.
	console.print("[bold green]Successfully retrieved data![/bold green]")

	# Switch over to data preprocessing
	prepare_data_1(progress)

# Prepare data
def prepare_data_1(progress):

	# Columns to remove
	columns_to_remove = [
		'Unnamed: 0.1',
		'Unnamed: 0.2',
		'Unnamed: 0.3',
		'Unnamed: 0.4',
		'Unnamed: 0.5',
		'Unnamed: 0',
		'player_name',
		'spin_dir',
		'spin_rate_deprecated',
		'break_angle_deprecated',
		'break_length_deprecated',
		'des',
		'game_type',
		'tfs_deprecated',
		'tfs_zulu_deprecated',
		'umpire',
		'sv_id',
		'estimated_ba_using_speedangle',
		'estimated_woba_using_speedangle',
		'woba_value',
		'woba_denom',
		'babip_value',
		'iso_value',
		'at_bat_number',
		'pitch_name',
		'estimated_slg_using_speedangle',
		'hyper_speed',
		'age_pit_legacy',
		'age_bat_legacy',
		'n_priorpa_thisgame_player_at_bat',
		'pitcher_days_since_prev_game',
		'batter_days_since_prev_game',
		'pitcher_days_until_next_game',
		'batter_days_until_next_game',
		'api_break_z_with_gravity',
		'api_break_x_arm',
		'api_break_x_batter_in',
		'arm_angle'
		]

	# Create a progress bar
	task = progress.add_task("[cyan]Preprocessing data...[/cyan]", total=len(columns_to_remove))

		# Load data from file
	console.print("[blink yellow]Loading dataset into a dataframe...[/blink yellow]")
	df = pd.read_csv("bw_data.csv")

	# Remove unneccesary columns
	console.print("[blink yellow]Removing unneccesary columns...[/blink yellow]")

	# Iterate through the list of columns to remove
	for col in columns_to_remove:
			task = progress.add_task("[cyan]Removing {col}[/cyan]", total=len(columns_to_remove))
			# Remove column
			df = df.drop(col, axis=1, errors='ignore')

			# Update the progress bar
			progress.update(task, advance=1)
			console.print("[blink yellow]Removing unneccesary columns...[/blink yellow]")

	# Print a message to let the user know that the process has completed
	console.print("[bold green]Successfully removed unneccessary columns![/bold green]")

	# Stop current progress bar
	progress.stop()

	# Write data to file
	console.print("[blink yellow]Updating dataset on disk...")
	df.to_csv("bw_data.csv")

				# Map different columns to indexes
				#	index_teams(progress)



def index_cols(progress):
    	# Tells the user that the teams are being mapped to an index
	console.print("[blink yellow]Mapping non-numeric columns to index...[/blink yellow]")

	# Reload the dataframe
	df = pd.read_csv("bw_data.csv")

	# Assign values to give to columns with missing values
	nan_index = {
	'on_1b': 0,
	'on_2b': 0,
	'on_3b': 0,
	'if_fielding_alignment': 0,
	'of_fielding_alignment': 0,
	'launch_speed_angle': 0,
	}

	# Map team names to an index
	team_index = {
	'TEX': 1,
	'SEA': 2,
 	'OAK': 3,
	'HOU': 4,
	'LAA': 5,
	'CWS': 6,
	'CLE': 7,
	'DET': 8,
	'KC': 9,
	'MIN': 10,
	'BAL': 11,
	'TOR': 12,
	'BOS': 13,
	'TB': 14,
	'NYY': 15,
	'AZ': 16,
	'SF': 17,
	'COL': 18,
	'SD': 19,
	'LAD': 20,
	'STL': 21,
	'CHC': 22,
	'CIN': 23,
	'PIT': 24,
	'MIL': 25,
	'WAS': 26,
	'ATL': 27,
	'PHI': 28,
	'NYM': 29,
	'MIA': 30,
	}

	# Events index
	events_index = {
	'field_out': 1,
	'single': 2,
	'home_run': 3,
	'strikeout': 4,
	'grounded_into_double_play': 5,
	'hit_by_pitch': 6,
	'triple': 7,
	'force_out': 8,
	'double': 9,
	'walk': 10,
	'sac_bunt': 11,
	'fielders_choice_out': 12,
	'catcher_interf': 13,
	'truncated_pa': 14,
	'fielders_choice': 15,
	'sac_fly': 16,
	'field_error': 17,
	'double_play': 18,
	'strikeout_double_play': 19,
	'triple_play': 20,
	'sac_fly_double_play': 21,
	}

	# Infield alignment index
	if_field_index = {
	'Standard': 1,
	'Strategic': 2,
	'Infield shade': 3,
	}


	# Outfield alignment index
	of_field_index = {
	'Standard': 1,
	'Strategic': 2,
	'4th outfielder': 3,
	}

	# Top/bottom of inning index
	topbot_index = {
	'Top': 1,
	'Bot': 2,
	}

	# Result of pitch (type) index
	type_index = {
	'X': 1,
	'S': 2,
	'B': 3,
	}

	# Launch speed angle index
	# This is done so that 0 refers to a missing value, not 0 degrees
	launch_speed_angle_index = {
	'0': 0,
	'1.': 1,
	'2.': 2,
	'3': 3,
	'4.': 4,
	'5.': 5,
	'6.': 6,
	}

	# Pitch type index
	pitch_type_index = {
	}

	# Combine events/description, assign values to a new column
	def pitch_result(row):

		# If a row is missing an entry in "events" or in "description"
		if pd.isna(row['events']) or pd.isna(row['description']):

			# If there is a value in the events column, use that in the events column
			# If there is no value in the events column, use the value from the description column
			return row['events'] if pd.notna(row['events']) else row['description']

		# If events is field_out, and description is hit_into_play
		if row['events'] == 'field_out' and row['description'] == 'hit_into_play':
			return 'field_out'

		# If events is home_run, and description is hit_into_play
		elif row['events'] == 'home_run' and row['description']=='hit_into_play':
			return 'home_run'

		# If description is foul
		elif row['description'] == 'foul':
			return 'foul'

		# If description is ball
		elif row['description'] == 'ball':
			return 'ball'

		# If description is swinging_strike
		elif row['description'] == 'swinging_strike':
			return 'swinging_strike'

		# If event is single, and description is hit_into_play
		elif row['event'] == 'single' and row['description'] == 'hit_into_play':
			return 'single'

		# If description is blocked_ball
		elif row['description'] == 'blocked_ball':
			return 'blocked_ball'

		# If description is called_strike
		elif row['description'] == 'called_strike':
			return 'called_strike'

		# If event is strikeout, and description is foul_tip
		elif row['event'] == 'strikeout' and row['description'] == 'foul_tip':
			return 'foul_tip'

		# If event is strikeout, and description is called_strike
		elif row['event'] == 'strikeout' and row['description'] == 'called_strike':
			return 'called_strike'

		# If description is foul_tip
		elif row['description'] == 'foul_tip':
			return 'foul_tip'

		# If event is grounded_into_double_play, and description is hit_into_play
		elif row['event'] == 'grounded_into_double_play' and row['description'] == 'hit_into_play':
			return 'grounded_into_double_play'

		# If event is hit_by_pitch, and description is hit_by_pitch
		elif row['event'] == 'hit_by_pitch' and row['description'] == 'hit_by_pitch':
			return 'hit_by_pitch'

		# If event is triple, and description is hit_into_play
		elif row['event'] == 'triple' and row['description'] == 'hit_into_play':
			return 'triple'

		# If event is force_out, and description is hit_into play
		elif row['event'] == 'force_out' and row['description'] == 'hit_into_play':
			return 'force_out'

		# If event is strikeout, and description is swinging_strike
		elif row['event'] == 'strikeout' and row['description'] == 'swinging_strike':
			return 'swinging_strike'

		# If event is double, and description is hit_into_play
		elif row['event'] == 'double' and row['description'] == 'hit_into_play':
			return 'double'

		# If event is walk, and description is ball
		elif row['event'] == 'walk' and row['description'] == 'hit_into_play':
			return 'walk'

		# If event is sac_bunt, and description is hit_into_play
		elif row['event'] == 'sac_bunt' and row['description'] == 'hit_into_play':
			return 'sac_bunt'

		# If description is foul_bunt
		elif row['description'] == 'foul_bunt':
			return 'foul_bunt'

		# If event is strikeout, and description swinging_strike_blocked
		elif row['event'] == 'strikeout' and row['description'] == 'swinging_strike_blocked':
			return 'swinging_strike_blocked'

		# If all else fails, use the value in the description column
		else:
			return row['description']

	df['pitch_result'] = df.apply(pitch_result, axis=1)


	# Map team names in-place

	console.print("[blink yellow]Mapping home teams to index...[/blink yellow]")
	df['home_team'] = df['home_team'].map(team_index).astype(int)

	console.print("[blink yellow]Mapping away teams to index...[/blink yellow]")
	df['away_team'] = df['away_team'].map(team_index).astype(int)



					# The main function
def main():
	# Clear the console
	console.clear()

	bw_logo = Figlet(font='slant')
	console.print(bw_logo.renderText('BaseballWizard'))

	# Print inital message to the user
	console.print("[bold blue]Welcome to BaseballWizard![/bold blue]")
	console.print("[yellow]First, we need a dataset. Do you have one?[/yellow]")
	console.print("[white]1 - I already have a dataset.[/white]")
	console.print("[white]2 - I need to download a dataset.[/white]")

	# Ask the user if they need to download the data
	choice1 = Prompt.ask(
	"[blue]What would you like to do?[/blue]",
	choices=["1", "2"],
 	show_choices=True,
	)

	with Progress() as progress:
			if choice1 == "1":
				sys.exit()
			elif choice1 == "2":
				retrieve_data(progress)

if __name__ == "__main__":
	main()
