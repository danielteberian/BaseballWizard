# map.py
#
# This should map different values to indexes, so that the data is easier to work with.


import pandas as pd

df = pd.read_csv("statcast_2024_data.csv")

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

df['home_team'] = df['home_team'].map(team_index).astype(int)
df['away_team'] = df['away_team'].map(team_index).astype(int)

df['home_team'] = df['home_team'].astype(int)

print(df['home_team'].head())
