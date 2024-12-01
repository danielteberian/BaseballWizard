# scrape_statcast.py
# A small script to gather data from Statcast.

import pybaseball
from pybaseball import statcast
import pandas as pd


# Enable caching
pybaseball.cache.enable()

# Data should include games from opening day, to the final regular game of the season.
data = statcast(start_dt="2023-03-30", end_dt="2023-10-03")
data['game_date'] = data['game_date'].astype(str)
# Save the data as Parquet, which should be more useful for PySpark.
data.to_parquet("statcast_data_2023.parquet", index=False)
