# scrape_statcast.py
# A small script to gather data from Statcast.

import pybaseball
from pybaseball import statcast
import pandas as pd


# Enable caching
pybaseball.cache.enable()

# Data should include games from opening day, to the final regular game of the season.
data = statcast(start_dt="2022-04-07", end_dt="2022-10-05")
data['game_date'] = data['game_date'].astype(str)
# Save the data as Parquet, which should be more useful for PySpark.
data.to_parquet("data/statcast_data_2022.parquet", index=False)
