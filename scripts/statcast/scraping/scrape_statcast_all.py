# scrape_statcast_all.py
#
# A simple script that scrapes all of the available data from Statcast, and puts it into a Parquet file.


import pybaseball
from pybaseball import statcast
import pandas as pd


# Enable caching
pybaseball.cache.enable()

# Provides the start and end dates for each season.
seasons  = {
	2015: {"start": "2015-04-05", "end": "2015-10-04"}, # April 5, 2015 - October 4, 2015
	2016: {"start": "2016-04-03", "end": "2016-10-02"}, # April 6, 2016 -  October 2, 2016
	2017: {"start": "2017-04-02", "end": "2017-10-01"}, # April 2, 2017 - October 1, 2017
	2018: {"start": "2018-03-29", "end": "2018-09-30"}, # March 29, 2018 - September 30, 2018
	2019: {"start": "2019-03-28", "end": "2019-09-29"}, # March 28, 2019 - September 29, 2019
	2020: {"start": "2020-07-23", "end": "2020-09-27"}, # July 23, 2020 - September 27, 2020
	2021: {"start": "2021-04-01", "end": "2021-10-03"}, # April 1, 2021 - October 3, 2021
	2022: {"start": "2022-04-07", "end": "2022-10-05"}, # April 7, 2022 -  October 5, 2022
	2023: {"start": "2023-03-30", "end": "2023-10-01"}, # March 30, 2023 - October 1, 2023
	2024: {"start": "2024-03-20", "end": "2024-09-29"}, # March 20, 2024 - September 29, 2024
}


# Scrape and save data from a given year
def scrape_for_year(year, start_date, end_date):
	print(f"[INFO] Scraping data from {year}...")

	data = statcast(start_dt=start_date, end_dt=end_date)

	# Make the 'game_date' column into strings
	data['game_date'] = data['game_date'].astype(str)

	return data

all_data = pd.DataFrame()


for year in range(2015, 2024):
	# Get the start date for each year
	start_date = seasons[year]["start"]
	end_date = seasons[year]["end"]

	# Scrape data from the selected year
	year_data = scrape_for_year(year, start_date, end_date)

	# Add data to the comprehensive dataframe
	all_data = pd.concat([all_data, year_data], ignore_index=True)

# Save data to a Parquet file
all_data.to_parquet("statcast_2015_2024.parquet", index=False)
