import pandas as pd

def load_player_map(player_file):
	df = pd.read_csv(player_file)

	# Make sure the file is in the correct format
	if 'code' not in df.columns or 'name' not in df.columns:
		raise ValueError("The player map must be in the correct format. Refer to documentation.")

	player_mapping = dict(zip(df['code'], df['name']))

	return player_mapping


def replace_player_codes(event_string, player_mapping):
	event_sect = event_string.split(',')

	translated_sects = []
	for part in event_sect:
		translated_sect = player_mapping.get(part, part)
		translated_sects.append(translated_sect)

	return ', '.join(translated_sects)


def proc_file(in_file, out_file, player_mapping):

	df = pd.read_csv(in_file)

	if 'id' not in df.columns:
		raise ValueError("The input file is not in the correct format. Refer to the documentation.")

	df['translated_sects'] = df['id'].apply(lambda event: replace_player_codes(event, player_mapping))


	# Write to output-file
	df.to_csv(out_file, index=False)

	print(f"Successfully processed. Output destination: '{out_file}'.")



# The main part of the script.
if __name__ == "__main__":

	in_file = "in_file.csv"
	out_file = "out_file.csv"
	player_file = "player_map.csv"

	player_mapping = load_player_map(player_file)
	proc_file(in_file, out_file, player_mapping)
