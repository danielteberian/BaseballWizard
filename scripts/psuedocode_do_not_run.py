# This file is not meant to be executed. If you do, it won't work. If it does work, let us know, since that would save us a lot of time.# Calculate home-runs
calc_hr = hit(distance, park, direction)
	# Each park has a different distance for the ball to be a homerun.
	# Each park should have the following:
	# <parkname>_lf = left-field-distance
	# <parkname>_rf = right-field-distance
	# <parkname>_cf = center-field-distance


	# Each hit is evaluated, based on which park it was hit in, how far it travelled, and in the direction in which it travelled.
	# Angle should be included, as well as the height of the ball hit.

	The ball must be hit above the height of the wall of that area of the fence.
	The ball must be hit to the left of the right foul pole, and to the right of the left foul pole.
	For Citizen's Bank Park:
		Left-Field Foul Pole: 329ft
		Left-Field Power-Alley: 374ft
		Monty's Angle: 409ft - 381ft - 387ft	# This is something specific to CBP.
		Right-Field Power-Alley: 369ft
		Right-Field Foul-Pole: 330ft


	evaluate the angle that the ball is hit, the distance it travels, and how high up it is (Y-axis).
	Wind can affect the hit, and the wind data is available in the dataset.








calc_pitcher_stats:
	pitcher: name
	pitch_type,
	strikeouts,
	strikes,
	balls,
	HBP,
	wild-pitch/error,
	spin_rate,
	velocity,
	accelaration,
	pitching_hand,
	balls-per-AB,
	walks



