# Guide
BaseballWizard is fairly easy to work with, provided that you have the required libraries and a basic understanding of how to use PySpark and Python.

1. Create a virtualenv (recommended)
2. Install dependencies by running,
	pip install -r requirements.txt
3. Navigate to the scripts/statcast directory
4. Gather data for the 2024 season (which is the one that the next script is designed to use, as of this writing) by running,
	python3 scraping/scrape_statcast_2024.py

5. Return to the root directory of the repository
6. Run the following command to train and test the model,
	python3 scripts/train.py
7. Record the evaluation at the end of the tests, and create a file in the "testing" directory.
	#TODO: Add a template
8. Submit a PR with your test report.
