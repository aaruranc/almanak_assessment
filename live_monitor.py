import yaml
import uvicorn
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

from monitoring.app import app

def main():

	# Load Config
	BASE_DIR = Path(__file__).resolve().parent
	with open(BASE_DIR / 'config.yaml', 'r') as f: config = yaml.safe_load(f)

	uvicorn.run(
		app,
		host=config['monitor_host'],
		port=config['monitor_port'],
		access_log=False
		)

	return

if __name__ == '__main__':
	main()