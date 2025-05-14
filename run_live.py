import os
import yaml
import logging
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

from risk.manager import RiskManager
from live.clients import BinanceClient, HyperliquidClient
from live.execution import Strategy, execution_loop


def main():

	# Load Config
	BASE_DIR = Path(__file__).resolve().parent
	with open(BASE_DIR / "config.yaml", "r") as f: config = yaml.safe_load(f)

	# Load Environment Variables
	load_dotenv(find_dotenv())

	# Initialize Logger
	if not os.path.exists(BASE_DIR / "logs"): os.makedirs(BASE_DIR / "logs")
	logging.basicConfig(
		filename=BASE_DIR / "logs"/ "production.log",
		level=logging.INFO,
		format="%(asctime)s %(levelname)-8s %(message)s",
		datefmt="%Y-%m-%dT%H:%M:%S"
		)
	logger = logging.getLogger()

    # Initialize Trading Clients
	bn_client = BinanceClient(config)
	hl_client = HyperliquidClient(config)
	risk_mgr = RiskManager(config)

	# Initialize Strategy Object
	strategy = Strategy(config, logger, bn_client, hl_client, risk_mgr)

	# Run Execution Loop
	try:
		execution_loop(strategy, config)
	except Exception as e:
		print(e)
	finally:
		print('Cancelling All Open Orders')
		strategy.cancel_orders()

	return


if __name__ == '__main__':
	main()