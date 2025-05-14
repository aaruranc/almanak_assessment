import os
import sys
import yaml
import pandas as pd

from pathlib import Path
from dotenv import load_dotenv, find_dotenv

from data.cex_data import get_cex_data
from data.hl_data import get_hl_data
from data.aggregate import clean_data
from strategy.signal import generate_signals
from strategy.sizing import compute_sizes
from risk.manager import RiskManager
from backtest.engine import backtest_strategy
from backtest.report import export_summary


def main(args):

	# Load Config
	BASE_DIR = Path(__file__).resolve().parent
	with open(BASE_DIR / "config.yaml", "r") as f:
		config = yaml.safe_load(f)

	# Load Environment Variables
	load_dotenv(find_dotenv())

	# Download Historical Data
	if len(sys.argv) > 1 and sys.argv[1] == '-d':
		get_cex_data(config)
		get_hl_data(config)
		clean_data(config)

	# Load Historical Data
	historical_data = {}
	for asset in config['assets']:
		f = os.path.join(BASE_DIR, 'data', 'historical', 'clean', asset, f'{asset}.csv')
		historical_data[asset] = pd.read_csv(f)

	# Generate Signals and Sizes
	signals = generate_signals(historical_data, config)
	sizes = compute_sizes(signals, config)

	# Initialize Risk Manager
	risk_mgr = RiskManager(config)

	# Run Backtest Engine
	result = backtest_strategy(historical_data, signals, sizes, risk_mgr, config)

	# Produce Report
	outpath = os.path.join(os.getcwd(), 'results')
	if not os.path.exists(outpath): os.makedirs(outpath)
	export_summary(result, outpath, config)

	return


if __name__ == '__main__':
	main(sys.argv)

