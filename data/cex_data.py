import os
import yaml
import ccxt
import pandas as pd
from time import sleep

from pathlib import Path
from datetime import timedelta, timezone
from dotenv import load_dotenv, find_dotenv


def get_price_data(start, end, exch, pair, method=None):

	since_ms = int(start.timestamp() * 1000)
	end_ms = int(end.timestamp() * 1000)

	rows = []
	while since_ms < end_ms:

		batch = []
		if not method: batch = exch.fetch_ohlcv(pair, '1m', since=since_ms, limit=1000)
		else: batch = exch.fetch_ohlcv(pair, '1m', since=since_ms, limit=1000, params={'price': method})
		if not len(batch): break

		rows.extend(batch)
		since_ms = batch[-1][0] + 60000
		sleep(exch.rateLimit / 1000) 

	return pd.DataFrame(rows, columns = ['t', 'O', 'H', 'L', 'C', 'V'])


def get_funding_data(start, end, exch, pair):

	since_ms = int(start.timestamp() * 1000)
	end_ms = int(end.timestamp() * 1000)
	
	rows = []

	while since_ms < end_ms:

		batch = exch.fetch_funding_rate_history(pair, since=since_ms, limit=1000)			
		if not len(batch): break
		rows.extend(batch)
		since_ms = batch[-1]['timestamp'] + 60000
		sleep(exch.rateLimit / 1000) 

	return pd.DataFrame(rows)


def spot_data(start, end, exch_id):
	
	exch = getattr(ccxt, exch_id)({'enableRateLimit': True})
	exch.load_markets()
	
	for asset in config['assets']:
		print('spot', exch_id, asset)
		px_df = get_price_data(start, end, exch, f'{asset}/USDT')
		path = os.path.join(BASE_DIR, 'data', 'historical', 'raw', 'spot', exch_id)
		if not os.path.exists(path): os.makedirs(path)
		px_df.to_csv(f'{path}/{asset}.csv', index=False)

	return


def perp_data(start, end, exch_id):

	exch = getattr(ccxt, exch_id)({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})
	exch.load_markets()
		
	for asset in config['assets']:

		print('perp', exch_id, asset)
		contract = f'{asset}/USDT:USDT'

		px_df = get_price_data(start, end, exch, contract)
		path = os.path.join(BASE_DIR, 'data', 'historical', 'raw', 'perp', exch_id, 'price')
		if not os.path.exists(path): os.makedirs(path)
		px_df.to_csv(f'{path}/{asset}.csv', index=False)
		
		fund_df = get_funding_data(start, end, exch, contract)
		path = os.path.join(BASE_DIR, 'data', 'historical', 'raw', 'perp', exch_id, 'funding')
		if not os.path.exists(path): os.makedirs(path)
		fund_df.to_csv(f'{path}/{asset}.csv', index=False)

		index_df = get_price_data(start, end, exch, contract, 'index')
		path = os.path.join(BASE_DIR, 'data', 'historical', 'raw', 'perp', exch_id, 'index')
		if not os.path.exists(path): os.makedirs(path)
		index_df.to_csv(f'{path}/{asset}.csv', index=False)

		mark_df = get_price_data(start, end, exch, contract, 'mark')
		path = os.path.join(BASE_DIR, 'data', 'historical', 'raw', 'perp', exch_id, 'mark')
		if not os.path.exists(path): os.makedirs(path)
		mark_df.to_csv(f'{path}/{asset}.csv', index=False)

	return


def get_cex_data(config):
	
	start = config['start'].replace(tzinfo=timezone.utc)
	end = config['end'].replace(tzinfo=timezone.utc)

	for exch_id in config['spot_exchs']: spot_data(start, end, exch_id)
	for exch_id in config['perp_exchs']: perp_data(start, end, exch_id)

	return

