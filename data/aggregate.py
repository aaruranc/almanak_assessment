import os
import yaml
import json
import pandas as pd
import multiprocessing as mp

from pathlib import Path
from dotenv import load_dotenv, find_dotenv


def list_dates(start, end):
	dates = []
	cur = start.date()
	while cur <= end.date():
		dates.append(cur.strftime("%Y%m%d"))
		cur += timedelta(days=1)
	return dates


def parse_HL_data(asset):

	# Load Config
	BASE_DIR = Path(__file__).resolve().parent
	with open(BASE_DIR / "config.yaml", "r") as f:config = yaml.safe_load(f)

	path = os.path.join(BASE_DIR, 'data', 'historical', 'raw', 'perp', 'hyperliquid', 'asset_ctxt')

	df = pd.DataFrame()
	for date_str in list_dates(config['start'], config['end']):

		f = os.path.join(path, f'{date_str}.csv')
		if not os.path.isfile(f): continue

		tdf = pd.read_csv(f)
		tdf = tdf[tdf['coin'] == asset]

		if df.empty: df = tdf
		else: df = pd.concat([df, tdf], ignore_index=True)

	df['time'] = pd.to_datetime(df['time'])
	df['year'] = df['time'].dt.year
	df['month'] = df['time'].dt.month
	df['day'] = df['time'].dt.day
	df['hour'] = df['time'].dt.hour
	df['t'] = df['time'].values.astype(int) // 10 ** 6

	outpath = os.path.join(BASE_DIR, 'data', 'historical', 'clean', asset)
	if not os.path.exists(outpath): os.makedirs(outpath)

	# Store Funding Data
	fnd_df = df.groupby(['year', 'month', 'day', 'hour']).agg(
		t = ('t', 'first'),
		funding_payment = ('funding', 'first'))
	outfile = os.path.join(outpath, 'hl_funding.csv')
	fnd_df[['t', 'funding_payment']].to_csv(outfile, index=False)

	# Get Price Data
	vlm_chg = df['day_ntl_vlm'].diff(1).apply(lambda x: float('nan') if x < 0 else x)
	df['volume_notional'] = vlm_chg.interpolate(method='linear').interpolate(method='bfill')
	df['volume'] = df['volume_notional'] / df['mid_px']

	rename_d = {'open_interest': 'hl_open_interest',
				'premium': 'hl_premium',
				'oracle_px': 'hl_index_price',
				'mark_px': 'hl_mark_price',
				'mid_px': 'hl_perp_price',
				'volume': 'hl_perp_volume'}
	df.rename(columns=rename_d, inplace=True)

	cols = ['t', 'hl_perp_price', 'hl_perp_volume', 'hl_mark_price', 'hl_index_price', 'hl_premium', 'hl_open_interest']
	outfile = os.path.join(outpath, 'hl_price.csv')
	df[cols].to_csv(outfile, index=False)

	return


def merge_data(asset):

	# Load Config
	BASE_DIR = Path(__file__).resolve().parent
	with open(BASE_DIR / "config.yaml", "r") as f:config = yaml.safe_load(f)

	PX_COLS = ['O', 'H', 'L', 'C']
	path = os.path.join(BASE_DIR, 'data', 'historical', 'raw')
	
	df = pd.DataFrame()
	fnd_df = pd.DataFrame()

	# Add CEX Spot Data
	for exch in config['spot_exchs']:

		spot_f = os.path.join(path, 'spot', exch, f'{asset}.csv')
		spot_df = pd.read_csv(spot_f).drop_duplicates()
		spot_df[f'{exch}_spot_price'] = spot_df[PX_COLS].median(axis=1, skipna=True)
		spot_df.rename(columns={'V': f'{exch}_spot_volume'}, inplace=True)
		trade_df = spot_df[['t', f'{exch}_spot_price', f'{exch}_spot_volume']]

		if df.empty: df = trade_df
		else: df = pd.merge(df, trade_df, how='outer', on='t')

	# Add CEX Perp Data
	for exch in config['perp_exchs']:

		# Price Data
		perp_px_f = os.path.join(path, 'perp', exch, 'price', f'{asset}.csv')
		px_df = pd.read_csv(perp_px_f).drop_duplicates()
		px_df[f'{exch}_perp_price'] = px_df[PX_COLS].median(axis=1, skipna=True)
		px_df.rename(columns={'V': f'{exch}_perp_volume'}, inplace=True)
		df = pd.merge(df, px_df[['t', f'{exch}_perp_price', f'{exch}_perp_volume']], how='outer', on='t')

		# Mark Data
		perp_mark_f = os.path.join(path, 'perp', exch, 'mark', f'{asset}.csv')
		mark_df = pd.read_csv(perp_mark_f).drop_duplicates()
		mark_df[f'{exch}_mark_price'] = mark_df[PX_COLS].median(axis=1, skipna=True)
		df = pd.merge(df, mark_df[['t', f'{exch}_mark_price']], how='outer', on='t')

		# Index Data
		perp_index_f = os.path.join(path, 'perp', exch, 'index', f'{asset}.csv')
		index_df = pd.read_csv(perp_index_f).drop_duplicates()
		index_df[f'{exch}_index_price'] = index_df[PX_COLS].median(axis=1, skipna=True)
		df = pd.merge(df, index_df[['t', f'{exch}_index_price']], how='outer', on='t')
		df[f'{exch}_premium'] = (df[f'{exch}_perp_price'] / df[f'{exch}_index_price']) - 1 

		# Funding Data
		perp_funding_f = os.path.join(path, 'perp', exch, 'funding', f'{asset}.csv')
		funding_df = pd.read_csv(perp_funding_f).drop_duplicates()
		funding_df.rename(columns={'timestamp': 't', 'fundingRate': f'{exch}_funding_rate'}, inplace=True)

		if fnd_df.empty: fnd_df = funding_df[['t', f'{exch}_funding_rate']]
		else: fnd_df = pd.merge(fnd_df, funding_df[['t', f'{exch}_funding_rate']], on='t', how='outer')

	# Add HL Data
	outpath = os.path.join(BASE_DIR, 'data', 'historical', 'clean', asset)
	hl_px_df = pd.read_csv(os.path.join(outpath, 'hl_price.csv'))
	hl_fnd_df = pd.read_csv(os.path.join(outpath, 'hl_funding.csv'))

	df = pd.merge(df, hl_px_df, on='t', how='outer').sort_values('t')
	df = df[df['t'] <= hl_px_df['t'].max()]
	df.to_csv(os.path.join(outpath, 'price.csv'), index=False)

	fnd_df = pd.merge(fnd_df, hl_fnd_df, on='t', how='outer').sort_values('t')
	fnd_df.to_csv(os.path.join(outpath, 'funding.csv'), index=False)

	# Delete Temporary Files
	os.remove(os.path.join(outpath, 'hl_price.csv'))
	os.remove(os.path.join(outpath, 'hl_funding.csv'))

	return


def merge_price_funding(asset):

	fpath = os.path.join(BASE_DIR, 'data', 'historical', 'clean', asset)
	price = pd.read_csv(os.path.join(fpath, 'price.csv'))
	funding = pd.read_csv(os.path.join(fpath, 'funding.csv'))
	funding.rename(columns={'binance_funding_rate': 'binance_funding', 
		'funding_payment': 'hl_funding'}, inplace=True)

	# Clean Funding
	tol = 1000 * 60 * 5
	st, en = funding[['t', 'hl_funding']].dropna().iloc[[0, -1]]['t'].to_list()
	schedule = pd.DataFrame(
	    index=pd.date_range(st*10e5, en*10e5, freq='1H', tz='UTC'),
	)
	schedule['t'] = schedule.index.values.astype(int) // 10 ** 6
	temp = pd.merge_asof(schedule, funding[['t', 'hl_funding']], on='t')
	temp = pd.merge_asof(temp, funding[['t', 'binance_funding']].dropna(), direction='nearest', on='t', tolerance=tol)
	temp = temp.loc[:temp[~temp['binance_funding'].isna()].index[-1]]

	# Add HL Funding Information
	HL = temp[['t', 'hl_funding']]
	HL['hl_funding_time'] = HL['t']
	df = pd.merge_asof(price, HL[['t', 'hl_funding']], on='t', 
	                  direction='backward').rename(columns={'hl_funding':'hl_funding_prev'})
	df = pd.merge_asof(df, HL[['t', 'hl_funding']], on='t', allow_exact_matches=False,
	                  direction='forward').rename(columns={'hl_funding':'hl_funding_next'})
	df = pd.merge_asof(df, HL[['t', 'hl_funding_time']], on='t',direction='forward')

	# Add Binance Funding Information
	BN = temp[['t', 'binance_funding']].dropna()
	BN['binance_funding_time'] = BN['t']
	df = pd.merge_asof(df, BN[['t', 'binance_funding']], on='t', 
	                  direction='backward').rename(columns={'binance_funding':'binance_funding_prev'})
	df = pd.merge_asof(df, BN[['t', 'binance_funding']], on='t', allow_exact_matches=False,
	                  direction='forward').rename(columns={'binance_funding':'binance_funding_next'})
	df = pd.merge_asof(df, BN[['t', 'binance_funding_time']], on='t',direction='forward').dropna()

	# Save Merged File
	df.to_csv(os.path.join(fpath, f'{asset}.csv'), index=False)

	return


def clean_data(config):

	with mp.Pool(5) as pool:
		pool.map(parse_HL_data, config['assets'])
		pool.map(merge_data, config['assets'])
		pool.map(merge_price_funding, config['assets'])

	return

