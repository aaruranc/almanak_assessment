import pandas as pd

def generate_signals(historical_data, config):

	# Simply predicts next funding rate spread as equal to previous

	signal_data = []
	for asset in historical_data:

		df = historical_data[asset].set_index('t')
		diff = df['hl_funding_prev'] - df['binance_funding_prev']

		s = pd.Series(0, index=diff.index, name=asset)
		s[diff >  config['edge_threshold']] = 1
		s[diff < -config['edge_threshold']] = -1

		signal_data.append(s)

	signals = pd.concat(signal_data, axis=1).fillna(0).astype(int)
	return signals

