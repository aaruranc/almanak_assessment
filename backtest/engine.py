class Portfolio:

	def __init__(self, name, config, cash, assets):
		self.name = name
		self.config = config
		self.cash = cash
		self.assets = assets
		self.positions = {a: {'position': 0, 'cost_basis': 0} for a in assets}

	def accrue_funding(self, t, state):

		exch = self.name
		for asset in self.assets:
			if not self.positions[asset]['position']: continue
			if t == state[asset].iloc[0][f'{exch}_funding_time']:
				
				funding_rate = state[asset].iloc[0][f'{exch}_funding_prev']
				mark_px = state[asset].iloc[0][f'{exch}_mark_price']

				position = self.positions[asset]['position']
				ntl = position * mark_px

				drc = -1 if position > 0 else 1
				mod = 1/8 if exch == 'hl' else 1
				funding_payment = drc * ntl * funding_rate * mod
				self.cash += funding_payment

		return

	def update_position(self, trade):

		asset, qty, side, px = trade
		ntl = qty * px

		position = self.positions[asset]['position']
		cost_basis = self.positions[asset]['cost_basis']
		commission = self.config['fees'][self.name]
		fee = ntl * commission
		drc = 1 if side == 'buy' else -1

		# Adjust Cash by Trading Fee
		self.cash -= fee
		# Adjust Cash by Notional of Trade
		self.cash -= (drc * ntl)
		
		A = position >= 0 and side == 'buy'
		B = position <= 0 and side == 'sell'

		# Trading in Same Direction as Position
		if A or B:
			curr_ntl = position * cost_basis
			new_ntl = curr_ntl + (ntl * drc)
			new_pos = position + (qty * drc)
			new_cost_basis = new_ntl / new_pos
			self.positions[asset]['position'] = new_pos
			self.positions[asset]['cost_basis'] = new_cost_basis

		# Trading in Opposite Direction of Position
		else:
			
			trade_pnl = 0
			# Fully Close Position
			if abs(position) == qty:

				trade_pnl = (cost_basis - px) * drc * qty 

				self.positions[asset]['position'] = 0
				self.positions[asset]['cost_basis'] = 0

			# Flipping on Directional Exposure
			elif abs(position) < qty:
				
				trade_pnl = (cost_basis - px) * drc * position 

				diff = qty - abs(position)
				self.positions[asset]['position'] = diff * drc
				self.positions[asset]['cost_basis'] = px

			# Partially Closing Position
			else:

				trade_pnl = (cost_basis - px) * drc * qty 

				new_pos = position + (drc * qty)
				self.positions[asset]['position'] = new_pos

			self.cash += trade_pnl

		return

	def mark_to_market(self, state):

		mtm_equity = self.cash
		for asset in self.positions:
			mark_px = state[asset].iloc[0][f'{self.name}_mark_price']
			mtm_equity += self.positions[asset]['position'] * mark_px

		return mtm_equity


class Strategy:

	def __init__(self, config, bn_portfolio, hl_portfolio):
		self.config = config
		self.bn_port = bn_portfolio
		self.hl_port = hl_portfolio
		self.equity_curve = []

	def accrue_funding(self, t, state):

		self.bn_port.accrue_funding(t, state)
		self.hl_port.accrue_funding(t, state)
		return

	def trade_intents(self, state, target_sizes):
		
		# +1 -> Sell Hyperliquid, Buy Binance
		# -1 -> Buy Hyperliquid, Sell Binance

		intents = []
		for asset, tgt in target_sizes.items():

			bn_pos = self.bn_port.positions[asset]['position']
			spot_px = state[asset].iloc[0]['binance_spot_price']
			bn_ntl = bn_pos * spot_px
			delta = tgt - bn_ntl

			# Add Flattening Trade Only if the position carries negatively
			# (i.e. signal not above edge levels, but yield spread is positive)
			flatten_long = bn_pos > 0 and tgt <= 0
			flatten_short = bn_pos < 0 and tgt >= 0
			if flatten_long or flatten_short:

				bn_prem = state[asset].iloc[0]['binance_premium']
				hl_prem = state[asset].iloc[0]['hl_premium']
				prem_diff = hl_prem - bn_prem

				A = flatten_long and prem_diff > 0
				B = flatten_short and prem_diff < 0 
				if A or B: continue

			# Otherwise add Intent
			if abs(delta) > 1e-8:

				if delta > 0:
					trade_data = [
						[asset, abs(delta) / spot_px],
						['binance', 'buy'],
						['hl', 'sell']]
					intents.append(trade_data)

				else:
					trade_data = [
						[asset, abs(delta) / spot_px],
						['hl', 'buy'],
						['binance', 'sell']]
					intents.append(trade_data)

		return intents

	def simulate_execution(self, state, trades):

		for trade in trades:

			asset, qty = trade[0]
			buy_exch = trade[1][0]
			sell_exch = trade[2][0]

			# Cap Trade Size by Volume Participation
			buy_vlm = state[asset].iloc[0][f'{buy_exch}_perp_volume']
			sell_vlm = state[asset].iloc[0][f'{sell_exch}_perp_volume']
			trade_vlm = min(buy_vlm, sell_vlm) * self.config['max_pov']
			trade_qty = min(abs(qty), trade_vlm)

			# Slippage Adjusted Prices
			slip = self.config['slippage']
			buy_px = state[asset].iloc[0][f'{buy_exch}_perp_price'] * (1 + slip)
			sell_px = state[asset].iloc[0][f'{sell_exch}_perp_price'] * (1 - slip)

			buy_port = self.bn_port if buy_exch =='binance' else self.hl_port
			sell_port = self.bn_port if sell_exch =='binance' else self.hl_port

			buy_trd = [asset, trade_qty, 'buy', buy_px]
			buy_port.update_position(buy_trd)

			sell_trd = [asset, trade_qty, 'sell', sell_px]
			sell_port.update_position(sell_trd)

		return

	def mark_to_market(self, t, state):
		
		bn_equity = self.bn_port.mark_to_market(state)
		hl_equity = self.hl_port.mark_to_market(state)
		self.equity_curve.append([t, bn_equity + hl_equity])

		return

	def summary(self):

		return {"equity_curve": self.equity_curve}


def backtest_strategy(historical_data, signals, sizes, risk_mgr, config):

	assets = signals.columns
	initial_capital = config['starting_capital'] / 2
	bn_portfolio = Portfolio('binance', config, initial_capital, assets)
	hl_portfolio = Portfolio('hl', config, initial_capital, assets)
	strategy = Strategy(config, bn_portfolio, hl_portfolio)
	
	for t in signals.index:

		# Get Current State
		state = {a: historical_data[a][historical_data[a]['t'] == t] for a in assets}
		target_sizes = sizes.loc[t]

		# Accrue Funding
		strategy.accrue_funding(t, state)

		# Get Intended Trades
		intents = strategy.trade_intents(state, target_sizes)

		# Assess Intents Against Portfolio
		trades = risk_mgr.perform_checks(state, intents, strategy.bn_port, strategy.hl_port)
		print(t, trades)

		# Simulate Execution
		strategy.simulate_execution(state, trades)

		# Determine if Any Risk Mitigation Necessary, if so Execute Orders
		er_trades = risk_mgr.excess_risk(state, strategy.bn_port, strategy.hl_port)
		if er_trades: strategy.simulate_execution(state, er_trades)

		# Mark to Market
		strategy.mark_to_market(t, state)


	return strategy.summary()

