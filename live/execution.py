import copy
import json
import pandas as pd

from strategy.signal import generate_signals
from strategy.sizing import compute_sizes

class Portfolio:

	def __init__(self, name, config, cash, assets, positions):
		self.name = name
		self.config = config
		self.cash = cash
		self.assets = assets
		self.positions = positions


class Strategy:
	def __init__(self, config, logger, bn_client, hl_client, risk_mgr):
		self.config = copy.deepcopy(config)
		self.logger = logger
		self.bn_client = bn_client
		self.hl_client = hl_client
		self.risk_mgr = risk_mgr

		### Not Supported on HL Testnet **
		self.config['assets'].remove('XRP')

		# Get Current Cash and Positions
		bn_cash, bn_positions = self.bn_client.get_balances()
		hl_cash, hl_positions = self.hl_client.get_balances()

		# Add in Empty Positions
		for asset in self.config['assets']:
			if asset not in bn_positions: bn_positions[asset] = {'position': 0, 'cost_basis': 0}
			if asset not in hl_positions: hl_positions[asset] = {'position': 0, 'cost_basis': 0}

		# Initialize Portfolio Objects
		self.bn_port = Portfolio('binance', config, bn_cash, self.config['assets'], bn_positions)
		self.hl_port = Portfolio('hl', config, hl_cash, self.config['assets'], hl_positions)
		self.logger.info(json.dumps({
			'event': 'position_snapshot',
			'bn_cash': self.bn_port.cash,
			'hl_cash': self.hl_port.cash,
			'bn_positions': self.bn_port.positions,
			'hl_positions': self.hl_port.positions
		}))

		self.market_data = None
		self.orders = {}

	def cancel_orders(self):

		# Get Open HL Orders and Cancel Them
		hl_open_orders = self.hl_client.get_open_orders()
		for asset in hl_open_orders:
			for order in hl_open_orders[asset]:
				try: 
					self.hl_client.cancel_order(asset, order['oid'])
					self.logger.info(json.dumps({
						'event': 'order_cancel',
						'asset': asset,
						'exch': 'hl',
						'order_data': order
					}))
				except: pass

		# Get Open BN Orders and Cancel Them
		bn_open_orders = self.bn_client.get_open_orders()
		for asset in bn_open_orders:
			for order in bn_open_orders[asset]:
				try:
					self.bn_client.cancel_order(asset, order['oid'])
					self.logger.info(json.dumps({
						'event': 'order_cancel',
						'asset': asset,
						'exch': 'binance',
						'order_data': order
					}))

				except:
					pass

		self.logger.info(json.dumps({
			'event': 'live_orders',
			'order_data': self.orders
		}))

		return

	def get_market_data(self):

		self.market_data = {
			'binance': self.bn_client.get_market_data(),
			'hl': self.hl_client.get_market_data()
		}

		self.logger.info(json.dumps({
			'event': 'market_data',
			'binance': self.market_data['binance'],
			'hl': self.market_data['hl']
		}))

		return

	def generate_signal_data(self):

		# HACKY WAY TO MAKE SURE TIMESTAMPS ALIGNED
		max_t = None
		for asset in self.config['assets']:
			t = float(self.market_data['binance'][asset]['ticker']['time'])
			if not max_t or t > max_t: max_t = t

		signal_data = {}
		for asset in self.config['assets']:		
			
			bn_prev_funding = self.market_data['binance'][asset]['funding'][0]['fundingRate']
			hl_prev_funding = self.market_data['hl'][asset]['funding'][0]['fundingRate']
				
			d = {'t': max_t, 
				'binance_funding_prev': float(bn_prev_funding),
				'hl_funding_prev': float(hl_prev_funding)}
			signal_data[asset] = pd.DataFrame([d])
				
		return signal_data

	def refresh_positions(self):

		# Update Binance Positions
		bn_cash, bn_positions = self.bn_client.get_balances()
		self.bn_port.cash = bn_cash
		for asset in bn_positions:
			self.bn_port.positions[asset] = bn_positions[asset]

		# Update Hyperliquid Positions
		hl_cash, hl_positions = self.hl_client.get_balances()
		self.hl_port.cash = hl_cash
		for asset in hl_positions:
			self.hl_port.positions[asset] = hl_positions[asset]

		self.logger.info(json.dumps({
			'event': 'position_snapshot',
			'bn_cash': self.bn_port.cash,
			'hl_cash': self.hl_port.cash,
			'bn_positions': self.bn_port.positions,
			'hl_positions': self.hl_port.positions
		}))

		return


	def get_trade_intents(self, target_sizes):

		# +1 -> Sell Hyperliquid, Buy Binance
		# -1 -> Buy Hyperliquid, Sell Binance

		self.refresh_positions()

		intents = []
		for asset, tgt in target_sizes.iloc[0].items():

			bn_pos = self.bn_port.positions[asset]['position']
			bn_ticker_data = self.market_data['binance'][asset]['ticker']
			bn_mid = (float(bn_ticker_data['bidPrice']) + float(bn_ticker_data['askPrice'])) / 2
			bn_ntl = bn_pos * bn_mid

			# At or Above Risk Target
			A = tgt > 0 and bn_ntl >= tgt
			B = tgt < 0 and bn_ntl <= tgt  
			if A or B: continue
			
			tgt_delta = tgt - bn_ntl
			tgt_delta_units = tgt_delta / bn_mid

			# Caping Size based on Top of Hedge Order Book
			hedgeable_qty = float(bn_ticker_data['askQty']) if tgt > 0 else float(bn_ticker_data['bidQty'])
			trade_size = min(abs(tgt_delta_units), hedgeable_qty)

			# No Hedge Liquidity
			if trade_size == 0: continue

			side = 'sell' if tgt > 0 else 'buy'
			intents.append([asset, side, trade_size])
			self.logger.info(json.dumps({
				'event': 'order_intent',
				'asset': asset,
				'side': side,
				'trade_size': trade_size
			}))
		
		return intents

	def create_orders(self, intents):

		# Send out Non-Hedge Orders
		for intent in intents:

			# Skip if actively working an order
			if intent[0] in self.orders: continue
			
			# Use Last Price if no BBO
			hl_ticker_data = self.market_data['hl'][intent[0]]['ticker']
			hl_px = hl_ticker_data['last']

			# In Case of Empty Order Book
			try:
				bid = self.market_data['hl'][intent[0]]['book']['bids'][0][0]
				ask = self.market_data['hl'][intent[0]]['book']['asks'][0][0]			
				hl_px = (bid + ask) / 2
			except: pass			

			order = {
				'asset': intent[0],
				'type': 'limit',
				'side': intent[1],
				'amount': intent[2],
				'price': hl_px,
			}

			try:
				r = self.hl_client.submit_order(order)
				if intent[0] not in self.orders: self.orders[intent[0]] = {}
				self.orders[intent[0]]['hl'] = {r['id']: order}
				self.logger.info(json.dumps({
					'event': 'order_submit',
					'asset': asset,
					'exch': 'hl',
					'order_data': order
				}))

			except:
				pass

		self.logger.info(json.dumps({
			'event': 'live_orders',
			'order_data': self.orders
		}))

		return



	def hedge_exposure(self):

		# Calculate Net Delta
		deltas = {}
		for asset in self.config['assets']:
			bn_pos = self.bn_port.positions[asset]['position']
			hl_pos = self.hl_port.positions[asset]['position']
			residual = bn_pos + hl_pos

			bn_ticker_data = self.market_data['binance'][asset]['ticker']
			bn_mid = (float(bn_ticker_data['bidPrice']) + float(bn_ticker_data['askPrice'])) / 2
			residual_ntl = residual * bn_mid

			if abs(residual_ntl) > self.config['hedge_threshold']:
				# HACKY - Should store/query precision 
				deltas[asset] = round(residual, 2)

		# Send Hedge Orders
		for asset in deltas:
			
			side = 'buy' if deltas[asset] < 0 else 'sell'
			order = {
				'asset': asset,
				'type': 'market',
				'side': side,
				'amount': abs(deltas[asset]),
			}

			# Assuming Guaranteed Exection of Taker Hedge
			try:
				self.bn_client.submit_order(order)
				self.logger.info(json.dumps({
					'event': 'execute_hedge',
					'asset': asset,
					'exch': 'binance',
					'order_data': order
				}))

			except:
				pass

		return

	def manage_orders(self):	

		hl_open_orders = self.hl_client.get_open_orders()

		for asset in self.config['assets']:
			# Previously Sent Live Order
			if asset in self.orders:
				# Fully Filled
				if asset not in hl_open_orders:
					del self.orders[asset]


				# Partially Filled
				else:

					# Only Working 1 Order at a Time
					asset_order = hl_open_orders[asset][0]
					oid = asset_order['oid']
					size = asset_order['size']
					price = asset_order['price']
					side = asset_order['side']

					# Use Last Price if no BBO
					hl_ticker_data = self.market_data['hl'][asset]['ticker']
					px = hl_ticker_data['last']

					# In Case of Empty Order Book
					try:
						bid = self.market_data['hl'][asset]['book']['bids'][0][0]
						ask = self.market_data['hl'][asset]['book']['asks'][0][0]			
						px = (bid + ask) / 2
					except: pass	

					# Check if outside of BBO
					A = side == 'sell' and price > ask
					B = side == 'buy' and price < bid
					if A or B:
						
						# Cancel Previous Order
						self.hl_client.cancel_order(asset, oid)
						self.orders[asset] = {}
						self.logger.info(json.dumps({
							'event': 'order_cancel',
							'asset': asset,
							'exch': 'hl',
							'order_data': asset_order
						}))

						try:

							order = {
								'asset': asset,
								'type': 'limit',
								'side': side,
								'amount': size,
								'price': px,
							}
						
							# Submit Order with Updated Price
							r = self.hl_client.submit_order(order)
							self.orders[asset]['hl'] = {r['id']: order}
							self.logger.info(json.dumps({
								'event': 'order_submit',
								'asset': asset,
								'exch': 'hl',
								'order_data': order
							}))

						except:
							pass

		self.logger.info(json.dumps({
			'event': 'live_orders',
			'order_data': self.orders
		}))
		
		return

	def perform_checks(self, intents):

		return self.risk_mgr.perform_checks(self.market_data, intents, self.bn_port, self.hl_port)

	def excess_risk(self):

		return self.risk_mgr.excess_risk(self.market_data, self.bn_port, self.hl_port)


	
def execution_loop(strategy, config):

	# Cancel Any Live Orders from Previous Session
	strategy.cancel_orders()
	
	# Pull Live Market Data
	strategy.get_market_data()
	
	# Ensure Delta Neutrality at Initiation
	strategy.hedge_exposure()

	while True:

		# Pull Live Market Data
		strategy.get_market_data()

		# Extract Signal Data
		signal_data = strategy.generate_signal_data()

		# Generate Signals
		signals = generate_signals(signal_data, config)
		
		# Compute Position Sizes
		target_sizes = compute_sizes(signals, config)

		# Determine Intended Trades
		intents = strategy.get_trade_intents(target_sizes)

		# Assess Intents Against Portfolio
		trades = strategy.perform_checks(intents)

		# Create Maker Orders on HL
		strategy.create_orders(trades)

		# Update Positions
		strategy.refresh_positions()

		# Hedge Any Fills
		strategy.hedge_exposure()

		# Determine if Any Risk Mitigation Necessary, if so Execute Orders
		er_trades = strategy.excess_risk()
		if er_trades: strategy.create_orders(er_trades)

		# Manage Open Orders
		strategy.manage_orders()


	return
