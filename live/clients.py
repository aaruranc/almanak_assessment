import os
import copy
import ccxt
import json
import pandas as pd
import requests as rq
from abc import ABC, abstractmethod
from binance.um_futures import UMFutures

class ExchangeClient(ABC):

	@abstractmethod
	def get_balances(self): pass

	@abstractmethod
	def get_market_data(self): pass

	@abstractmethod
	def submit_order(self): pass

	@abstractmethod
	def get_open_orders(self): pass

	@abstractmethod
	def cancel_order(self): pass


class HyperliquidClient(ExchangeClient):
	def __init__(self, config):
		self.config = copy.deepcopy(config)
		
		### Not Supported on HL Testnet **
		self.config['assets'].remove('XRP')

		self.client = ccxt.hyperliquid({
			"walletAddress": os.getenv('HL_API_WALLET_ADDRESS'),
			"privateKey": os.getenv('HL_PRIVATE_KEY'),
			"enableRateLimit": True,
			"urls": {"api": {
				"public": self.config['hl_url'],
				"private":self.config['hl_url']}
				},
			})

		# For Testnet Configuration
		self.client.setSandboxMode(True)

		# Preload Markets
		self.client.load_markets()

		# Get Contract Names
		contract_names = {}
		symbols = pd.Series(self.client.symbols)		
		for asset in self.config['assets']:
			matches = symbols[symbols.str.match(f'{asset}/')]
			contract_names[asset] = matches.iloc[0]
		
		self.contract_names = contract_names

	def get_balances(self):

		## ISSUE WITH CCXT self.client.fetch_open_orders
		# Straight API Call instead

		endpoint = self.config['hl_url'] +'/info'
		headers = {"Content-Type": "application/json"}
		payload = {
			'type': "clearinghouseState",
			'user': os.getenv('HL_WALLET_ADDRESS')
			}

		r = rq.post(url=endpoint, headers=headers, data=json.dumps(payload))
		data = r.json()

		positions = {}
		for data in r.json()['assetPositions']:
			positions[data['position']['coin']] ={
				'position': float(data['position']['szi']),
				'cost_basis': float(data['position']['entryPx'])
			}

		cash = float(r.json()['marginSummary']['totalRawUsd'])
		return cash, positions

	def get_market_data(self):

		market_data = {}
		for asset in self.config['assets']:
			contract = self.contract_names[asset]
			market_data[asset] = {
				'ticker': self.client.fetch_ticker(contract),
				'book': self.client.fetch_order_book(contract),
				'funding': self.client.fetch_funding_rate_history(contract, limit=1) 
			}

		return market_data

	def submit_order(self, order):

		return self.client.createOrder(
				symbol=self.contract_names[order['asset']], 
				type=order['type'], 
				side=order['side'],
				amount=order['amount'],
				price=order['price']
				)

	def get_open_orders(self):

		## ISSUE WITH CCXT self.client.fetch_open_orders
		# Straight API Call instead

		endpoint = self.config['hl_url'] +'/info'
		headers = {"Content-Type": "application/json"}
		payload = {
			'type': "openOrders",
			'user': os.getenv('HL_WALLET_ADDRESS')
			}

		r = rq.post(url=endpoint, headers=headers, data=json.dumps(payload))
		
		open_orders = {}
		for order in r.json():

			if order['coin'] not in open_orders:
				open_orders[order['coin']] = []

			open_orders[order['coin']].append({
				'oid': order['oid'],
				'side': 'buy' if order['side'] == 'B' else 'sell',
				'price': float(order['limitPx']),
				'size': float(order['sz'])
				})

		return open_orders

	def cancel_order(self, asset, order_id):
		return self.client.cancelOrder(
				id=order_id, 
				symbol=self.contract_names[asset]
				)


class BinanceClient(ExchangeClient):
	def __init__(self, config):
		self.config = copy.deepcopy(config)
		
		### Not Supported on HL Testnet **
		self.config['assets'].remove('XRP')

		self.client = UMFutures(
				key=os.getenv('BINANCE_API_KEY'),
				secret=os.getenv('BINANCE_API_SECRET'),
				base_url=self.config['binance_url']
				)

		# Set Contract Names
		self.contract_names = {a: f"{a}USDT" for a in config['assets']}
		
	def get_balances(self):

		account_data = self.client.account()
		cash = float(account_data['totalWalletBalance'])

		positions = {}
		for data in self.client.get_position_risk():	
			positions[data['symbol'][:-4]] = {
				'position': float(data['positionAmt']),
				'cost_basis': float(data['entryPrice'])
			}

		return cash, positions

	def get_market_data(self):

		market_data = {}
		for asset in self.config['assets']:
			contract = self.contract_names[asset]
			market_data[asset] = {
				'ticker': self.client.book_ticker(symbol=contract),
				'book': self.client.depth(symbol=contract, limit=10),
				'funding': self.client.funding_rate(symbol=contract, limit=1) 
			}

		return market_data

	def submit_order(self, order):

		if order['type'] == 'limit':
			return self.client.new_order(
				symbol=self.contract_names[order['asset']],
				type=order['type'].upper(),
				side=order['side'].upper(),
				quantity=order['amount'],
				price=order['price'],
				timeInForce="GTC"
				)
		elif order['type'] == 'market':
			return self.client.new_order(
				symbol=self.contract_names[order['asset']],
				type=order['type'].upper(),
				side=order['side'].upper(),
				quantity=order['amount']
				)

	def get_open_orders(self):

		open_orders = {}
		for asset in self.config['assets']:
			contract = self.contract_names[asset]
			data = self.client.get_orders(symbol=contract)
			if not data: continue

			if asset not in open_orders:
				open_orders[asset] = []

			for order in data:
				open_orders[asset].append({
					'oid': order['orderId'],
					'side': order['side'].lower(),
					'price': float(order['price']),
					'size': float(order['origQty']) - float(order['executedQty'])
					})

		return open_orders

	def cancel_order(self, asset, order_id):
		return self.client.cancel_order(
				symbol=self.contract_names[asset],
				orderId=order_id
				)
