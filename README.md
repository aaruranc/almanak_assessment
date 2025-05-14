# Funding Rate Arbitrage POC

## Overview
This repository contains a proof-of-concept funding-rate arbitrage strategy between Hyperliquid and Binance. It contains both a backtesting framework and a live trading / monitoring system. 

# How to Run

## Environment File
Install all Necessary Packages with 'pip install -r requirements.txt' in shell.
Create a .env file with your credentials for each of the following: 
1. AWS_ACCESS_KEY_ID
2. AWS_SECRET_ACCESS_KEY
3. BINANCE_API_KEY
4. BINANCE_API_SECRET
5. HL_WALLET_ADDRESS
6. HL_API_WALLET_ADDRESS
7. HL_PRIVATE_KEY

Note that AWS Credentials are required for pulling from Hyperliquid S3 Buckets. AWS credentials do not need to be added if you don't plan on downloading data.

## Backtest

To run the backtesting system use the following command: ''' python run_backtest.py '''
You can optionally download historical data by adding a -d parameter: ''' python run_backtest.py -d '''
Results will output in results/

## Live

To run the live system use the following command: ''' python run_live.py '''
After the live system is initialized, run the followign to launch monitor: ''' python live_monitor.py '''
Browse to http://<monitor_host>:<monitor_port>/ to see positions & open orders, auto-refreshing every 3 s.

## Limitations, Expected Returns & Risks

With aggressive backtext parameters, the performance is basically breakeven. The Sharp was 0.09 and annualized return was 0.37%. While this is a toy example, there are a couple clear explanations for why the performance is weak. We are only trading a small universe of the largest/most liquid tokens. We expect these to be the most efficiently priced unlike the smaller cap tokes. This backtest was done on the first few months of 2025 which was notably a period of depressed/negative funding. Beyond that, this system is naive in the sense of expecting the next funding period to be eual to the previous (i.e. rules based logic instead of model driven) - adding a predictive component would shift things drastically. Similarly, downside risk would be capped with a fully implemented risk management module. Lastly, integrating several exchanges and choosing optimal hedges along with dynamic position sizes relative to signal would improve the performance. The general risks come from significant slippage, counterparty/smart contract risk with Hyperliquid, margin risk in high leverage situations, cross exchange basis risk, capacity constraints, and unexpected spikes in funding or volatility among other things.


## Extensions
- Implemented Risk Management Module
- Develop Predictive Funding Model
- Dynamic Sizing Relative to Edge Level
- Expanded universe of assets
- Testing Suite
- Margin / Liquidity Management and Optimized Leverage
- Using higher granularity historical data (L2 books)
