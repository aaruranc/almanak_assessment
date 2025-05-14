import copy

# UNIMPLEMENTED MODULE

class RiskManager:
    def __init__(self, config):
        self.config = copy.deepcopy(config)

    # Vetting Trade Intents against Portfolios
    def perform_checks(self, state, intents, bn_port, hl_port):

        trades = []
        for intent in intents:
            # Check Individual Position Limits
            # Check Portfolio Position Exposure
            # Check Cross-Exchange Basis Risk
            # Check Pre-Trade/Post-Trade Margin Risk
            # Check Liquidity across Books
            trades.append(intent)

        return intents

    # Evaluating Portfolio for Risk 
    def excess_risk(self, state, bn_port, hl_port):
        
        excess_risk_orders = []

        # Check if Drawdown Limit Breached
        # Check for Realized PnL Shocks
        # Check Margin Ratio / Leverage Limit
        # Check for Excess Concentration
        # Check for Spike in Volatility
        # Check for Liquidity Drop

        return excess_risk_orders
        



