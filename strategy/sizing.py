def compute_sizes(signals, config):

	# Simply equally weights all signals to position cap
	# Safety Cap (in case of incorrect config)

	sizes = signals.astype(float) * config["notional_per_trade"]
	cap = config["max_position_size"]
	sizes = sizes.clip(lower=-cap, upper=cap)

	return sizes

