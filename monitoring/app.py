import json
import yaml
from pathlib import Path
from collections import deque
from fastapi import FastAPI, Response

# Load Config
BASE_DIR = Path(__file__).resolve().parent.parent
with open(BASE_DIR / "config.yaml", "r") as f: config = yaml.safe_load(f)

# Load Log File
LOGFILE = Path(__file__).parent.parent / "logs" / "production.log"

def tail_log_entries():
	with LOGFILE.open("r", encoding="utf-8") as f:
		last_lines = deque(f, maxlen=config["tail_lines"])
	return list(last_lines)

def parse_entries(lines):
	entries = []
	for line in lines:
		try:
			ts, level, payload = line.strip().split(' ', 2)
			data = json.loads(payload)
			data["timestamp"] = ts
			data["level"] = level
			entries.append(data)
		except Exception:
			continue
	return entries

def aggregate_state(entries):

	state = {"positions": {}, "live_orders": {}}
	for e in entries:
		evt = e.get("event")
		if evt == "position_snapshot":
			state["positions"] = {
				"bn_cash": e["bn_cash"],
				"hl_cash": e["hl_cash"],
				"bn_positions": e["bn_positions"],
				"hl_positions": e["hl_positions"]
			}
		elif evt == "live_orders":
			state["live_orders"] = e["order_data"]
	return state


app = FastAPI()

@app.get('/')
async def home():

	lines = tail_log_entries()
	entries = parse_entries(lines)
	state = aggregate_state(entries)

	# build HTML
	html = [
		"<html><head>",
		'<meta http-equiv="refresh" content="2">',
		"<style>table,th,td{border:1px solid #ccc;border-collapse:collapse;padding:4px}</style>",
		"</head><body>"
	]

	# Positions table
	html.append(
		"<h2>Positions</h2><table><tr>"
		"<th>Exchange</th><th>Asset</th><th>Size</th><th>Cost Basis</th></tr>"
	)
	for exch in ("bn_positions","hl_positions"):
		for asset, pos in state["positions"].get(exch, {}).items():
			
			size = pos["position"]
			cb = pos.get("cost_basis","—")
			html.append(
				f"<tr><td>{exch[:2].upper()}</td><td>{asset}</td>"
				f"<td>{size}</td><td>{cb}</td></tr>"
			)
	html.append("</table>")
	html.append("</br>")

	# Open orders table
	html.append(
		"<h2>Open Orders</h2><table>"
		"<tr><th>Asset</th><th>Side</th><th>Amount</th><th>Price</th>"
	)

	# print(state['live_orders'])
	for asset in state["live_orders"]:
		for oid in state["live_orders"][asset]['hl']:
			html.append(
				"<tr>"
				f"<td>{asset}</td>"
				f"<td>{state['live_orders'][asset]['hl'][oid].get('side')}</td>"
				f"<td>{state['live_orders'][asset]['hl'][oid].get('amount')}</td>"
				f"<td>{state['live_orders'][asset]['hl'][oid].get('price')}</td>"
				
				"</tr>"
			)
	html.append("</table>")
	html.append("</body></html>")
	return Response(content="".join(html), media_type="text/html")