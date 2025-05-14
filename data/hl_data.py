import os
import yaml
import boto3
import lz4.frame
import pandas as pd
import multiprocessing as mp

from io import BytesIO
from pathlib import Path
from datetime import timedelta
from dotenv import load_dotenv, find_dotenv


def list_dates(start, end):
	dates = []
	cur = start.date()
	while cur <= end.date():
		dates.append(cur.strftime("%Y%m%d"))
		cur += timedelta(days=1)
	return dates


def get_hl_data(config):

	s3 = boto3.client(
		"s3",
		aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
		aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
		)

	for date_str in list_dates(config['start'], config['end']):

		try:

			key = f"asset_ctxs/{date_str}.csv.lz4"
			resp = s3.get_object(
				Bucket="hyperliquid-archive",
				Key=key,
				RequestPayer="requester"
				)
			
			compressed = resp["Body"].read()
			raw_bytes  = lz4.frame.decompress(compressed)
			df = pd.read_csv(BytesIO(raw_bytes))

			path = os.path.join(BASE_DIR, 'data', 'historical', 'raw', 'perp', 'hyperliquid', 'asset_ctxt')
			if not os.path.exists(path): os.makedirs(path)
			df.to_csv(f'{path}/{date_str}.csv', index=False)

		# Data Not Uploaded to S3 Bucket
		except Exception as e:
			print(date_str, 'asset_ctxt', e)

	return
