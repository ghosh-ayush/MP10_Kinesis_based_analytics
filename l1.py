import os
import datetime
import json
import boto3
import time
import csv
import io

STREAM_NAME    = os.environ['STREAM_NAME']
BUCKET         = os.environ['BUCKET_NAME']
KEY            = os.environ['S3_KEY']
TICKER         = os.environ.get('TICKER', 'AMD')
FOLDERS        = ['output/part_a','output/part_b','output/part_c']

s3             = boto3.resource('s3')
s3_client      = boto3.client('s3')
kinesis_client = boto3.client('kinesis', region_name='us-east-1')

def lambda_handler(event, context):
    # … your S3‐cleanup code …

    body = event.get('body') or ''
    if event.get('isBase64Encoded'):
        import base64
        body = base64.b64decode(body).decode('utf-8')

    records = []
    try:
        parsed = json.loads(body)
        if isinstance(parsed, list):
            records = parsed
        elif isinstance(parsed, dict):
            records = [parsed]
        elif isinstance(parsed, str):
            rdr = csv.DictReader(io.StringIO(parsed))
            records = list(rdr)
        else:
            records = []
    except json.JSONDecodeError:
        rdr = csv.DictReader(io.StringIO(body))
        records = list(rdr)

    for rec in records:
        generate(STREAM_NAME, kinesis_client, rec)

    return {
        'statusCode': 200,
        'body': json.dumps(f"Sent {len(records)} records")
    }

def get_data(data_dict):
    date   = data_dict.get('Date') or data_dict.get('date')
    ticker = data_dict.get('Ticker') or data_dict.get('ticker') or TICKER

    open_p = float(data_dict.get('Open') or data_dict.get('open_price') or 0)
    high_p = float(data_dict.get('High') or data_dict.get('high')       or 0)
    low_p  = float(data_dict.get('Low')  or data_dict.get('low')        or 0)
    close  = float(data_dict.get('Close') or data_dict.get('close_price') or 0)
    adj    = float(data_dict.get('Adj Close') or data_dict.get('adjclose') or 0)
    vol    = int(data_dict.get('Volume') or data_dict.get('volume')     or 0)

    return {
        'date'        : date,
        'ticker'      : ticker,
        'open_price'  : open_p,
        'high'        : high_p,
        'low'         : low_p,
        'close_price' : close,
        'adjclose'    : adj,
        'volume'      : vol,
        'event_time'  : datetime.datetime.utcnow().isoformat()
    }

def generate(stream_name, client, raw):
    payload = get_data(raw)
    client.put_record(
        StreamName   = stream_name,
        PartitionKey = payload['ticker'],
        Data         = json.dumps(payload)
    )