import os
import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta
import time
from decimal import Decimal
import gzip

TABLE_NAME = "LOBDumps"

pairs = [
    "BTC_ATOM",
    "BTC_BCHABC",
    "BTC_BCHSV",
    "BTC_DASH",
    "BTC_EOS",
    "BTC_ETC",
    "BTC_ETH",
    "BTC_LTC",
    "BTC_MANA",
    "BTC_STORJ",
    "BTC_STR",
    "BTC_STRAT",
    "BTC_TRX",
    "BTC_XEM",
    "BTC_XMR",
    "BTC_XRP",
    "BTC_ZEC",

    "ETH_ETC",

    "TRX_ETH",
    "TRX_XRP",

    "USDC_BTC",
    "USDC_ETH",
    "USDC_USDT",

    "USDT_BTC",
    "USDT_DASH",
    "USDT_EOS",
    "USDT_ETH",
    "USDT_GNT",
    "USDT_LTC",
    "USDT_NXT",
    "USDT_STR",
    "USDT_XMR",
    "USDT_XRP",
    "USDT_ZEC",
    "USDT_ZRX",
]

dynamodb = boto3.resource('dynamodb', region_name='eu-west-2')
table = dynamodb.Table(TABLE_NAME)
paginator = dynamodb.meta.client.get_paginator('query')

s3 = boto3.resource('s3')
bucket_name = os.environ['DUMPS_BUCKET']
bucket = s3.Bucket(bucket_name)

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

def lambda_handler(event, context):
    current_timestamp_hour = int(datetime.now().replace(minute=0, second=0, microsecond=0).timestamp())
    
    for pair in pairs:
        objects = list(bucket.objects.filter(Prefix=pair))
        
        if len(objects) > 0:
            oldest_timestamp_hour = 0
            for obj in objects:
                date = obj.key.split('/')[-1] # obj.key = 'BTC_ETH/2020/03/27/20200327_12.json.gz'
                latest_timestamp_hour = int((datetime.strptime(date, "%Y%m%d_%H.json.gz") + timedelta(hours=1)).timestamp()) #date = '20200327_12.json.gz'
                oldest_timestamp_hour = latest_timestamp_hour if latest_timestamp_hour > oldest_timestamp_hour else oldest_timestamp_hour
            
            if oldest_timestamp_hour < current_timestamp_hour:
                copy_hourly_snapshot_to_s3(pair, oldest_timestamp_hour)
                if (oldest_timestamp_hour  + 60 * 60) < current_timestamp_hour:
                    copy_hourly_snapshot_to_s3(pair, oldest_timestamp_hour + 60 * 60) # If S3 is still behind DynamoDB copy another one hour
            else:
                print(datetime.fromtimestamp(oldest_timestamp_hour).strftime(pair + ' for %Y%m%d_%H already saved to S3'))
            
        else:
            response = table.query(
                TableName=TABLE_NAME,
                KeyConditionExpression=Key('Pair').eq(pair),
                ProjectionExpression='#ts',
                ExpressionAttributeNames={ "#ts": "Timestamp" },
                Limit=1,
            )
            
            if 'Items' in response and len(response['Items']) > 0:
                oldest_timestamp = response['Items'][0]['Timestamp']
                oldest_timestamp_hour = int(datetime.fromtimestamp(oldest_timestamp).replace(minute=0, second=0, microsecond=0).timestamp())
                
                if oldest_timestamp_hour < current_timestamp_hour:
                    copy_hourly_snapshot_to_s3(pair, oldest_timestamp_hour)
                elif oldest_timestamp_hour == current_timestamp_hour:
                    print(datetime.fromtimestamp(current_timestamp_hour).strftime(pair + ' for %Y%m%d_%H is still being saved to DynamoDB'))
            else:
                print(datetime.fromtimestamp(oldest_timestamp_hour).strftime(pair + ' %Y%m%d_%H missing in DynamoDB'))

    return True

def copy_hourly_snapshot_to_s3(pair, oldest_timestamp_hour):
    snapshots = {}
    counter = 0
    pages =paginator.paginate(
        TableName=TABLE_NAME,
        KeyConditionExpression=Key('Pair').eq(pair) & Key('Timestamp').between(oldest_timestamp_hour, oldest_timestamp_hour + 60 * 60),
    )

    for page in pages:
        counter += len(page['Items'])

        for item in page['Items']:
            item_key = datetime.fromtimestamp(item['Timestamp']).strftime(pair + '-%Y%m%d_%H%M%S')
            snapshots[item_key] = item['Book']
            
    object_key = datetime.fromtimestamp(oldest_timestamp_hour).strftime(pair + '/%Y/%m/%d/%Y%m%d_%H.json.gz')
    snapshots_json = json.dumps(snapshots, cls=DecimalEncoder)
    snapshots_json_gz = gzip.compress(snapshots_json.encode('utf-8'))
    
    bucket.put_object(Key=object_key, Body=snapshots_json_gz, ContentType='application/json', ContentEncoding='gzip')

    print('Saved ' + object_key + ' to S3')
    if counter != 360:
        print('Gap in number of books for ' + object_key + ', saved only ' + str(counter) + ' snapshots')
