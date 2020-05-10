import os
import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta
import time
import gzip

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

table_name = os.environ['TABLE_NAME']
bucket_name = os.environ['DUMPS_BUCKET']

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(table_name)
paginator = dynamodb.meta.client.get_paginator('query')

s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)
cache = {}
    
def lambda_handler(event, context):
    global cache
    cache = {}
    
    current_timestamp_hour = int(datetime.now().replace(minute=0, second=0, microsecond=0).timestamp())

    latest_timestamp_hour = 0
    
    for pair in pairs:
        
        objects = list(bucket.objects.filter(Prefix=pair))
        if len(objects) == 0:
            objects = list(bucket.objects.filter(Prefix=pair))
        if len(objects) > 0:
            oldest_timestamp_hour = 0
            for obj in objects:
                date = obj.key.split('/')[-1] # obj.key = 'BTC_ETH/2020/03/27/20200327_12.json.gz'
                latest_timestamp_hour = int((datetime.strptime(date, "%Y%m%d_%H.json.gz") + timedelta(hours=1)).timestamp()) #date = '20200327_12.json.gz'
                oldest_timestamp_hour = latest_timestamp_hour if latest_timestamp_hour > oldest_timestamp_hour else oldest_timestamp_hour
            
            if oldest_timestamp_hour < current_timestamp_hour:
                copy_hourly_snapshot_to_s3(pair, oldest_timestamp_hour)
            else:
                print(datetime.fromtimestamp(oldest_timestamp_hour).strftime(pair + ' for %Y%m%d_%H already saved to S3'))

        else:
            # Nothing saved to S3, checking DynamoDB
            if latest_timestamp_hour == 0:
                response = table.query(
                    TableName=table_name,
                    KeyConditionExpression=Key('Pair').eq('Poloniex-all'),
                    ProjectionExpression='#ts',
                    ExpressionAttributeNames={ "#ts": "Timestamp" },
                    Limit=1,
                )

                if 'Items' in response and len(response['Items']) > 0:
                    latest_timestamp = response['Items'][0]['Timestamp']
                    latest_timestamp_hour = int(datetime.fromtimestamp(latest_timestamp).replace(minute=0, second=0, microsecond=0).timestamp())
                else:
                    print('Can\'t find any books in DynamoDB')
                    
            if latest_timestamp_hour < current_timestamp_hour:
                copy_hourly_snapshot_to_s3(pair, latest_timestamp_hour) # !!!!!!!!!!!!!!!!!!!!!!!!!!!!11
            elif latest_timestamp_hour == current_timestamp_hour:
                print(datetime.fromtimestamp(current_timestamp_hour).strftime('Books for ' + pair + '%Y%m%d_%H is still being saved to DynamoDB'))
                
    return True

def copy_hourly_snapshot_to_s3(pair, oldest_timestamp_hour):
    
    print('copy: ' + str(pair) + ' - ' + str(oldest_timestamp_hour))
    global cache
    if oldest_timestamp_hour not in cache:
        print('Querying DynamoDB')
        cache[oldest_timestamp_hour] = {}
        
        pages = paginator.paginate(
            TableName=table_name,
            KeyConditionExpression=Key('Pair').eq('Poloniex-all') & Key('Timestamp').between(oldest_timestamp_hour, oldest_timestamp_hour + 60 * 60 -1), # !!!!!!!!!!!!!!!!!!!!!!!!!!!!11
        )

        counter = 0
        for page in pages:
            for item in page['Items']:
                counter = counter + 1
                books_unzipped = gzip.decompress(item['Book'].value)
                books_json = json.loads(books_unzipped)
                cache[oldest_timestamp_hour][item['Timestamp']] = books_json
        print(str(counter) + ' items retrieved from DynamoDB for period starting at ' +  datetime.fromtimestamp(oldest_timestamp_hour).strftime('%Y %m %d %H:%M'))
        
    snapshots = {}
    counter = 0
    gap_tracker = oldest_timestamp_hour
    print('looping through cache[oldest_timestamp_hour]')
    for item_timestamp in cache[oldest_timestamp_hour]:
        item = cache[oldest_timestamp_hour][item_timestamp][pair]
        #print('getting item: ' + str(item_timestamp))

        #exit()
        # If there are gaps in 10 second intervals before previous item and this one fill the gap with the current snapshot
        while item_timestamp - gap_tracker > 5 : # Allow max 5 second delay for gaps in snapshots
            item_key = datetime.fromtimestamp(gap_tracker).strftime(pair + '-%Y%m%d_%H%M%S')
            snapshots[item_key] = item
            snapshots[item_key]['isFrozen'] = "1"
            print('Missing snapshot for ' + item_key)
            gap_tracker += 10

        item_key = datetime.fromtimestamp(item_timestamp).strftime(pair + '-%Y%m%d_%H%M%S')
        snapshots[item_key] = item
        gap_tracker += 10
        counter += 1

                
    object_key = datetime.fromtimestamp(oldest_timestamp_hour).strftime(pair + '/%Y/%m/%d/%Y%m%d_%H.json.gz')
    #print('saving: ' + object_key)

    snapshots_gz = gzip.compress(json.dumps(snapshots).encode('utf-8'))
    
    bucket.put_object(Key=object_key, Body=snapshots_gz, ContentType='application/json', ContentEncoding='gzip')

    print('Saved ' + object_key + ' to S3')
    if counter != 360:
        print('Gap in number of books for ' + object_key + ', saved only ' + str(counter) + ' snapshots')

    