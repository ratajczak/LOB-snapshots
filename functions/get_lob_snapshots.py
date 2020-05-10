import time
import threading
import requests
import boto3
import gzip
import os

dynamodb = boto3.resource('dynamodb')
table_name = os.environ['TABLE_NAME']
table = dynamodb.Table(table_name)

def get_lob_snapshot(request_id):
    # Get LOB snapshots for all pairs from Poloniex
    response = requests.get('https://poloniex.com/public?command=returnOrderBook&currencyPair=all&depth=100')
    if response.status_code == 200:
        print('LOB snapshot downloaded successfully for RequestId: ' + request_id)
        # Compress json and save in DynamoDB
        snapshot_gz = gzip.compress(response.text.encode('utf-8'))
        current_timestamp = int(time.time())
        table.put_item(
        Item = {
                'Pair': 'Poloniex-all',
                'Timestamp': current_timestamp,
                'Book': snapshot_gz,
                'TTL': current_timestamp + (60 * 60 * 72) #72 hours
            }
        )
        print('LOB snapshot saved to DynamoDB for RequestId: ' + request_id)
    else:
        print('LOB snapshot download error for RequestId: ' + request_id)
        print(response)

def lambda_handler(event, context):
    # Start at a time divisible by 10 seconds
    current_second = time.gmtime().tm_sec

    print('tm_sec:')
    print(time.gmtime().tm_sec)

    if current_second > 10:
        print(time.strftime('Lambda execution delayed start at %H:%M:%S for RequestId: ' + context.aws_request_id, time.gmtime()))
    time.sleep(10 - current_second % 10)

    # Create 5 threads executed with 10 seconds delay each
    threads = []
    for i in range(10, 51, 10):
        thread = threading.Timer(i, get_lob_snapshot, [context.aws_request_id])
        thread.start()
        threads.append(thread)

    # Get one now before 5 threads above start
    get_lob_snapshot(context.aws_request_id)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()