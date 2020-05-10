import time
import threading
import requests
import boto3
import gzip
import os

dynamodb = boto3.resource('dynamodb')
table_name = os.environ['TABLE_NAME']
table = dynamodb.Table(table_name)

def get_lob_snapshot():
    # Get LOB snapshots for all pairs from Poloniex
    response = requests.get('https://poloniex.com/public?command=returnOrderBook&currencyPair=all&depth=100')
    if response.status_code == 200:
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
    else:
        print(response)

def lambda_handler(event, context):
    # Start at a time divisible by 10 seconds
    till_cosest_ten = 10 - int(time.time()) % 10
    time.sleep(till_cosest_ten)

    # Create 5 threads executed with 10 seconds delay each
    threads = []
    for i in range(10, 51, 10):
        thread = threading.Timer(i, get_lob_snapshot)
        thread.start()
        threads.append(thread)

    # Get one now before 5 threads above start
    get_lob_snapshot()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()