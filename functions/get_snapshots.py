import time
import threading
import requests
import json
import os

# import ptvsd
# ptvsd.enable_attach(address=('0.0.0.0', 5890), redirect_output=True)
# ptvsd.wait_for_attach()

TEMP_FOLDER = '/mnt/efs/temp'
os.makedirs(TEMP_FOLDER, exist_ok=True) 

def get_lob_snapshot(request_id):
    # Get LOB snapshots for all pairs from Poloniex
    request_time = time.gmtime()
    print(time.strftime(f'Thread executed %H-%M-%S RequestId: {request_id}', request_time))

    response = requests.get('https://poloniex.com/public?command=returnOrderBook&currencyPair=all&depth=100')
    if response.status_code == 200:
        all_books = json.loads(response.text)
        for pair in all_books:
            this_hour = time.strftime('%Y%m%d_%H', request_time)
            this_hour_folder = f'{TEMP_FOLDER}/{pair}/{this_hour}'
            os.makedirs(this_hour_folder, exist_ok=True) 
            this_minute_second = time.strftime('%M%S', request_time)
            book = all_books[pair]

            filename = f'{this_hour_folder}/{this_minute_second}.json'

            with open(filename, 'w') as outfile:
                json.dump(book, outfile)
                print(time.strftime(f'Snapshot {filename} saved RequestId: {request_id}', request_time))

    else:
        print(time.strftime(f'Books snapshot for %H-%M-%S download error RequestId: {request_id}', request_time))
        print(response)
        print(response.content)


def lambda_handler(event, context):
    # starting from next miute (to mitigate delayed AWS cron starts)
    time.sleep(1) # ensure not first second
    till_next_minute = (60 - time.gmtime().tm_sec % 60) % 60
    threads = []

    # Start threads with 10 second interval for 12 minutes (72 in total)
    # executed 5 times an hour = 360 snapshots an hour in total
    for seconds_to_wait in range(till_next_minute, till_next_minute + 12 * 60, 10):
        thread = threading.Timer(seconds_to_wait, get_lob_snapshot, [context.aws_request_id])
        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()