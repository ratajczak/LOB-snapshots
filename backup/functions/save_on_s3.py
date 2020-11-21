from os import makedirs, environ, listdir, remove, path
from time import gmtime, sleep, strftime
from json import loads, dumps
import requests
from time import time
from threading import Timer
from boto3 import client
import gzip

# import ptvsd
# ptvsd.enable_attach(address=('0.0.0.0', 5890), redirect_output=True)
# ptvsd.wait_for_attach()

TEMP_FOLDER = '/tmp/LOB'
makedirs(TEMP_FOLDER, exist_ok=True) 

#bucket = 'limit-order-books-poloniex-backup-limitorderbooks-1b4kpx6y6lq48'
bucket = environ['BUCKET']

s3_client = client('s3')

def get_books(request_id, temp_file):

    # Get LOB snapshots for all pairs from Poloniex
    request_time = gmtime()
    print(strftime(f'Thread executed %H-%M-%S RequestId: {request_id}', request_time))

    response = requests.get('https://poloniex.com/public?command=returnOrderBook&currencyPair=all&depth=100')
    if response.status_code == 200:
        all_books = response.content
        json_key = strftime('%Y%m%d_%H%M%S', request_time)

        # comma in front if not earliest thread
        if temp_file.tell() == 0:
            book_json_string = b'{"' + bytes(json_key, 'utf-8') + b'":' + all_books
        else:
            book_json_string = b',\n"' + bytes(json_key, 'utf-8') + b'":' + all_books

        temp_file.write(book_json_string)

    else:
        print(strftime(f'Books snapshot for %H-%M-%S download error RequestId: {request_id}', request_time))
        print(response)
        print(response.content)
    print('Thread end')
    return

def lambda_handler(event, context):

    now_plus_one_minute = gmtime(time() + 60) # Lambda starts one minute earlier
    this_hour = strftime('%Y%m%d_%H', now_plus_one_minute)
    part = int(now_plus_one_minute.tm_min / 6) # Lambda starts 60 / 6 = 10 times a hour

    temp_file_name = f'{TEMP_FOLDER}/{this_hour}-{part}.json'
    temp_file = open(temp_file_name, 'ab+')

    threads = []
    # Starting from next miute (to mitigate delayed AWS cron starts) with 4 second interval for 6 minutes (90 in total)
    till_next_minute = (60 - gmtime().tm_sec % 60) % 60
    for seconds_to_wait in range(till_next_minute, till_next_minute + 6 * 60, 4):
        thread = Timer(seconds_to_wait, get_books, [context.aws_request_id, temp_file])
        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    print('All threads finished')

    temp_file.write(b'}') # close json afer all trhreads appended

    temp_file.seek(0) # rewind to the beginning of the file
    compressed_file = gzip.open(f'{temp_file_name}.gz', 'wb')

    while True:
        data = temp_file.read(10*2**20) # 10MB chunks
        if not data:
            break
        compressed_file.write(data)

    temp_file.close()
    compressed_file.close()

    print('Temporary file compressed')

    print(listdir(TEMP_FOLDER))

    year = this_hour[0:4]
    month = this_hour[4:6]
    day = this_hour[6:8]
    key = f'{year}/{month}/{day}/{this_hour}-{part}.json.gz'

    try:
        s3_client.upload_file(
            f'{temp_file_name}.gz',
            bucket,
            key,
            ExtraArgs={'ContentType': 'application/json', 'ContentEncoding': 'gzip'}
        )

        print(f'Saved {key}')
        remove(f'{temp_file_name}')
        remove(f'{temp_file_name}.gz')

    except Exception as e:
        print(f'Error during copying {temp_file_name}.gz to S3')
        print(e)
    return

# context = type('', (), {})()
# context.aws_request_id = "abc"
# lambda_handler(context, context)