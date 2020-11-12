from os import makedirs, environ, listdir, remove, path
from time import gmtime, sleep, strftime
from json import loads, dumps
from requests import get
from threading import Timer
from boto3 import client
from gzip import compress

# import ptvsd
# ptvsd.enable_attach(address=('0.0.0.0', 5890), redirect_output=True)
# ptvsd.wait_for_attach()

TEMP_FOLDER = '/tmp/LOB'
makedirs(TEMP_FOLDER, exist_ok=True) 

bucket = environ['BUCKET']
s3_client = client('s3')

def get_books(request_id):

    # Get LOB snapshots for all pairs from Poloniex
    request_time = gmtime()
    print(strftime(f'Thread executed %H-%M-%S RequestId: {request_id}', request_time))

    response = get('https://poloniex.com/public?command=returnOrderBook&currencyPair=all&depth=100')
    if response.status_code == 200:
        all_books = response.content

        this_hour = strftime('%Y%m%d_%H', request_time)
        this_minute_second = strftime('%M%S', request_time)
        part = int(request_time.tm_min / 12) # Lambda fires 60 / 12 = 5 times a hour

        file_name = f'{TEMP_FOLDER}/{this_hour}-{part}'

        # comma in front if appending
        if path.exists(file_name):
            book_json_string = b',"' + bytes(this_hour, 'utf-8') + bytes(this_minute_second, 'utf-8') + b'":' + all_books
        else:
            book_json_string = b'"' + bytes(this_hour, 'utf-8') + bytes(this_minute_second, 'utf-8') + b'":' + all_books

        with open(file_name, 'ab') as outfile:
            outfile.write(all_books)
            outfile.close()
            #print(strftime(f'Snapshot {filename} appended RequestId: {request_id}', request_time))

    else:
        print(strftime(f'Books snapshot for %H-%M-%S download error RequestId: {request_id}', request_time))
        print(response)
        print(response.content)
    print('Thread end')
    return

def lambda_handler(event, context):
    # starting from next miute (to mitigate delayed AWS cron starts)
    sleep(1) # ensure not first second
    till_next_minute = (60 - gmtime().tm_sec % 60) % 60
    threads = []

    # Start threads with 10 second interval for 12 minutes (60 in total)
    # executed 6 times an hour = 360 snapshots an hour in total
    for seconds_to_wait in range(till_next_minute, till_next_minute + 12 * 60, 5):
        thread = Timer(seconds_to_wait, get_books, [context.aws_request_id])

        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    files = listdir(TEMP_FOLDER)
    for file_name in files:

        year = file_name[0:4]
        month = file_name[4:6]
        day = file_name[6:8]

        key = f'{year}/{month}/{day}/{file_name}.json.gz'

        with open(f'{TEMP_FOLDER}/{file_name}', 'rb') as in_file:
            books_file = in_file.read()
            books = b'{' + books_file + b'}' 
            zipped_books = compress(books)

            try:
                s3_client.put_object(
                    Bucket=bucket,
                    Body=zipped_books,
                    Key=key,
                    ContentType = 'application/json',
                    ContentEncoding = 'gzip'
                )

                print(f'Saved {key}')
                remove(f'{TEMP_FOLDER}/{file_name}') 

            except Exception as e:
                print(f'Error during copying {file_name} to S3')
                print(e)