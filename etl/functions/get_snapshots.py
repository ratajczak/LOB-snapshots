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
delay = environ['DELAY']
s3_client = client('s3')

def get_lob_snapshot(request_id):
    # Get LOB snapshots for all pairs from Poloniex
    request_time = gmtime()
    print(strftime(f'Thread executed %H-%M-%S RequestId: {request_id}', request_time))

    response = get('https://poloniex.com/public?command=returnOrderBook&currencyPair=all&depth=100')
    if response.status_code == 200:
        all_books = loads(response.text)
        for pair in all_books:
            book = all_books[pair]

            this_hour = strftime('%Y%m%d_%H', request_time)
            this_minute_second = strftime('%M%S', request_time)
            part = int(request_time.tm_min / 10) # Lambda fires 60 / 10 = 6 times a hour

            filename = f'{TEMP_FOLDER}/{pair}-{this_hour}-{part}'

            # comma in front if appending
            if path.exists(filename):
                book_json_string = f',"{pair}-{this_hour}{this_minute_second}": {dumps(book)}'
            else:
                book_json_string = f'"{pair}-{this_hour}{this_minute_second}": {dumps(book)}'

            with open(filename, 'a') as outfile:
                outfile.write(book_json_string)
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

    # Start threads with 10 second interval for 10 minutes (60 in total)
    # executed 6 times an hour = 360 snapshots an hour in total
    for seconds_to_wait in range(till_next_minute + int(delay), till_next_minute + int(delay) + 10 * 60, 6):
        thread = Timer(seconds_to_wait, get_lob_snapshot, [context.aws_request_id])

        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    files = listdir(TEMP_FOLDER)
    for file_name in files:
    #    print(file_name)

        file_name_split = file_name.split('-')

        pair = file_name_split[0]
        part = file_name_split[2]
        datetime_hour = file_name_split[1]
        year = datetime_hour[0:4]
        month = datetime_hour[4:6]
        day = datetime_hour[6:8]

        key = f'{pair}/{year}/{month}/{day}/{datetime_hour}-{part}-{delay}.json.gz'

        with open(f'{TEMP_FOLDER}/{file_name}', 'rb') as in_file:
            books = b'{' + in_file.read() + b'}' 
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