from os import makedirs, environ, listdir, path
from time import gmtime, sleep, strftime
from datetime import datetime
from json import loads, dumps
from requests import get
from threading import Timer, Lock
import shutil

# import ptvsd
# ptvsd.enable_attach(address=('0.0.0.0', 5890), redirect_output=True)
# ptvsd.wait_for_attach()

TEMP_FOLDER = '/tmp/LOB'
makedirs(TEMP_FOLDER, exist_ok=True) 

delay = environ['DELAY']
esf_path = environ['EFS_PATH']

def get_lob_snapshot(request_id, lock, thread_number):
    # Get LOB snapshots for all pairs from Poloniex
    request_time = gmtime()
    start_datetime = datetime.utcnow()
    print(f'{request_id} | #{thread_number} | {str(start_datetime).split(" ")[1]} | start')
    response = get('https://poloniex.com/public?command=returnOrderBook&currencyPair=all&depth=100')
    print(f'{request_id} | #{thread_number} | {str(datetime.utcnow()).split(" ")[1]} | http: {response.status_code}')

    if response.status_code == 200:
        with lock:
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
                    #print(f'Snapshot {filename} appended RequestId: {request_id}')

    else:
        print(f'Error | {response}')
        print(response.content)
    print(f'{request_id} | #{thread_number} | {str(datetime.utcnow()).split(" ")[1]} | end ')
    print(f'{request_id} | #{thread_number} total: {str(datetime.utcnow() - start_datetime).split(":")[-1]} seconds')
    print('-') #log separator for easer viewing
    return

def lambda_handler(event, context):

    # starting from next miute (to mitigate delayed AWS cron starts)
    sleep(1) # ensure not first second
    till_next_minute = (60 - gmtime().tm_sec % 60) % 60
    threads = []
    lock = Lock()
    thread_number = 0
    # Start threads with 6 second interval for 10 minutes (100 in total)
    # executed 6 times an hour = 600 snapshots an hour in total
    for seconds_to_wait in range(till_next_minute + int(delay), till_next_minute + int(delay) + 10 * 60, 6):
        thread = Timer(seconds_to_wait, get_lob_snapshot, [context.aws_request_id, lock, thread_number])
        thread.start()
        threads.append(thread)
        thread_number += 1
    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    files = listdir(TEMP_FOLDER)
    for file_name in files:
        print(f'Moving {file_name} to EFS')
        today_folder = file_name.split('-')[1][0:8]
        pair = file_name.split('-')[0]
        makedirs(f'{esf_path}/{today_folder}/{pair}', exist_ok=True)
        shutil.move(f'{TEMP_FOLDER}/{file_name}', f'{esf_path}/{today_folder}/{pair}/{file_name}')

# class Object(object):
#     pass
# context = Object()
# context.aws_request_id = '84bbc9aa-2669-45ce-bc0f-2b82ef890874'
# lambda_handler({}, context)s