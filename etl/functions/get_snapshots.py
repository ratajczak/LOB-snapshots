from os import makedirs, environ
from time import gmtime, sleep, strftime
from datetime import datetime
from requests import adapters, Session
from threading import Timer, Lock
import polars as pl
import io

# environ['SYMBOL0'] = 'ETH_BTC'
# environ['SYMBOL1'] = 'ETC_BTC'
# environ['SYMBOL2'] = 'LTC_BTC'
# environ['SYMBOL3'] = 'NXT_BTC'
# environ['SYMBOL4'] = 'TRX_BTC'
# environ['EFS_PATH'] = '/tmp'
# environ['DELAY'] = '0'

SYMBOLS = []
SYMBOLS.append(environ['SYMBOL0'])
SYMBOLS.append(environ['SYMBOL1'])
SYMBOLS.append(environ['SYMBOL2'])
SYMBOLS.append(environ['SYMBOL3'])
SYMBOLS.append(environ['SYMBOL4'])

EFS_PATH = environ['EFS_PATH']
DELAY = environ['DELAY']

def get_lob_snapshot(lambda_id, session, scheduler_thread, worker_thread, lock, symbol, book_list):
    try:
        start = datetime.utcnow()
        response = session.get(f'https://api.poloniex.com/markets/{symbol}/orderBook?limit=100', timeout=10)
        got_response =  datetime.utcnow()

        if response.status_code == 200:
            #print(f'RESPONSE: {response.text}')
            poloniex_timestamp = int(response.text[12:26])/1000
            poloniex_datetime = datetime.fromtimestamp(poloniex_timestamp)
            book = response.text.replace("\n", "")
            book_with_request_timestamp = book[:-1] + f', "Datetime": {int(datetime.timestamp(start)*1000)}' + '}'
            book_list.append(book_with_request_timestamp)

        else:
            with lock:
                print(f'{lambda_id} | {symbol} #{scheduler_thread}-{worker_thread} | Error | {response.content}')

        with lock:
            # Print thread number separtor for better log readabiliy
            global thread_counter
            if thread_counter < worker_thread:
                thread_counter = worker_thread
                print(f'{lambda_id} -')
    
            print(f'{lambda_id} | {symbol} #{scheduler_thread}-{worker_thread} | req {str(start).split(" ")[1]} | poloniex {str(poloniex_datetime).split(" ")[1]} | resp {str(got_response).split(" ")[1]} | end {str(datetime.utcnow()).split(" ")[1]} | total {str(datetime.utcnow() - start).split(":")[-1]} | delay {str(datetime.utcnow() - poloniex_datetime).split(":")[-1]}')

    except Exception as e:
        print(f'{lambda_id} | {symbol} #{scheduler_thread}-{worker_thread} | Exception | {e}')
    return

def thread_scheduler(lambda_id, session, scheduler_counter, lock, symbols_books):
    with lock:
        print(f'{lambda_id} | Scheduler thread #{scheduler_counter} started ')
    worker_counter = 0
    worker_threads = []
    now = datetime.utcnow()
    till_next_10seconds = 10 - now.second % 10 - now.microsecond / 1000000
    sleep(till_next_10seconds)  # Start worker thread at round minutes
    with lock:
        print(f'{lambda_id} | Scheduler thread #{scheduler_counter} dispatching worker threads')
    global thread_counter
    thread_counter = 0

    time_slippage = 0
    for seconds_to_wait in range(int(DELAY), int(DELAY) + 10, 1): # Every second fo 10 seconds
        for symbol in SYMBOLS:
            #print(f'{lambda_id} | Starting worker thread {symbol} #{scheduler_counter}-{worker_counter}')
            worker_thread = Timer(seconds_to_wait + 0.3 - time_slippage, get_lob_snapshot, [lambda_id, session, scheduler_counter, worker_counter, lock, symbol, symbols_books[symbol]])
            worker_thread.start()
            worker_threads.append(worker_thread)
        worker_counter +=1
        time_slippage += 0.005
    for worker_thread in worker_threads:
        worker_thread.join() # Wait for all threads to finish

def lambda_handler(event, context):

    request_time = gmtime()
    symbols_books = {}
    for symbol in SYMBOLS:
        symbols_books[symbol] = []

    # starting from the next miute to mitigate delayed AWS cron starts
    scheduler_threads = []

    global thread_counter # For log readability
    thread_counter = 0

    lock = Lock()
    lambda_id = context.aws_request_id.split('-')[-1]

    session = Session()
    adapter = adapters.HTTPAdapter(pool_connections=1000, pool_maxsize=1000)
    session.mount('https://', adapter)
    sleep(1) # ensure not the first second

    scheduler_interval = 10
    scheduler_counter = 0
    # Start scheduler threads with 10 seconds interval for 10 minutes (60 in total), each scheduler starts 10 worker threads per symbol (600 snapshots per 10 minutes)
    # Lambda is executed 6 times an hour = 3600 snapshots an hour in total

    now = datetime.utcnow()
    till_next_minute_minus9sec = 60 - 9 - now.second - now.microsecond / 1000000
    sleep(till_next_minute_minus9sec)

    for seconds_to_wait in range(int(DELAY), int(DELAY) + 60 * scheduler_interval, scheduler_interval): # One 10 seconds
        print(f'{lambda_id} | Starting scheduler thread #{scheduler_counter}')
        scheduler_thread = Timer(seconds_to_wait, thread_scheduler, [lambda_id, session, scheduler_counter, lock, symbols_books])
        scheduler_thread.start()
        scheduler_threads.append(scheduler_thread)
        scheduler_counter +=1

    for scheduler_thread in scheduler_threads:
        scheduler_thread.join() # Wait for all threads to finish

    this_hour = strftime('%Y%m%d_%H', request_time)
    part = int(request_time.tm_min / 10) # Lambda fires 60 / 10 = 6 times a hour
    today_folder = this_hour.split('_')[0]

    for symbol in symbols_books:
        makedirs(f'{EFS_PATH}/{today_folder}/{symbol}', exist_ok=True)

        joined = '\n'.join(symbols_books[symbol])
        book_IO = io.StringIO()
        book_IO.write(joined)
        symbol_books_nested = pl.read_ndjson(book_IO)
        symbol_books_nested.write_parquet(f'{EFS_PATH}/{today_folder}/{symbol}/{symbol}-{this_hour}-{part}.parquet', compression='snappy')

# class Object(object):
#     pass
# context = Object()
# context.aws_request_id = '84bbc9aa-2669-45ce-bc0f-2b82ef890874'
# lambda_handler({}, context)