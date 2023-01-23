from os import makedirs, environ, listdir, remove
from time import gmtime, sleep, strftime
from datetime import datetime
from requests import adapters, Session

from threading import Timer, Lock
import gzip
from shutil import rmtree
import polars as pl
import io

# import ptvsd
# ptvsd.enable_attach(address=('0.0.0.0', 5890), redirect_output=True)
# ptvsd.wait_for_attach()

environ['SYMBOL0'] = 'ETH_BTC'
environ['SYMBOL1'] = 'ETC_BTC'
environ['SYMBOL2'] = 'LTC_BTC'
environ['SYMBOL3'] = 'NXT_BTC'
environ['SYMBOL4'] = 'STR_BTC'
environ['SYMBOL5'] = 'XEM_BTC'
environ['SYMBOL6'] = 'XRP_BTC'
environ['EFS_PATH'] = '/tmp'

SYMBOLS_NO = 7
SYMBOLS = []
for i in range(SYMBOLS_NO):
    SYMBOLS.append(environ[f'SYMBOL{i}'])

EFS_PATH = environ['EFS_PATH']

def get_lob_snapshot(lambda_id, session, thread_number, lock, symbol, book_list):
    try:
        request_time = gmtime()
        start = datetime.utcnow()

        response = session.get(f'https://api.poloniex.com/markets/{symbol}/orderBook?limit=10', timeout=10)

        got_response =  datetime.utcnow()

        poloniex_timestamp = int(response.text[12:26])/1000
        poloniex_datetime = datetime.fromtimestamp(poloniex_timestamp)

        if response.status_code == 200:
            #print('RESPONSE: ')
            #print(response.text)

            this_hour = strftime('%Y%m%d_%H', request_time)
            this_minute_second = strftime('%M%S', request_time)
            snapshot_id = f'{symbol}-{this_hour}{this_minute_second}'

            book = response.text.replace("\n", "")
            #start1 = datetime.utcnow()
            book_IO = io.StringIO()
            book_IO.write(book)
            nested = pl.read_ndjson(book_IO)

            asks = pl.DataFrame(nested["asks"].explode())
            bids = pl.DataFrame(nested["bids"].explode())

            asks_bids_sizes_prices = pl.concat([asks,bids], how="horizontal").with_row_count().with_column(
                    (pl.col("row_nr")/2).cast(pl.Int16).alias("level")
                ).drop("row_nr").groupby("level").agg(
                    [
                        pl.first('asks').cast(pl.Float32).alias('ask_price'),
                        pl.last('asks').cast(pl.Float32).alias('ask_size'),
                        pl.first('bids').cast(pl.Float32).alias('bid_price'),
                        pl.last('bids').cast(pl.Float32).alias('bid_size'),
                    ]
            ).sort("level")

            book = asks_bids_sizes_prices.join(
                nested.drop(["asks", "bids"]).with_column(
                    pl.lit(f'{symbol}-{this_hour}{this_minute_second}').alias("key")
                ).with_column(
                    pl.lit(0).cast(pl.Int16).alias("level")
                ), on="level", how="outer").fill_null(strategy="forward")

            #book = nested.drop(["asks", "bids"]).with_column(pl.lit(0).cast(pl.Int16).alias("level")).join(asks_bids_sizes_prices, on="level", how="outer").fill_null(strategy="forward")

            #timing = datetime.utcnow() - start1
            book_list.append(book)

        else:
            with lock:
                print(f'{lambda_id} | {symbol} #{thread_number} | Error | {response.content}')

        with lock:
            # print thread number separtor for better log readabiliy 
            global thread_counter
            if thread_counter < thread_number:
                thread_counter = thread_number
                print(f'{lambda_id} -')
    
            print(f'{lambda_id} | {symbol} #{thread_number} | request {str(start).split(" ")[1]} || poloniex {str(poloniex_datetime).split(" ")[1]} || response {str(got_response).split(" ")[1]} | end {str(datetime.utcnow()).split(" ")[1]} | total {str(datetime.utcnow() - start).split(":")[-1]} | delay {str(datetime.utcnow() - poloniex_datetime).split(":")[-1]}')
    except Exception as e:
        print(f'{lambda_id} | {symbol} #{thread_number} | Exception | {e}')
    return

def lambda_handler(event, context):

    request_time = gmtime()
    symbols_books = {}
    for i in range(SYMBOLS_NO):
        symbols_books[SYMBOLS[i]] = []

    # starting from next miute (to mitigate delayed AWS cron starts)

    threads = []
    thread_number = 0
    global thread_counter
    thread_counter = 0
    lock = Lock()
    lambda_id = context.aws_request_id.split('-')[-1]
    time_slippage = -0.2

    session = Session()
    adapter = adapters.HTTPAdapter(pool_connections=6000, pool_maxsize=6000)
    session.mount('https://', adapter)

    sleep(1) # ensure not first second
    now = datetime.utcnow()
    till_next_minute = 60 - now.second - now.microsecond / 1000000
    #sleep(till_next_minute)
    print(f'{lambda_id} | {datetime.utcnow()} starting threads')

    # Start threads with 1 second interval for 10 minutes (600 in total)
    # executed 6 times an hour = 3600 snapshots an hour in total
    for seconds_to_wait in range(0, 10, 1): #range(0, 10 * 60, 1):
        for symbol in SYMBOLS:
            thread = Timer(seconds_to_wait - time_slippage, get_lob_snapshot, [lambda_id, session, thread_number, lock, symbol, symbols_books[symbol]])
            thread.start()
            threads.append(thread)
        time_slippage += 0.003
        thread_number += 1
    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    this_hour = strftime('%Y%m%d_%H', request_time)
    part = int(request_time.tm_min / 10) # Lambda fires 60 / 10 = 6 times a hour

    # for pair_folder in pairs:
    #     print(f'Concatenating, compressing and saving {pair_folder} on EFS')
    #     today_folder = pair_folder.split('-')[1][0:8]
    #     pair = pair_folder.split('-')[0]
    #     makedirs(f'{esf_path}/{today_folder}/{pair}', exist_ok=True)

    #     files = listdir(f'{TEMP_FOLDER}/{pair_folder}')
    #     books = []
    #     for file_name in files:
    #         with open(f'{TEMP_FOLDER}/{pair_folder}/{file_name}', 'r') as f:
    #             books.append(f.read())
    #         remove(f'{TEMP_FOLDER}/{pair_folder}/{file_name}')
    #     all_books_json_string = ','.join(books)
    #     #print(f'CONTENT of {pair_folder}.gz')

    #     with gzip.open(f'{esf_path}/{today_folder}/{pair}/{pair_folder}.gz', 'wb') as f:
    #         f.write(all_books_json_string.encode())

class Object(object):
    pass
context = Object()
context.aws_request_id = '84bbc9aa-2669-45ce-bc0f-2b82ef890874'
lambda_handler({}, context)