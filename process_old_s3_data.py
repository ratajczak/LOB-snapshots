from datetime import datetime, timedelta
import gzip
import json
import orjson
import os
#import shutil
import boto3
#from os import listdir
#from os.path import isfile, join
from concurrent import futures
from threading import Lock
import polars as pl
from io import StringIO

bucket = 'limit-order-books-polonie-limitorderbooksnapshots-1ggf6vguvne3r'
parquet_data_folder = '/home/pawel/LOB'
symbols = ['UT_USDT', 'TRR_USDT', 'BTT_USDT', 'FCT2_USDT', 'TRX_USDT', 'BTC_USDT', 'VOLT_USDT', 'BNB_USDT', 'ETH_BTC', 'LTC_BTC', 'AAVE_BTC', 'TRX_BTC', 'XRP_BTC', 'LINK_BTC', 'SNX_BTC', 'DOT_BTC', 'SOL_BTC', 'DOGE_BTC', 'BCH_BTC', 'MATIC_BTC', 'AVA_BTC', 'WBTC_BTC', 'ZRX_BTC', 'FIL_BTC', 'TUSD_USDT', 'TUSD_USDC', 'USDD_USDT', 'ETH_TRX', 'ETHW_ETH', 'AFC_USDD']

oldest_date = datetime(2020,11,10)
newest_date = datetime(2022,12,20)

s3_client = boto3.client('s3')

def get_lob_data(symbol):

    #old symbos are other way round
    pair = symbol.split('_')[1] + '_' + symbol.split('_')[0]

    print(f'Checking LOB data on S3 for {pair}')

    oldest_year = oldest_date.year
    while oldest_year != datetime.now().year:
        resp = s3_client.list_objects(Bucket=bucket, Prefix=f'{pair}/{oldest_year}', Delimiter='/', MaxKeys=1)
        if 'CommonPrefixes' in resp:
            break
        oldest_year+=1

    if 'CommonPrefixes' not in resp:
        print(f'No data for {pair}')
        return

    oldest_month = 1
    while oldest_month <= 12:
        resp = s3_client.list_objects(Bucket=bucket, Prefix=f'{pair}/{oldest_year}/{oldest_month:02}', Delimiter='/', MaxKeys=1)
        if 'CommonPrefixes' in resp:
            break
        oldest_month+=1

    oldest_day = 1
    while oldest_day <= 31:
        resp = s3_client.list_objects(Bucket=bucket, Prefix=f'{pair}/{oldest_year}/{oldest_month:02}/{oldest_day:02}', Delimiter='/', MaxKeys=1)
        if 'CommonPrefixes' in resp:
            break
        oldest_day+=1



    date_to_process = datetime(oldest_year, oldest_month, oldest_day)

    while date_to_process <= newest_date:
        parquet_file_name = f'{parquet_data_folder}/aggregated/{pair}/{date_to_process.year}{date_to_process.month:02}{date_to_process.day:02}.parquet'

        from pathlib import Path

        if not Path(parquet_file_name).is_file():
    
            json_data = download_s3_folder(f'{pair}/{date_to_process.year}/{date_to_process.month:02}/{date_to_process.day:02}')
            books = []
            raw_data = {}
            for json_string in json_data:
                raw_data_temp = load_lob_json(json_string, pair)
                raw_data.update(raw_data_temp)

            for book_key in raw_data:
                book_json = raw_data[book_key]
                try:
                    book_date_time = datetime.strptime(book_key[9:25], "%Y%m%d_%H%M%S")
                except Exception:
                    continue
                if book_json["asks"] == [] or book_json["bids"] == []:
                    continue
                asks_json_string = f'{book_json["asks"]}'.replace("'", "").replace('[','').replace(']','')
                bids_json_string = f'{book_json["bids"]}'.replace("'", "").replace('[','').replace(']','')

                book_json_string = '{"asks": [' + asks_json_string + '], "bids": [' + bids_json_string + '], "Datetime": ' + str(int(book_date_time.timestamp())) + '000, "isFrozen": ' + str(raw_data[book_key]['isFrozen']) + ', "seq": ' + str(raw_data[book_key]['seq']) + '}'
                output = StringIO()
                output.write(book_json_string)

                books.append(pl.read_ndjson(output))

            del raw_data

            if books:
                data = pl.concat(books)

                data = data.sort(
                    'Datetime'
                ).with_row_count(
                    
                ).with_columns([
                    pl.col('Datetime').cast(pl.Datetime, strict=False).dt.with_time_unit("ms").alias('Datetime')
                ]).with_columns([
                    (pl.Series([[''] * 200])).alias("empty_200_strings")
                ]).with_columns([
                    (pl.when(pl.col("asks").arr.lengths() == 200).then(
                        pl.col("asks")
                        ).otherwise(
                            pl.col("asks").arr.concat(
                                pl.col("empty_200_strings").arr.slice(0, 200 - pl.col("asks").arr.lengths())
                            )
                        )
                    ).alias("asks")
                ]).with_columns([
                    (pl.when(pl.col("bids").arr.lengths() == 200).then(
                        pl.col("bids")
                        ).otherwise(
                            pl.col("bids").arr.concat(
                                pl.col("empty_200_strings").arr.slice(0, 200 - pl.col("bids").arr.lengths())
                            )
                        )
                    ).alias("bids")
                ]).drop("empty_200_strings").explode(
                    ["asks", "bids"]
                ).with_columns([
                    pl.lit(1).alias("ones")
                ]).with_columns([
                    (pl.col("ones").cumsum().over("row_nr") / 2 - 0.5).cast(pl.Int64).alias("temp_level")
                ]).groupby(["row_nr", "temp_level"], maintain_order=True).agg([
                    pl.col("Datetime").first(),
                    pl.col("isFrozen").first(),
                    pl.col("seq").first(),
                    pl.col("asks").first().cast(pl.Float64, strict=False).alias("Ask_Price"),
                    pl.col("asks").last().cast(pl.Float64, strict=False).alias("Ask_Size"),
                    pl.col("bids").first().cast(pl.Float64, strict=False).alias("Bid_Price"),
                    pl.col("bids").last().cast(pl.Float64, strict=False).alias("Bid_Size"),
                    pl.col("temp_level").first().alias("Level")
                ]).drop(
                    ["row_nr", "temp_level"]
                )

                os.makedirs(f'{parquet_data_folder}/aggregated/{pair}', exist_ok=True)
                data.write_parquet(parquet_file_name)

                stats = {}
                stats['gaps'] = int((60*60*24*100 - len(data)) / 100)
                os.makedirs(f'{parquet_data_folder}/stats/{date_to_process.year}{date_to_process.month:02}{date_to_process.day:02}/{pair}', exist_ok=True)
                with open(f'{parquet_data_folder}/stats/{date_to_process.year}{date_to_process.month:02}{date_to_process.day:02}/{pair}/stats.json', 'w') as file:
                    file.write(json.dumps(stats))
                print(f'Aggregated {date_to_process.year}{date_to_process.month:02}{date_to_process.day:02}')
            else:
                print(f'Data for day {date_to_process.year}/{date_to_process.month:02}/{date_to_process.day:02} missing')
        else:
            print(f'Found parquet file for {date_to_process.year}{date_to_process.month:02}{date_to_process.day:02}, skipping this day')

        date_to_process += timedelta(days=1)

def download_S3_object(key, store, lock):
    try:
        resp = s3_client.get_object(Bucket=bucket, Key=key)
        unzipped = gzip.decompress(resp['Body'].read())
        contents = unzipped.decode('utf-8')
        with lock:
            store.append(contents)
    except Exception as e:
        print(e)

def download_s3_folder(day_folder):
    store = []
    lock = Lock()
    result = s3_client.list_objects_v2(Bucket=bucket, Prefix=day_folder)
    keys = []
    for obj in result.get('Contents'):
        keys.append(obj.get('Key'))

    with futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_key = {executor.submit(download_S3_object, key, store, lock): key for key in keys}
        for future in futures.as_completed(future_to_key):
            future_to_key[future]
    return store

def load_lob_json(json_string, pair):
    '''
    Function decode json and fix malformed data issues.
    Calls itself recursvely on exceptions until all issues are fixed. 
    Arguments:
    json_string -- string, json to decode and fix
    Returns: dictionary from decoded json string
    '''
    try:
        json_dict = orjson.loads(json_string)

    except json.JSONDecodeError as e:
        print(f'Malformed JSON {e.msg} in file at position {e.pos} - {json_string[e.pos-20 :e.pos+20]}')

        this_snapshot = json_string.rindex(pair, 0, e.pos)
        try:
            next_snapshot = json_string.index(pair, e.pos)
        except Exception as e:
            #Pair substring not found after this malformation, this is the last snapshot
            fixed_json_string = json_string[:this_snapshot-2] + '}'
            return load_lob_json(fixed_json_string, pair)

        fixed_json_string = json_string[:this_snapshot] + json_string[next_snapshot:]
        return load_lob_json(fixed_json_string, pair)
    return json_dict

for symbol in symbols:
    get_lob_data(symbol)