import yaml
from datetime import date, datetime, timedelta
import time
from os import makedirs
from pathlib import Path
import requests
import json
import polars as pl
import pyarrow.parquet as pq

# Retrieve all sybmols from SAM template
symbols = []

def any_constructor(loader, tag_suffix, node):
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    return loader.construct_scalar(node)

yaml.add_multi_constructor('', any_constructor, Loader=yaml.SafeLoader)

with open('etl/template.yaml', 'r') as template_file:
    template = yaml.safe_load(template_file)

for resource in template['Resources']:
    if 'Properties' in template['Resources'][resource]:
        if 'Environment' in template['Resources'][resource]['Properties']:
            if 'Variables' in template['Resources'][resource]['Properties']['Environment']:
                for variable in template['Resources'][resource]['Properties']['Environment']['Variables']:
                    if variable[0:6] == 'SYMBOL':
                        symbols.append(template['Resources'][resource]['Properties']['Environment']['Variables'][variable])

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

yesterday = date.today() - timedelta(days=1)

columns = ['low', 'high', 'open', 'close', 'amount', 'quantity', 'buyTakerAmount', 'buyTakerQuantity', 'tradeCount', 'ts', 'weightedAverage', 'interval', 'startTime', 'closeTime']

for symbol in symbols:
    makedirs(f'candles/{symbol}', exist_ok=True)
    # Getting start date from symbol information endpoint
    response = requests.get(f'https://api.poloniex.com/markets/{symbol}')
    info = json.loads(response.content)
    tradable_start_time = info[0]['tradableStartTime']
    start_date = datetime.utcfromtimestamp(int(tradable_start_time / 1000)).date() + timedelta(days=1)

    print(f'Getting {symbol} since {start_date}')
    for single_date in daterange(start_date, yesterday):
        parquet_file_name = f'candles/{symbol}/{single_date.strftime("%Y%m%d")}.parquet'
        if not Path(parquet_file_name).is_file():
            single_date_time = datetime(year=single_date.year, month=single_date.month, day=single_date.day)
            start_time = int(time.mktime(single_date_time.timetuple()))
            end_time = int(time.mktime((single_date_time + timedelta(minutes=480)).timetuple()))
            response = requests.get(f'https://api.poloniex.com/markets/{symbol}/candles?interval=MINUTE_1&limit=480&startTime={start_time}000&endTime={end_time}000', timeout=10)
            candles = json.loads(response.content)
            df1 = pl.DataFrame(candles, columns).drop(['interval', 'ts'])

            start_time = int(time.mktime((single_date_time + timedelta(minutes=480)).timetuple()))
            end_time = int(time.mktime((single_date_time + timedelta(minutes=480*2)).timetuple()))
            response = requests.get(f'https://api.poloniex.com/markets/{symbol}/candles?interval=MINUTE_1&limit=480&startTime={start_time}000&endTime={end_time}000', timeout=10)
            candles = json.loads(response.content)
            df2 = pl.DataFrame(candles, columns).drop(['interval', 'ts'])

            start_time = int(time.mktime((single_date_time + timedelta(minutes=480*2)).timetuple()))
            end_time = int(time.mktime((single_date_time + timedelta(minutes=480*3)).timetuple()))
            response = requests.get(f'https://api.poloniex.com/markets/{symbol}/candles?interval=MINUTE_1&limit=480&startTime={start_time}000&endTime={end_time}000', timeout=10)
            candles = json.loads(response.content)
            df3 = pl.DataFrame(candles, columns).drop(['interval', 'ts'])

            day_candles = pl.concat([df1, df2, df3])

            day_candles = day_candles.with_columns([
                pl.col('low').first().cast(pl.Float64, strict=False).alias('low'),
                pl.col('high').first().cast(pl.Float64, strict=False).alias('high'),
                pl.col('open').first().cast(pl.Float64, strict=False).alias('open'),
                pl.col('close').first().cast(pl.Float64, strict=False).alias('close'),
                pl.col('amount').first().cast(pl.Float64, strict=False).alias('amount'),
                pl.col('quantity').first().cast(pl.Float64, strict=False).alias('quantity'),
                pl.col('buyTakerAmount').first().cast(pl.Float64, strict=False).alias('buyTakerAmount'),
                pl.col('buyTakerQuantity').first().cast(pl.Float64, strict=False).alias('buyTakerQuantity'),
                pl.col('tradeCount').first().cast(pl.Int64, strict=False).alias('tradeCount'),
                pl.col('weightedAverage').first().cast(pl.Float64, strict=False).alias('weightedAverage'),
                pl.col('startTime').cast(pl.Datetime, strict=False).dt.with_time_unit("ms").alias('startTime'),
                pl.col('closeTime').cast(pl.Datetime, strict=False).dt.with_time_unit("ms").alias('closeTime')
            ])
            pyarrow_table = day_candles.to_arrow()
            pq.write_table(pyarrow_table, parquet_file_name, compression='ZSTD')
            print(f'Saved: {parquet_file_name}')
        else:
            print(f'Found: {parquet_file_name}')