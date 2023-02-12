import os
import time
from pathlib import Path
import json
from shutil import rmtree
import polars as pl

#pl.Config.set_tbl_cols(20)
efs_path = os.environ["EFS_PATH"]

def aggregate_day(date, pair):
    files_in_path = os.listdir(f'{efs_path}/{date}/{pair}')
    if 'stats.json' not in files_in_path:
        files_list = [f'{efs_path}/{date}/{pair}/{file}' for file in files_in_path] # full paths
        data = pl.concat([pl.scan_parquet(file_path) for file_path in files_list])
        stats = {}

        if 'time' in data.columns:
            data = data.with_columns([pl.col('time').cast(pl.Datetime, strict=False).dt.with_time_unit("ms").alias('time')])
        if 'ts' in data.columns:
            data = data.with_columns([pl.col('ts').cast(pl.Datetime, strict=False).dt.with_time_unit("ms").alias('ts')])
        if 'scale' in data.columns:
            data.drop("scale") #TODO keep it? data.with_column(pl.col('scale').cast(pl.Float64, strict=False).alias('scale'))

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
            pl.col("time").first(),
            pl.col("ts").first(),
            pl.col("asks").first().cast(pl.Float64, strict=False).alias("Ask_Price"),
            pl.col("asks").last().cast(pl.Float64, strict=False).alias("Ask_Size"),
            pl.col("bids").first().cast(pl.Float64, strict=False).alias("Bid_Price"),
            pl.col("bids").last().cast(pl.Float64, strict=False).alias("Bid_Size"),
            pl.col("temp_level").first().alias("Level")
        ]).drop(
            ["row_nr", "temp_level"]
        ).collect()

        os.makedirs(f'{efs_path}/aggregated/{pair}', exist_ok=True)
        data.write_parquet(f'{efs_path}/aggregated/{pair}/{date}.parquet')

        stats['gaps'] = int((60*60*24*100 - len(data)) / 100)
        with open(f'{efs_path}/{date}/{pair}/stats.json', 'w') as file:
            file.write(json.dumps(stats))

    for file_name in files_in_path:
        if file_name != 'stats.json':
            os.remove(f'{efs_path}/{date}/{pair}/{file_name}')

def write_stats(date, pairs):
    all_stats = []
    for pair in pairs:
        with open(f'{efs_path}/{date}/{pair}/stats.json', 'r') as f:
            stats = json.load(f)
            all_stats.append({'pair': pair, 'gaps': stats['gaps']})
    all_stats_df = pl.DataFrame(all_stats)
    os.makedirs(f'{efs_path}/stats', exist_ok=True)
    all_stats_df.write_csv(f'{efs_path}/stats/{date}.csv')

def lambda_handler(event, context):
    now = time.gmtime()
    date_now = time.strftime('%Y%m%d', now)
    day_folders = os.listdir(efs_path)
    for day in day_folders:
        if day != date_now and day != 'last_pair_heartbeat' and day != 'aggregated':
            pairs = os.listdir(f'{efs_path}/{day}')
            for pair in pairs:
                print(f'Processing {pair} for {day}')
                aggregate_day(day, pair)
            
                Path(f'{efs_path}/last_pair_heartbeat').touch()
            write_stats(day, pairs)
            rmtree(f'{efs_path}/{day}')
            break #one day at a time due to lambda timeout
