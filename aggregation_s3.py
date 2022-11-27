import os
import time
import os
from pathlib import Path
import numpy as np
import pandas as pd
import json, orjson

def read_json_files(files_list, pair):
    raw_data = {}
    for file in files_list:
        try:
            with open(file, 'r') as f:
                json_string = '{' + f.read() + '}'
                if json_string:
                    raw_data_temp = load_lob_json(json_string, pair)
                    raw_data.update(raw_data_temp)
        except Exception as e:
            print(f'{e} on {file}')

    return raw_data

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

def clean_lob_data(raw_data):

    pair_raw_df = pd.DataFrame.from_dict(raw_data, orient='index') # drop into df
    # pair_raw_df = pd.DataFrame(raw_data) 
    pair_raw_df.index = pd.to_datetime(pair_raw_df.index.str[-15:], format='%Y%m%d_%H%M%S', errors='coerce')
    pair_raw_df.index.name = 'Datetime'

    # explode bids
    exploded_bids_raw = pair_raw_df['bids'].explode()

    # drop the nulls
    exploded_bids_valid = exploded_bids_raw.loc[~exploded_bids_raw.isnull()]
    exploded_bids = pd.DataFrame(
        exploded_bids_valid.tolist(), 
        index= exploded_bids_valid.index, 
        columns=['Bid_Price', 'Bid_Size']
    )

    # if there are nulls, treat them separately and append to valid df
    if exploded_bids_valid.shape[0] < exploded_bids_raw.shape[0]:
        exploded_bids_nans = exploded_bids_raw.loc[exploded_bids_raw.isnull()]
        bids_nans_df = pd.DataFrame(
            [[np.nan, np.nan]] * exploded_bids_nans.shape[0],
            exploded_bids_nans.index,
            columns=['Bid_Price', 'Bid_Size']
        )
        exploded_bids = pd.concat([exploded_bids, bids_nans_df])

    # create column with ob levels
    exploded_bids['Level'] = exploded_bids.groupby(level='Datetime')['Bid_Price'].cumcount() + 1




    # explode asks
    exploded_asks_raw = pair_raw_df['asks'].explode()

    # drop the nulls
    exploded_asks_valid = exploded_asks_raw.loc[~exploded_asks_raw.isnull()]
    exploded_asks = pd.DataFrame(
        exploded_asks_valid.tolist(), 
        index= exploded_asks_valid.index, 
        columns=['Ask_Price', 'Ask_Size']
    )

    # if there are nulls, treat them separately and append to valid df
    if exploded_asks_valid.shape[0] < exploded_asks_raw.shape[0]:
        exploded_asks_nans = exploded_asks_raw.loc[exploded_asks_raw.isnull()]
        asks_nans_df = pd.DataFrame(
            [[np.nan, np.nan]] * exploded_asks_nans.shape[0],
            exploded_asks_nans.index,
            columns=['Ask_Price', 'Ask_Size']
        )
        exploded_asks = pd.concat([exploded_asks, asks_nans_df])


    # create column with ob levels
    exploded_asks['Level'] = exploded_asks.groupby(level='Datetime')['Ask_Price'].cumcount() + 1

    # merge the exploded bids and asks
    df_depth = exploded_bids.reset_index().set_index(['Datetime', 'Level']).join(
        exploded_asks.reset_index().set_index(['Datetime', 'Level']),
        on=['Datetime', 'Level'],
        how='outer'
    )

    # merge exploded dfs with original df
    df_depth = df_depth.join(
        pair_raw_df[['isFrozen', 'postOnly', 'seq']],
        on='Datetime',
        how='left'
    ).sort_values(by=['Datetime', 'Level'])

    # cast datatypes
    df_depth['Ask_Price'] = df_depth['Ask_Price'].astype('float64')
    df_depth['Bid_Price'] = df_depth['Bid_Price'].astype('float64')
    df_depth['Ask_Size'] = df_depth['Ask_Size'].astype('float64')
    df_depth['Bid_Size'] = df_depth['Bid_Size'].astype('float64')

    return df_depth

def group_depth(df_depth, freq):
    
    # create additional features useful for resampling
    df_depth['Mid_Price'] = (df_depth['Ask_Price'] + df_depth['Bid_Price']) / 2

    # top level mid at each timestamp
    df_depth['Prevailing_Mid'] = df_depth.groupby('Datetime')['Mid_Price'].transform('first')

    # cumulative depth size
    df_depth['Bid_Cum_Size'] = df_depth.groupby('Datetime')['Bid_Size'].transform(np.cumsum)
    df_depth['Ask_Cum_Size'] = df_depth.groupby('Datetime')['Ask_Size'].transform(np.cumsum)

    # spread against prevailing mid
    df_depth['Bid_Spread'] = (df_depth['Bid_Price'] - df_depth['Prevailing_Mid']) / df_depth['Prevailing_Mid'] * -1
    df_depth['Ask_Spread'] = (df_depth['Ask_Price'] - df_depth['Prevailing_Mid']) / df_depth['Prevailing_Mid']


    # resample dataframe to the wanted frequency
    day_data_grp = df_depth.groupby([pd.Grouper(level='Datetime', freq=freq), 'Level']).agg(
        Bid_Spread=('Bid_Spread', np.mean), 
        Ask_Spread=('Ask_Spread', np.mean), 
        Bid_Cum_Size=('Bid_Cum_Size', np.mean), 
        Ask_Cum_Size=('Ask_Cum_Size', np.mean), 
        Bid_Price=('Bid_Price', np.mean),
        Ask_Price=('Ask_Price', np.mean),
        Bid_Size=('Bid_Size', np.mean),
        Ask_Size=('Ask_Size' ,np.mean),
        Mid_Price=('Mid_Price', np.mean),
        close=('Mid_Price', 'last'),
        open=('Mid_Price', 'first'),
        high=('Mid_Price', max),
        low=('Mid_Price', min),
    ).reset_index()

    # print(day_data_grp.head())
    
    # extract features that use depth in a more compact way
    def get_depth(df, side, target_sprd):

        tgt_sprd_bps = int(target_sprd*10000)
        df_mask = df[df[f'{side}_Spread']<=target_sprd].copy()
        df_grouped = df_mask.groupby(pd.Grouper(key='Datetime', freq=freq)).agg({'Level':np.max, f'{side}_Size':np.sum})
        df_grouped.rename(columns={'Level':f'{side}_Level_{tgt_sprd_bps}bps', f'{side}_Size': f'{side}_Size_{tgt_sprd_bps}bps'}, inplace=True)
        # if spread is too wide, 
        #df_grouped.loc[:,f'{side}_Level_{tgt_sprd_bps}bps'] = df_grouped.loc[:,f'{side}_Level_{tgt_sprd_bps}bps'].fillna(101)

        # max_lvl = df_grouped[f'{side}_Level_{tgt_sprd_bps}bps'].max()
        # print(f"max level on {side} side: {max_lvl}")
        # if max_lvl == 99:
        #     print(f"timestamps where level has been maxed out: {df_grouped[df_grouped[f'{side}_Level_{tgt_sprd_bps}bps']==99].index}")

        return df_grouped

    df_depth_list = []
    for target_sprd in [0.0005, 0.0010, 0.0020, 0.0030]:
        df_depth_list.append(get_depth(day_data_grp, 'Ask', target_sprd))
        df_depth_list.append(get_depth(day_data_grp, 'Bid', target_sprd))
    df_depth = pd.concat(df_depth_list, axis=1)

    # print(df_depth.head())

    # merge first level order book and depth features
    lv_0_fields = ['Datetime', 'Ask_Price', 'Bid_Price', 'Mid_Price', 'close', 'open', 'high', 'low']
    df_px_lv0 = day_data_grp[day_data_grp['Level']==1][lv_0_fields].set_index('Datetime')
    df_px_final = pd.merge(df_px_lv0, df_depth, left_index=True, right_index=True, how='left').reset_index()

    # imputation, fill NAs left from depth aggregation
    level_cols = [col for col in df_depth.columns if 'Level' in col]
    size_cols = [col for col in df_depth.columns if 'Size' in col]

    df_px_final.loc[:,level_cols] = df_px_final.loc[:,level_cols].fillna(-1).astype('int64') # assign negative level value if no quote meet spread criteria
    df_px_final.loc[:, size_cols] = df_px_final.loc[:, size_cols].fillna(0).astype('float64') # and assign zero size to those

    return df_px_final

def process_day(date, pair):
    files_in_path = os.listdir(f'{efs_path}/{date}/{pair}')
    if 'stats.json' not in files_in_path:
        full_paths = [f'{efs_path}/{date}/{pair}/{file}' for file in files_in_path] # full paths
        raw_data = read_json_files(full_paths, pair)
        stats = {}
        if raw_data:
            df_depth = clean_lob_data(raw_data)
            df_depth.to_parquet(f's3://{destination_bucket}/{pair}/1s/{date}.parquet', engine='pyarrow', compression='snappy')

            df_grouped = group_depth(df_depth, "60s") # one minute aggregation 
            df_grouped.to_parquet(f's3://{destination_bucket}/{pair}/1m/{date}.parquet', engine='pyarrow', compression='snappy')

            stats['gaps'] = 60*60*24 - len(raw_data)
            with open(f'{efs_path}/{date}/{pair}/stats.json', 'w') as file:
                file.write(json.dumps(stats))
    for file_name in files_in_path:
        if file_name != 'stats.json':
            os.remove(f'{efs_path}/{day}/{pair}/{file_name}')


efs_path = f'/home/ec2-user/efs/{os.environ["EFS_FOLDER"]}'
destination_bucket = os.environ['BUCKET']

now = time.gmtime()
date_now = time.strftime('%Y%m%d', now)
day_folders = os.listdir(efs_path)
for day in day_folders:
    if day != date_now:
        pairs = os.listdir(f'{efs_path}/{day}')
        for pair in pairs:
            print(f'Processing {pair} for {day}')
            process_day(day, pair)
            Path(f'{efs_path}/last_pair_heartbeat').touch()