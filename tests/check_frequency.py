from os import environ, listdir
from time import gmtime, strftime
import json
import gzip

frequency = 4
folder = '/home/pawel/Documents/LOB-data/backup-test'

#                          file_name = '20201118_16-2.json.gz'

data = {}
files = listdir(folder)
counter = 0
for file_name in files:
    print(file_name)
    part = file_name.split('.')[0][-1:]
    with gzip.GzipFile(f'{folder}/{file_name}', 'r') as fin:  
        json_bytes = fin.read()                      # bytes

    json_str = json_bytes.decode('utf-8')            # decode bytes
    
    json_obj = json.loads(json_str)
    for key in json_obj:
        data[f'{key}-{part}'] = '' #json_obj[key]
    counter += 1

prev_seconds = 60 - frequency
prev_key = ''
diffs = {0:0, 1:0, 2:0, 3:0, 4:0, 5:0, 6:0, 7:0, 8:0, 9:0}
print(f'number of keys: {len(data.keys())}')
for key in sorted(data.keys()):
    #print(key)
    parts = key.split('-')
    time = parts[0][-6:] ###### 0 for backup 1 for etl
    seconds = int(time[-2:])
    if seconds == 0:
        seconds = 60
    diff = (frequency + seconds - (prev_seconds + frequency)) % 60
    diffs[diff] = diffs[diff] + 1

    prev_seconds = seconds
    prev_key = key
print(diffs)