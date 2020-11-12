from os import environ, listdir
from time import gmtime, strftime
import json
import gzip

folder = '/home/pawel/Documents/LOB-data/new-format-test/6'
data = {}
files = listdir(folder)
counter = 0
for file_name in files:
    #print(file_name)
    part = file_name.split('.')[0][-1:]
    with gzip.GzipFile(f'{folder}/{file_name}', 'r') as fin:  
        json_bytes = fin.read()                      # bytes

    json_str = json_bytes.decode('utf-8')            # decode bytes
    
    json_obj = json.loads(json_str)
    for key in json_obj:
        data[f'{key}-{part}'] = json_obj[key]
    counter += 1

prev_seconds = 59
prev_key = ''
morethanone = {0:0, 1:len(data.keys()), 2:0, 3:0, 4:0, 5:0, 6:0}
print(f'number of keys: {len(data.keys())}')
for key in sorted(data.keys()):
    #print(key)
    parts = key.split('-')
    time = parts[1][-6:]
    seconds = int(time[-2:])
    if (prev_seconds + 1) % 60 != seconds:
        if seconds == 0:
            seconds = 60
        diff = (1 + seconds - (prev_seconds + 1)) % 60
        morethanone[diff] = morethanone[diff] + 1
    prev_seconds = seconds
    prev_key = key
print(morethanone)