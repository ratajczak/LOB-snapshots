import os
import time
import json
import gzip
import shutil

TEMP_FOLDER = '/mnt/efs/temp'
READY_FOLDER = '/mnt/efs/ready'
os.makedirs(READY_FOLDER, exist_ok=True) 

def lambda_handler(event, context):
    request_time = time.gmtime()
    this_hour = time.strftime('%Y%m%d_%H', request_time)
    pairs = os.listdir(TEMP_FOLDER)
    for pair in pairs:
        pair_folder = f'{TEMP_FOLDER}/{pair}'
        hours = [folder_name for folder_name in os.listdir(pair_folder) if folder_name != this_hour]
        for hour in sorted(hours):
            hour_folder = f'{pair_folder}/{hour}'
            print(f'Aggregating {hour_folder}')
            book_snapshot_files = [file_name for file_name in os.listdir(hour_folder)]
            gap_tracker = 0
            books_json_string = '{'
            previous_snapshot = ''

            x = 0

            for file_name in sorted(book_snapshot_files):
                minute_second = os.path.splitext(file_name)[0]
                file_minute = int(minute_second[0:2])
                file_second = int(minute_second[2:4])
                file_time_seconds = file_minute * 60 + file_second

                # If gaps in 10 second intervals before previous item and this one fill the gap with the previous (or this if first) snapshot
                while file_time_seconds - gap_tracker > 5: # check for gaps with 5 seconds accuracy 
                    if previous_snapshot:
                        previous_snapshot = previous_snapshot.replace('"isFrozen": "0"', '"isFrozen": "1"')
                    else:
                        with open(f'{hour_folder}/{file_name}') as snapshot_file:
                            previous_snapshot = snapshot_file.read().replace('"isFrozen": "0"', '"isFrozen": "1"')

                    missing_minute_second = f'{str(int(gap_tracker/60)).rjust(2, "0")}{str(gap_tracker % 60).rjust(2, "0")}'
                    key = f'{pair}-{hour}{missing_minute_second}'
                    books_json_string += f'"{key}": {previous_snapshot},'
                    print(f'Missing snapshot for {pair}-{hour}{missing_minute_second}')


                    if x == 0:
                        print(os.listdir(f'{hour_folder}'))
                        x = 1

                    gap_tracker += 10
                gap_tracker += 10

                with open(f'{hour_folder}/{file_name}') as snapshot_file:
                    book_snapshot = snapshot_file.read()
                    key = f'{pair}-{hour}{minute_second}'
                    books_json_string += f'"{key}": {book_snapshot},'

                previous_snapshot = book_snapshot

            books_json_string = books_json_string[:-1] + '}' # remove trailing comma and add closing bracket 

            archive_file_path = f'{READY_FOLDER}/{pair}-{hour}.json.gz'
            zipped_books = gzip.compress(books_json_string.encode('utf-8'))

            try:
                with open(archive_file_path, 'wb') as archive_file:
                    archive_file.write(zipped_books)
                print(f'Saved {archive_file_path}')
                shutil.rmtree(hour_folder)
                print(f'Deleted {hour_folder}')


            except Exception as e:
                print(f'Failed to save {archive_file_path}')
                print(e)
                
    return True