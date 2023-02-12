import os
import boto3

efs_path = os.environ["EFS_PATH"]
bucket = os.environ["BUCKET"]

def lambda_handler(event, context):
    client = boto3.client("s3")
    pair_folders = os.listdir(f'{efs_path}/aggregated')
    for pair in pair_folders:
        files = os.listdir(f'{efs_path}/aggregated/{pair}')
        for file in files:
            print(f'Moving {pair}/{file} to S3')
            client.upload_file(f'{efs_path}/aggregated/{pair}/{file}', bucket, f'{pair}/{file}')
            os.remove(f'{efs_path}/aggregated/{pair}/{file}')
            
    stats_folder = os.listdir(f'{efs_path}/stats')
    for stats_file in stats_folder:
        print(f'Moving {stats_file} to S3')
        client.upload_file(f'{efs_path}/stats/{stats_file}', bucket, f'stats/{stats_file}')
        os.remove(f'{efs_path}/stats/{stats_file}')
