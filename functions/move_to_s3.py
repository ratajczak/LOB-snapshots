import os
import boto3

READY_FOLDER = '/mnt/efs/ready'
os.makedirs(READY_FOLDER, exist_ok=True) 

bucket = os.environ['DATA_BUCKET']
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    files = os.listdir(READY_FOLDER)
    for archive_file in files:
        pair, name = archive_file.split('-')
        year = name[0:4]
        month = name[4:6]
        day = name[6:8]

        object_name = f'{pair}/{year}/{month}/{day}/{name}'

        try:
            s3_client.upload_file(
                f'{READY_FOLDER}/{archive_file}',
                bucket,
                object_name,
                ExtraArgs={
                    'ContentType': 'application/json',
                    'ContentEncoding': 'gzip'
                    }
            )
            os.remove(f'{READY_FOLDER}/{archive_file}') 
            print(f'Moved {object_name} to S3')
          

        except Exception as e:
            print(f'Error during moving {object_name} to S3')
            print(e)

    return True