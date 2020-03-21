def lambda_handler(event, context):
    for response in event: 
        if response['statusCode'] != 200:
            raise Exception(response['body'])

    return True