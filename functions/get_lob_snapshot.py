import time
import requests
import json
from decimal import Decimal
import boto3

TABLE_NAME = "LOBDumps"

pairs = [
    "BTC_ATOM",
    "BTC_BCHABC",
    "BTC_BCHSV",
    "BTC_DASH",
    "BTC_EOS",
    "BTC_ETC",
    "BTC_ETH",
    "BTC_LTC",
    "BTC_MANA",
    "BTC_STORJ",
    "BTC_STR",
    "BTC_STRAT",
    "BTC_TRX",
    "BTC_XEM",
    "BTC_XMR",
    "BTC_XRP",
    "BTC_ZEC",

    "ETH_ETC",

    "TRX_ETH",
    "TRX_XRP",

    "USDC_BTC",
    "USDC_ETH",
    "USDC_USDT",

    "USDT_BTC",
    "USDT_DASH",
    "USDT_EOS",
    "USDT_ETH",
    "USDT_GNT",
    "USDT_LTC",
    "USDT_NXT",
    "USDT_STR",
    "USDT_XMR",
    "USDT_XRP",
    "USDT_ZEC",
    "USDT_ZRX",
]

dynamodb = boto3.resource('dynamodb', region_name='eu-west-2')
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    current_timestamp = int(time.time())
    response = requests.get('https://poloniex.com/public?command=returnOrderBook&currencyPair=all&depth=100')

    if response.status_code == 200:
        all_books = json.loads(response.text, parse_float=Decimal)
        filtered = { key :value for key, value in all_books.items() if key in pairs }

        for pair, book in filtered.items():
            table.put_item(
            Item = {
                    'Pair': pair,
                    'Timestamp': current_timestamp,
                    'book': book
                    }
            )
        return {"statusCode": 200, "body": "Success"}

    else:
        print(response)
        return {"statusCode": response.status_code, "body": response.text}