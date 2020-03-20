import time
import requests
import json
from decimal import Decimal

pairs = {
    "BTC_ATOM",
    # "BTC_BCHABC",
    # "BTC_BCHSV",
    # "BTC_DASH",
    # "BTC_DOGE",
    # "BTC_EOS",
    # "BTC_ETC",
    # "BTC_ETH",
    # "BTC_LTC",
    # "BTC_MANA",
    # "BTC_STORJ",
    # "BTC_STR",
    # "BTC_STRAT",
    # "BTC_TRX",
    # "BTC_XEM",
    # "BTC_XMR",
    # "BTC_XRP",
    # "BTC_ZEC",

    # "ETH_ETC",
    # "ETH_ZEC",

    # "TRX_ETH",
    # "TRX_XRP",

    # "USDC_BTC",
    # "USDC_ETH",
    # "USDC_USDT",
    # "USDC_ZEC",

    # "USDT_BAT",
    # "USDT_BTC",
    # "USDT_DASH",
    # "USDT_DOGE",
    # "USDT_EOS",
    # "USDT_ETC",
    # "USDT_ETH",
    # "USDT_GNT",
    # "USDT_LSK",
    # "USDT_LTC",
    # "USDT_MANA",
    # "USDT_NXT",
    # "USDT_REP",
    # "USDT_STR",
    # "USDT_XMR",
    # "USDT_XRP",
    # "USDT_ZEC",
    # "USDT_ZRX",
}

def lambda_handler(event, context):
    all_books_response = requests.get('https://poloniex.com/public?command=returnOrderBook&currencyPair=all&depth=100')
    all_books = json.loads(all_books_response.text)
    #all_books = json.loads(all_books_response.text, parse_float=Decimal)
    filtered = { key :value for key, value in all_books.items() if key in pairs }

    return {
        "statusCode": 200,
        "body": json.dumps(filtered)
    }