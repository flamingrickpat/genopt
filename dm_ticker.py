import json
import os

import ccxt
import pandas as pd
import requests

import file_utils
from dm_utils import *
from file_utils import alogger


@alogger
def update_ticker_binance(directory, timeframe, pairs, initial_candles, zeitpunkt=None, postfix=""):
    return _update_ticker_binance(directory, timeframe, pairs, initial_candles, 0, end=zeitpunkt, postfix=postfix)

@alogger
def update_ticker_binance_futures_usdt(directory, timeframe, pairs, initial_candles, zeitpunkt=None, postfix=""):
    return _update_ticker_binance(directory, timeframe, pairs, initial_candles, 1, end=zeitpunkt, postfix=postfix)

@alogger
def update_ticker_binance_futures_coin(directory, timeframe, pairs, initial_candles, zeitpunkt=None, postfix=""):
    return _update_ticker_binance(directory, timeframe, pairs, initial_candles, 2, end=zeitpunkt, postfix=postfix)


@alogger
def update_ticker_forexsb(directory, timeframe, pairs, initial_candles):
    for pair in pairs:
        p = pair.replace("-", "").replace("/", "").replace("\\", "")
        path = get_path(pair, directory, ".csv")

        url = "https://data.forexsb.com/%s%s.gz" % (p, str(timeframe))
        r = requests.get(url)
        data = json.loads(r.content)

        t = data["time"]
        o = data["open"]
        h = data["high"]
        l = data["low"]
        c = data["close"]
        v = data["volume"]
        df = pd.DataFrame(list(zip(t, o, h, l, c, v)), columns=['time', 'open', 'high', 'low', 'close', 'volume'])
        df["time"] = 946684800 + (60 * df["time"]) + (60 * timeframe) # End Of Candle!
        df['date'] = pd.to_datetime(df['time'],unit='s')

        df.to_csv(path)

def fetch_ohlcv_v2(self, symbol, timeframe='1m', since=None, limit=None, params={}):
    try:
        self.load_markets()
        market = self.market(symbol)
        request = {
            'symbol': market['id'],
            'interval': self.timeframes[timeframe],
        }
    except:
        symbol = symbol.replace("/", "")
        self.load_markets()
        market = self.market(symbol)
        request = {
            'symbol': market['id'],
            'interval': self.timeframes[timeframe],
        }

    if since is not None:
        request['startTime'] = since
    if limit is not None:
        request['limit'] = limit

    defaultType = self.safe_string_2(self.options, 'fetchOrders', 'defaultType', market['type'])
    type = self.safe_string(params, 'type', defaultType)

    method = 'publicGetKlines'
    if type == 'future':
        method = 'fapiPublicGetKlines'
    elif type == 'delivery':
        method = 'dapiPublicGetKlines'
    response = getattr(self, method)(self.extend(request, params))
    return self.parse_ohlcvs(response, market, timeframe, since, limit)


def _binance_get_server_time():
    url = "https://fapi.binance.com/fapi/v1/time"
    r = requests.get(url)
    data = json.loads(r.content)
    return data["serverTime"]

@alogger
def _update_ticker_binance(directory, timeframe, pairs, initial_candles, mode, end=None, postfix=""):
    """
    Args:
        directory:
        timeframe:
        pairs:
        initial_candles:
        mode: 0 ... Spot, 1 ... Future (USDT), 2 ... Delivery (COIN)

    Returns:

    """

    if end is None:
        end = int(_binance_get_server_time() / 1000)

    ccxt_exchange = ccxt.binance()

    if mode == 1:
        ccxt_exchange.options = {
            'defaultType': 'future',
            'adjustForTimeDifference': True
        }
    elif mode == 2:
        ccxt_exchange.options = {
            'defaultType': 'delivery',
            'adjustForTimeDifference': True
        }

    if timeframe < 60:
        tf = str(timeframe) + "m"
    else:
        tf = str(int(timeframe / 60)) + "h"

    for pair in pairs:
        path = get_path(pair, directory, postfix + ".json")

        # Init
        since_ms = int((end - (initial_candles * timeframe * 60)) * 1000)
        old = []

        # Gibts schon Daten?
        if os.path.isfile(path):
            logger.info("Candles gefunden: " + pair)
            old = file_utils.load_json(path)
            since_ms = old[-1][0] - (100 * timeframe * 60 * 1000)

        p = pair

        # Neue Daten holen
        server_time = end * 1000

        new = fetch_ohlcv_v2(ccxt_exchange, p, timeframe=tf, since=since_ms, limit=500)
        while True:
            since_ms = new[-1][0] + 1
            tmp = fetch_ohlcv_v2(ccxt_exchange, p, timeframe=tf, since=since_ms, limit=500)
            new += tmp

            if len(tmp) < 500 or  tmp[-1][0] > server_time:
                break

        while len(new) > 0:
            if new[-1][0] + (timeframe * 60 * 1000) > server_time:
                del new[-1]
            else:
                break

        # Nur neue Daten behalten!
        start = new[0][0]
        for i in range(len(old)):
            if old[i][0] > start:
                old = old[0:i]
                break

        data = old + new


        for i in range(len(data)):
            data[i][0] = data[i][0] + (timeframe * 60 * 1000)

        # Speichern
        file_utils.save_json(data, path)
        logger.info("Neues JSON Länge: " + str(len(data)))