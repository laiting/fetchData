import ccxt
import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys
sys.path.append('../../../src')
from logger import log
import time


exchanges = ['huobipro', 'binance', 'coinbase','coinbasepro','bitstamp1', 'bitstamp', 'kraken', 'okex']
# exchanges = ['huobipro', 'binance', 'coinbase','coinbasepro','bitstamp1', 'bitstamp', 'kraken', 'okex', 'gemini']
exceptionPairs = ['VEN/USDT', 'VEN/USD']
coinList = ['btc', 'bch', 'eth', 'eos', 'ltc', 'xrp', 'etc']


def getData(exchange_id, freq='1h', routinely=True):
    exchange = getattr(ccxt, exchange_id)()
    exchange.options['adjustForTimeDifference'] = False
    markets = exchange.load_markets()
    count = 0
    pairs = []
    if exchange_id == 'bitstamp1':
        count += 1
        pairs.append('BTC/USD')
    else:
        for k,v in exchange.markets.items():
            if k in exceptionPairs:
                continue
            tokens = k.split('/')
            if len(tokens) > 1 and tokens[0].lower() in coinList and\
                    (tokens[1] == 'USDT' or tokens[1] == 'USD' or tokens[1] == 'BTC' or tokens[1] == 'ETH'):
                count += 1
                pairs.append(k)
    log.info(f"### {exchange_id} {exchange.has['fetchOHLCV']} {count} ###")
    if not exchange.has['fetchOHLCV']:
        return
    if routinely:
        for pair in pairs:
            daily_update(exchange, pair, freq)
            time.sleep(0.5)
    else:
        for pair in pairs:
            daily_update(exchange, pair, freq)
            time.sleep(0.5)


def daily_update(exchange, pair, frec):
    current = pd.to_datetime(datetime.utcnow())
    y = current.year
    m = "%02d" % current.month
    symbol = pair.replace('/', '_')
    if not os.path.isdir(f"../data/{exchange.id}"):
        os.mkdir(f"../data/{exchange.id}")
    result_path = f"../data/{exchange.id}/{symbol}_{frec}_{y}{m}.csv"
    log.info(f"reading result_path:{result_path}")
    cur_result = pd.DataFrame()
    if os.path.isfile(result_path):
        try:
            cur_result = pd.read_csv(result_path)
            log.debug(f"cur_result \n{cur_result.iloc[:3]}\n{cur_result.iloc[-3:]}")
        except Exception as e:
            log.error("Error result file: " + result_path + ", e: " + str(e))
    else:
        log.info("new result file: " + result_path)
    df = get_kline(exchange, pair, frec, y, m)
    if len(df) > 0:
        df['date'] = df['date'].astype(int)
        df = df.sort_values('date', ascending=False)
    res = df.iloc[1:].copy()
    res = res.append(cur_result)
    res.drop_duplicates('date', keep='first', inplace=True)
    log.debug("new result " + str(res.iloc[:3]))
    res.to_csv(result_path, index=False)


def get_all(exchange, pair, frec):
    current = pd.to_datetime(datetime.utcnow())
    y = current.year
    m = "%02d" % current.month
    symbol = pair.replace('/', '_')
    if not os.path.isdir(f"../data/{exchange.id}"):
        os.mkdir(f"../data/{exchange.id}")
    result_path = f"../data/{exchange.id}/{symbol}_{frec}_until_{y}{m}.csv"
    log.info(f"reading result_path:{result_path}")
    cur_result = pd.DataFrame()
    if os.path.isfile(result_path):
        try:
            cur_result = pd.read_csv(result_path)
            log.debug(f"cur_result \n{cur_result.iloc[:3]}\n{cur_result.iloc[-3:]}")
        except Exception as e:
            log.error("Error result file: " + result_path + ", e: " + str(e))
    else:
        log.info("new result file: " + result_path)
    df = get_kline(exchange, pair, frec)
    if len(df) > 0:
        df['date'] = df['date'].astype(int)
        df = df.sort_values('date', ascending=False)
    res = df.iloc[1:].copy()
    res = res.append(cur_result)
    res.drop_duplicates('date', keep='first', inplace=True)
    log.debug("new result " + str(res.iloc[:3]))
    res.to_csv(result_path, index=False)


def get_kline(ex, pair, freq, y=0, m=0):
    log.info(f"getting {freq} kline of {pair} from {ex}")
    try:
        data = []
        if y == 0 and m == 0:
            for y in [2017, 2018, 2019, 2020]:
                for m in range(1, 13):
                    since = ex.parse8601(f'{y}-{m}-01T00:00:00Z')
                    d = ex.fetch_ohlcv(pair, timeframe=freq, since=since)
                    data.append(d)
        else:
            since = ex.parse8601(f'{y}-{m}-01T00:00:00Z')
            data = ex.fetch_ohlcv(pair, timeframe=freq, since=since)
        # data = ex.fetch_ohlcv(pair, timeframe=freq)
    except Exception as e:
        log.error("Error fetch_ohlcv: " + pair + " " + freq + ", e: " + str(e))
        return pd.DataFrame()
    df = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'vol'])
    for d in data:
      df = df.append({
        "date": int(d[0]),
        "open": d[1],
        "high": d[2],
        "low": d[3],
        "close": d[4],
        "vol": d[5]
      }, ignore_index=True)
    log.debug(pair)
    log.debug(df.iloc[-3:])
    return df


if __name__ == '__main__':
    log.reset('../log/log', log.INFO)
    log.reset('', log.DEBUG)
    log.info('test')
    for exchange_id in exchanges:
        getData(exchange_id, '1h')
        getData(exchange_id, '1d')
