import ccxt
import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys
sys.path.append('../../../src')
from logger import log
import time


exchanges = ['binance']
exceptionPairs = ['VEN/USDT', 'VEN/USD']
base = 'btc'
quote = 'usdt'


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
            if len(tokens) > 1 and tokens[0].lower() == base and\
                    tokens[1].lower() == quote:
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
            get_all(exchange, pair, freq)
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
    log.debug(f"new result {str(res.iloc[:3])}\n{len(res)}")
    res.to_csv(result_path, index=False)


def get_kline(ex, pair, freq, y=0, m=0):
    log.info(f"getting {freq} kline of {pair} from {ex}")
    try:
        data = []
        if y == 0 and m == 0:
            if freq == '1h':
                for year in [2017, 2018, 2019, 2020]:
                    for month in range(1, 13):
                        since = ex.parse8601(f'{year}-{"%02d" % month}-01T00:00:00Z')
                        d = ex.fetch_ohlcv(pair, timeframe=freq, since=since, limit=1000)
                        log.info(f'{pair} {freq} {year} {month} {len(d)} {pd.to_datetime(int(d[-1][0]), unit="ms")}')
                        data.extend(d)
            elif freq == '5m':
                def get_time_range(s_d, e_d):
                    date_list = pd.date_range(s_d, e_d).tolist()
                    return date_list
                today = datetime.utcnow().strftime("%Y%m%d")
                print(f'today {today}')
                time.sleep(5)
                tList = [ex.parse8601(t.strftime('%Y-%m-%dT00:00:00Z')) for t in get_time_range('20170101', today)]
                for since in tList:
                    d = ex.fetch_ohlcv(pair, timeframe=freq, since=since, limit=500)
                    log.info(f'{pair} {freq} {since} {len(d)} {pd.to_datetime(int(d[-1][0]), unit="ms")}')
                    data.extend(d)
        else:
            since = ex.parse8601(f'{y}-{m}-01T00:00:00Z')
            data = ex.fetch_ohlcv(pair, timeframe=freq, since=since)
        # data = ex.fetch_ohlcv(pair, timeframe=freq)
    except Exception as e:
        log.error("Error fetch_ohlcv: " + pair + " " + freq + ", e: " + str(e))
        return pd.DataFrame()
    df = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'vol'])
    log.info(f"fetch done")
    for d in data:
      df = df.append({
        "date": int(d[0]),
        "open": d[1],
        "high": d[2],
        "low": d[3],
        "close": d[4],
        "vol": d[5]
      }, ignore_index=True)
    log.info(f"append done")
    df = df.drop_duplicates('date')
    log.info(f"drop duplicates done")
    log.info(pair)
    log.info(df.iloc[-3:])
    return df


if __name__ == '__main__':
    log.reset('../log/fetchAll_log', log.DEBUG)
    log.info('test')
    for exchange_id in exchanges:
        getData(exchange_id, '5m', False)
