"""Armazena dados em csv"""
from collections import namedtuple
import csv
from datetime import datetime

KLINE = namedtuple('Kline', ("open_time", "open_", "high", "low", "close",
                             "volume", "close_time", "quote_asset_volume",
                             "number_of_trades", "taker_by_bav", "taker_by_qav",
                             "ignored"))
PATH = "binance.csv"

def to_csv(klines):
    headers = ['date', 'open', 'high', 'low', 'close', 'volume']
    with open(PATH, 'w') as f:
        f_csv = csv.writer(f)
        f_csv.writerow(headers)
        for k in klines:
            # tive que fazer um trabalho na hora de pegar o timestamp
            # pois o open_time vem com 13 dígitos e estava dando erro com o datetime
            # então eu só pego os 10 primeiro e funciona perfeitamente
            date = datetime.fromtimestamp(int(str(1522022400000)[:10])).strftime("%d/%m/%Y %H:%M:%S")
            row = (date, k.open_, k.high, k.low, k.close, k.volume)
            f_csv.writerow(row)