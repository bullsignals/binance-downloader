"""Armazena dados em csv"""
from collections import namedtuple
import csv
import os
from datetime import datetime

from binance.utils import timestamp_to_datetime

KLINE = namedtuple('Kline', ("open_time", "open_", "high", "low", "close",
                             "volume", "close_time", "quote_asset_volume",
                             "number_of_trades", "taker_by_bav", "taker_by_qav",
                             "ignored"))


def to_csv(klines, output='binance'):
    headers = ['date', 'open', 'high', 'low', 'close', 'volume']
    output = "{}.csv".format(output)
    exist_output = os.path.exists(output)
    with open(output, 'a', newline=None) as f:
        f_csv = csv.writer(f)
        if not exist_output:
            f_csv.writerow(headers)

        for k in klines:
            date = timestamp_to_datetime(k.open_time)
            row = (date, k.open_, k.high, k.low, k.close, k.volume)
            f_csv.writerow(row)
