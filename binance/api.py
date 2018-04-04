import logging

import requests

from binance.db import KLINE
from binance.exceptions import IntervalException, ParamsException

log = logging.getLogger()
log.setLevel(logging.DEBUG)


class BinanceAPI:
    def __init__(self, interval, symbol, kwargs):
        self.base_url = 'https://api.binance.com/api/v1/klines?'
        self.symbol = symbol
        self.interval = interval
        self.kwargs = kwargs
        self.klines = []

        if not self.interval:
            raise ParamsException("Interval must have used!")

        if self.interval not in self.intervals:
            raise IntervalException("Interval not in intervals list")

    @property
    def intervals(self):
        return ('1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h',
                '1d', '3d', '1w', '1M')

    def consult(self):
        response = self._resquest_api()
        self._generate_list_of_kline_numedtuple(response)

    def _resquest_api(self):
        d = {'symbol': self.symbol, 'interval': self.interval}
        payload = {**d, **self.kwargs}
        try:
            log.debug('calling ' + self.base_url)
            response = requests.get(self.base_url, params=payload)
        except requests.exceptions.RequestException as he:
            raise he
        else:
            return response.json()

    def _generate_list_of_kline_numedtuple(self, response):
        """return a list of Kline numedTuple to make attributes access easy"""
        self.klines = [KLINE(*kline) for kline in response]
