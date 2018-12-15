"""Module that makes the requests to the binance api"""
import logging
import math

import requests
from binance.db import KLINE, to_csv
from binance.exceptions import IntervalException, ParamsException
from binance.utils import update_start_time
from tqdm import tqdm

log = logging.getLogger()
log.setLevel(logging.DEBUG)


class BinanceAPI:
    """Make consulta to binace api"""

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
        """return the intervals"""
        return ('1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h',
                '1d', '3d', '1w', '1M')

    def consult(self, output):
        """Main method to make the consult to the binance api"""
        default = 500
        limit = int(self.kwargs.get('limit', default))
        acc = 0
        number_loops = math.ceil(limit / default)
        with tqdm(total=limit) as pbar:
            for _ in range(number_loops):
                self.set_limit(limit, acc, default)
                self.request()
                # from the second request forward
                # the first item returned is equal to the last item from previous request
                # and it was already written in the file.  
                if len(self.klines) > 0:
                    to_csv(self.klines, output)
                    if 'startTime' in self.kwargs:
                        # The last returned date will be the startTime of the next request
                        self.kwargs['startTime'] = update_start_time(
                            self.klines[-1].open_time)
                    pbar.update(self.kwargs['limit'])
                    acc += self.kwargs['limit']
                else:
                    if self.klines:
                        # No updates from this iteration, but there were some
                        # from previous request
                        print('All records downloaded for the provided date range.')
                    return

    def set_limit(self, limit, acc, default):
        """set the limit to the next consult"""
        if (limit - acc) <= default:
            self.kwargs['limit'] = limit - acc
        else:
            self.kwargs['limit'] = default

    def request(self):
        """Call the method that make request to binance api
        and the method to generate the klines list."""
        response = self._resquest_api()

        # Check the status code to see if we got an expected result
        st_code = response.status_code
        if st_code % 100 == 4:
            print('There was an error with this request:')
            if st_code == 429:
                print(f'Binance API rate limit was exceeded (error {st_code})')
            elif st_code == 418:
                print(f'Binance API has auto-banned this IP (error {st_code})')
            else:
                print(f'Unknown Binance API status code: {st_code} - ({response.json()})')
        elif st_code % 100 == 5:
            print(f'Binance internal error returned status code: {st_code}. '
                  'The operation may not have completed successfully')
        # There are cases where the request may return successfully, but there
        # were no results in that range. In this case, inform the user instead
        # of just the generic 'completed' message
        if st_code == 200 and not response.json():
            print('The request completed successfully, but no results were '
                  'returned.\nBinance may not have any data for the '
                  'date range requested.')

        self._generate_list_of_kline_numedtuple(response.json())

    def _resquest_api(self):
        """make request to the binance api"""
        data = {'symbol': self.symbol, 'interval': self.interval}
        payload = {**data, **self.kwargs}
        try:
            log.debug('calling ' + self.base_url)
            response = requests.get(self.base_url, params=payload)
        except requests.exceptions.RequestException as exc:
            raise exc
        else:
            return response

    def _generate_list_of_kline_numedtuple(self, response):
        """return a list of Kline numedTuple to make attributes access easy"""
        self.klines = [KLINE(*kline) for kline in response]
