import pandas as pd
import requests
import logging

log = logging.getLogger()

BASE_URL = 'https://api.binance.com/api/v1'


def get_request_freq(req_weight: int = 1, exchange_info: dict = None) -> pd.Timedelta:
    if exchange_info is None:
        log.warning(
            'No exchange info passed to {name}, so pulling this from '
            'API. Instead, cache exchange_info and pass it to the '
            'function to minimize requests'.format(
                name=get_request_freq.__name__))
        exchange_info = get_exchange_info()
    rate_limits = exchange_info['rateLimits']
    request_limits = [rate for rate in rate_limits if 'REQUEST' in rate['rateLimitType']]
    for rate in request_limits:
        interval = pd.Timedelta('%s %s' % (rate['intervalNum'], rate['interval']))
        rate['req_freq'] = interval / rate['limit']

    lowest_allowable_freq = pd.Timedelta(0)
    for limit in request_limits:
        # RAW_REQUESTS type may be present, which should be treated as a
        # request weight of 1
        weight = req_weight if limit['rateLimitType'] == 'REQUEST_WEIGHT' else 1
        allowable_freq = limit['req_freq'] * weight
        lowest_allowable_freq = max(lowest_allowable_freq, allowable_freq)
    return lowest_allowable_freq


def get_exchange_info() -> dict:
    req = requests.get(BASE_URL + '/exchangeInfo')
    if req.status_code != 200:
        raise ConnectionError(
            'Failed to get expected response from the API. Status '
            'code {} (error: {}): {}'.format(req.status_code,
                                             req.json()['code'],
                                             req.json()['msg']))
    data = req.json()
    if isinstance(data, dict):
        return data
    else:
        raise ConnectionError('No exchange info returned from Binance')
