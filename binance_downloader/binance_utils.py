import json
import logging
from typing import List, Union

import pandas as pd
import requests

log = logging.getLogger()

BASE_URL = "https://api.binance.com/api/v1"
KLINE_URL = BASE_URL + "/klines"

KLINE_INTERVALS = (
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
)

EXCHANGE_INFO_FILE = "exchange_info.json"


def get_request_freq(req_weight: int = 1, exchange_info: dict = None) -> pd.Timedelta:
    """Get smallest allowable frequency for API calls.

    The return value is the smallest permissible interval between calls to the API

    :param req_weight: (int) weight assigned to this type of request
    :param exchange_info: (dict) JSON response from a call to Binance `exchangeInfo`
        endpoint.
    :return: pandas.Timedelta of the smallest allowable time between calls
    """

    exchange_info = exchange_info or get_exchange_info()
    rate_limits = exchange_info["rateLimits"]
    request_limits = [
        rate for rate in rate_limits if "REQUEST" in rate["rateLimitType"]
    ]
    for rate in request_limits:
        interval = pd.Timedelta("%s %s" % (rate["intervalNum"], rate["interval"]))
        rate["req_freq"] = interval / rate["limit"]

    lowest_allowable_freq = pd.Timedelta(0)
    for limit in request_limits:
        # RAW_REQUESTS type may be present, which should be treated as a
        # request weight of 1
        weight = req_weight if limit["rateLimitType"] == "REQUEST_WEIGHT" else 1
        allowable_freq = limit["req_freq"] * weight
        lowest_allowable_freq = max(lowest_allowable_freq, allowable_freq)
    return lowest_allowable_freq


def get_exchange_info() -> dict:
    # Try to read in from disk:
    try:
        with open(EXCHANGE_INFO_FILE, "r") as infile:
            prev_json = json.load(infile)
    except IOError:
        print("Error reading in exchange info from JSON on disk. Fetching new")
    else:
        old_timestamp = pd.to_datetime(
            prev_json.get("serverTime", None), unit="ms", utc=True
        )
        age = pd.Timestamp("now", tz="utc") - old_timestamp
        print(f"Age of cached exchange info is {age}")
        if old_timestamp and age < pd.Timedelta("1 day"):
            # Data is OK to use
            return prev_json
    # Otherwise, get it again
    log.warning(
        "No exchange info given, so pulling this from API. Instead, cache "
        "exchange_info and pass it to the function to minimize requests"
    )

    req = requests.get(BASE_URL + "/exchangeInfo")
    if req.status_code != 200:
        raise ConnectionError(
            "Failed to get expected response from the API. Status "
            "code {} (error: {}): {}".format(
                req.status_code, req.json()["code"], req.json()["msg"]
            )
        )
    data = req.json()

    # Write out to disk for next time
    with open(EXCHANGE_INFO_FILE, "w") as outfile:
        json.dump(data, outfile, ensure_ascii=False)

    if isinstance(data, dict):
        return data
    else:
        raise ConnectionError("No exchange info returned from Binance")


def interval_to_milliseconds(interval) -> Union[int, None]:
    """Tries to get milliseconds from an interval input

    :param interval: (str, pandas.Timedelta, int)
        Interval in one of several types. Attempt to convert this value into
        milliseconds for a Binance API call
    :return: (int) milliseconds of the interval if successful, otherwise None
    """
    if isinstance(interval, pd.Timedelta):
        return int(interval.total_seconds() * 1000)
    elif isinstance(interval, int):
        log.info(
            "Assuming interval is already in milliseconds and " "returning unchanged"
        )
        return interval
    # Try to convert from a string
    seconds_per_unit = {"m": 60, "h": 60 * 60, "d": 24 * 60 * 60, "w": 7 * 24 * 60 * 60}
    try:
        return int(interval[:-1]) * seconds_per_unit[interval[-1]] * 1000
    except (ValueError, KeyError):
        return None


def date_to_milliseconds(date_str, date_format="YMD") -> int:
    day_first = date_format.upper() == "DMY"
    year_first = date_format.upper() == "YMD"
    epoch = pd.Timestamp(0, tz="utc")
    d = pd.to_datetime(date_str, yearfirst=year_first, dayfirst=day_first)
    if d.tz is None or d.tzinfo.utcoffset(d) is None:
        d = d.tz_localize("utc")
    return int((d - epoch).total_seconds() * 1000.0)


def get_klines(
    symbol, interval: str, start_time=None, end_time=None, limit=None
) -> List:
    """Helper function to get klines from Binance for a single request

    :param symbol: (str)
        Symbol pair of interest (e.g. 'XRPBTC')

    :param interval: (str)
        Valid kline interval (e.g. '1m').
    :param start_time: (int, str, pandas.Timestamp)
        First kline open time desired. If int, should be in milliseconds since
        Epoch. If string or pandas.Timestamp, will assume UTC unless otherwise
        specified.
    :param end_time: (int, str, pandas.Timestamp)
        Last kline open time desired. If int, should be in milliseconds since
        Epoch. If string or pandas.Timestamp, will assume UTC unless otherwise
        specified.
    :param limit: (int)
        Maximum number of klines to fetch. Will be clamped to 1000 if higher
        due to current maximum Binance limit. A value <= 0 will be assumed as
        no limit, and 1000 will be used.
    :return: List[List]]
        Returns a list of klines in list format if successful (may be empty list)
    """
    if not isinstance(symbol, str):
        raise ValueError(f"Cannot get kline for symbol {symbol}")
    if not isinstance(start_time, int) and start_time is not None:
        start_time = date_to_milliseconds(start_time)
    if not isinstance(end_time, int) and end_time is not None:
        end_time = date_to_milliseconds(end_time)
    if limit is None:
        limit = 1000
    elif limit > 1000:
        log.info("Clamping kline request limit to 1000")
        limit = 1000
    elif limit <= 0:
        log.info("Cannot have negative limit. Using 1000")
        limit = 1000

    # Set parameters and make the request
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if end_time is not None:
        params["endTime"] = end_time
    if start_time is not None:
        params["startTime"] = start_time

    response = requests.get(KLINE_URL, params=params)

    # Check for valid response
    if response.status_code in [429, 418]:
        raise ConnectionError(
            "Rate limits exceeded or IP banned: {}".format(response.json())
        )
    elif response.status_code % 100 == 4:
        raise ConnectionError("Request error: {}".format(response.json()))
    elif response.status_code % 100 == 5:
        raise ConnectionError(
            "API error, status is unknown: {}".format(response.json())
        )
    elif response.status_code != 200:
        raise ConnectionError(
            "Unknown error on kline request: {}".format(response.json())
        )

    return response.json()


def earliest_valid_timestamp(symbol: str, interval: str) -> int:
    if interval not in KLINE_INTERVALS:
        raise ValueError(f"{interval} is not a valid kline interval")
    kline = get_klines(symbol, interval, start_time=0, limit=1)
    return int(kline[0][0])


def kline_df_from_flat_list(flat_list: List):
    df = pd.DataFrame(
        flat_list,
        columns=[
            "OpenTime",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "CloseTime",
            "qav",
            "numTrades",
            "tbbav",
            "tbqav",
            "ignore",
        ],
    )
    # Fix dates
    df.OpenTime = pd.to_datetime(df.OpenTime, unit="ms")
    df.CloseTime = pd.to_datetime(df.CloseTime, unit="ms")
    # Fix numeric values
    for f in ["Open", "High", "Low", "Close", "Volume"]:
        df[f] = pd.to_numeric(df[f])
    # Sort by interval open
    df = df.sort_values("OpenTime")
    # Remove duplicates (from interval overlaps)
    df = df.drop_duplicates("OpenTime").reset_index(drop=True)
    return df
