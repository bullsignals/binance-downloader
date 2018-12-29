import json
from typing import List, Union

import pandas as pd
import requests
from logbook import Logger

log = Logger(__name__)

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
EARLIEST_TIMESTAMPS_FILE = "earliest_timestamps.json"


def max_request_freq(req_weight: int = 1) -> float:
    """Get smallest allowable frequency for API calls.

    The return value is the maximum number of calls allowed per second

    :param req_weight: (int) weight assigned to this type of request
        Default: 1-weight
    :return: float of the maximum calls permitted per second
    """

    # Pull Binance exchange metadata (including limits) either from cached value, or
    # from the server if cached data is too old
    request_limits = _req_limits(get_exchange_info())

    for rate in request_limits:
        # Convert JSON response components (e.g.) "5" "minutes" to a Timedelta
        interval = pd.Timedelta(f"{rate['intervalNum']} {rate['interval']}")
        # Frequency (requests/second) is, e.g., 5000 / 300 sec or 1200 / 60 sec
        rate["req_freq"] = int(rate["limit"]) / interval.total_seconds()

    max_allowed_freq = None

    for limit in request_limits:
        # RAW_REQUESTS type should be treated as a request weight of 1
        weight = req_weight if limit["rateLimitType"] == "REQUEST_WEIGHT" else 1
        this_allowed_freq = limit["req_freq"] * weight

        if max_allowed_freq is None:
            max_allowed_freq = this_allowed_freq
        else:
            max_allowed_freq = min(max_allowed_freq, this_allowed_freq)

    log.info(
        f"Maximum permitted request frequency for weight {req_weight} is "
        f"{max_allowed_freq} / sec"
    )

    if max_allowed_freq is None:
        return 0
    else:
        return max_allowed_freq


def _req_limits(exchange_info: dict) -> List:
    return [
        rate
        for rate in (exchange_info["rateLimits"])
        if "REQUEST" in rate["rateLimitType"]
    ]


def get_exchange_info() -> dict:
    refresh_after = pd.Timedelta("1 day")
    # Try to read in from disk:
    try:
        with open(EXCHANGE_INFO_FILE, "r") as infile:
            prev_json = json.load(infile)
    except IOError:
        log.warn("Error reading in exchange info from JSON on disk. Fetching new")
    else:
        old_timestamp = pd.to_datetime(
            prev_json.get("serverTime", None), unit="ms", utc=True
        )
        age = pd.Timestamp("now", tz="utc") - old_timestamp

        if old_timestamp and age <= refresh_after:
            # Data is OK to use
            log.info(
                f"Using cached exchange info since its age ({age}) is less "
                f"than {refresh_after}"
            )
            return prev_json
        else:
            # Otherwise, get it again
            log.notice(
                "Exchange info cached data either not available or stale. "
                "Pulling from server..."
            )

    response = requests.get(BASE_URL + "/exchangeInfo")
    _validate_api_response(response)
    data = response.json()

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
        log.info(f"Assuming interval '{interval}' is already in milliseconds")
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
        log.warn("Clamping kline request limit to 1000")
        limit = 1000
    elif limit <= 0:
        log.warn("Cannot have negative limit. Using 1000")
        limit = 1000

    # Set parameters and make the request
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if end_time is not None:
        params["endTime"] = end_time
    if start_time is not None:
        params["startTime"] = start_time

    response = requests.get(KLINE_URL, params=params)

    # Check for valid response
    _validate_api_response(response)

    return response.json()


def _validate_api_response(response):
    if response.status_code in [429, 418]:
        raise ConnectionError(f"Rate limits exceeded or IP banned: {response.json()}")
    elif response.status_code % 100 == 4:
        raise ConnectionError(f"Request error: {response.json()}")
    elif response.status_code % 100 == 5:
        raise ConnectionError(f"API error, status is unknown: {response.json()}")
    elif response.status_code != 200:
        raise ConnectionError(f"Unknown error on kline request: {response.json()}")


def earliest_valid_timestamp(symbol: str, interval: str) -> int:
    if interval not in KLINE_INTERVALS:
        raise ValueError(f"{interval} is not a valid kline interval")

    # Check for locally cached response
    identifier = f"{symbol}_{interval}"
    prev_json = {}
    try:
        with open(EARLIEST_TIMESTAMPS_FILE, "r") as infile:
            prev_json = json.load(infile)
    except IOError:
        log.info(f"Could not open {EARLIEST_TIMESTAMPS_FILE}")
    else:
        # Loaded JSON from disk, check if we already have this value:
        timestamp = prev_json.get(identifier, None)
        if timestamp is not None:
            log.info(
                f"Found cached earliest timestamp for {identifier}: "
                f"{pd.to_datetime(timestamp, unit='ms')}"
            )
            return timestamp
    log.info(f"No cached earliest timestamp for {identifier}, so fetching from server")

    # This will return the first recorded k-line for this interval and symbol
    kline = get_klines(symbol, interval, start_time=0, limit=1)

    # Get just the OpenTime value (timestamp in milliseconds)
    earliest_timestamp = int(kline[0][0])

    # Cache on disk
    prev_json[identifier] = earliest_timestamp
    with open(EARLIEST_TIMESTAMPS_FILE, "w") as outfile:
        json.dump(prev_json, outfile, ensure_ascii=False)

    log.info(f"Wrote new data to {EARLIEST_TIMESTAMPS_FILE} for {identifier}")

    return earliest_timestamp


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
