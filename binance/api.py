"""Module that makes the requests to the binance api"""
import logging
import math
from typing import Tuple

import requests
from binance.db import KLINE, to_csv
from binance.exceptions import IntervalException, ParamsException
from binance.utils import update_start_time
from binance.binance_utils import (
    get_request_freq,
    KLINE_INTERVALS,
    interval_to_milliseconds,
    date_to_milliseconds,
    get_klines,
)
from tqdm import tqdm
from requests_futures.sessions import FuturesSession
from time import sleep
import pandas as pd

from binance_utils import earliest_valid_timestamp
from utils import rate_limited

log = logging.getLogger()
log.setLevel(logging.DEBUG)


class BinanceAPI:
    """Make call to Binance api"""

    wait_time = get_request_freq(1).total_seconds()

    def __init__(self, interval, symbol, kwargs, date_format="DMY"):
        self.base_url: str = "https://api.binance.com/api/v1/klines"
        self.symbol: str = symbol
        self.interval: pd.Timestamp = interval
        self.kwargs: dict = kwargs  # TODO: get rid of this
        self.klines = []
        self.date_format = date_format
        self.start_time, self.end_time, self.limit = self._infer_date_parameters(kwargs)

        if not self.interval:
            raise ParamsException("Interval must have used!")

        if self.interval not in self.intervals:
            raise IntervalException("Interval not in intervals list")

    @property
    def intervals(self):
        """return the intervals"""
        return KLINE_INTERVALS

    def consult(self, output):
        """Main method to make the consult to the Binance API

        :param: output: the CSV file to which data will be written

        :return: None

        """
        print(
            "Minimum allowable time between requests: ",
            get_request_freq(1).total_seconds(),
        )
        default = 500
        limit = int(self.kwargs.get("limit", default))
        acc = 0
        number_loops = math.ceil(limit / default)
        with tqdm(total=limit) as pbar:
            for _ in range(number_loops):
                self.set_limit(limit, acc, default)
                self.request()
                # from the second request forward
                # the first item returned is equal to the last item from prev. request
                # and it was already written in the file.
                if len(self.klines) > 0:
                    to_csv(self.klines, output, self.date_format)
                    if "startTime" in self.kwargs:
                        # The last returned date is the startTime of the next request
                        self.kwargs["startTime"] = update_start_time(
                            self.klines[-1].open_time
                        )
                    pbar.update(self.kwargs["limit"])
                    acc += self.kwargs["limit"]
                else:
                    print("All records downloaded for the provided date range.")
                    break

    def set_limit(self, limit, acc, default):
        """set the limit to the next consult"""
        if (limit - acc) <= default:
            self.kwargs["limit"] = limit - acc
        else:
            self.kwargs["limit"] = default

    def request(self):
        """Call the method that make request to binance api
        and the method to generate the klines list."""
        response = self._request_api()
        self._generate_list_of_kline_namedtuple(response)

    def _request_api(self):
        """make request to the binance api"""
        data = {"symbol": self.symbol, "interval": self.interval}
        payload = {**data, **self.kwargs}
        try:
            log.debug("calling " + self.base_url)
            response = requests.get(self.base_url, params=payload)
        except requests.exceptions.RequestException as exc:
            raise exc
        else:
            return response.json()

    def _generate_list_of_kline_namedtuple(self, response):
        """return a list of Kline namedtuple to make attributes access easy"""
        self.klines = [KLINE(*kline) for kline in response]

    @rate_limited(1.0 / wait_time)
    def get_klines_helper(self, start_end_times: Tuple[int, int]):
        start, end = start_end_times
        return get_klines(
            self.symbol, self.interval, start_time=start, end_time=end, limit=self.limit
        )

    def fetch_parallel(self):
        # Get earliest possible kline
        earliest = pd.Timestamp(
            earliest_valid_timestamp(self.symbol, self.interval), unit='ms', tz='utc')
        self.start_time = max(self.start_time, earliest)

        # Create list of all start and end timestamps

        # Create workers for all needed requests and start them

        # Close out the pool and flatten the list

    def fetch_parallel_sessions(self, output_filename):
        # First, calculate how many total klines are needed
        # To fulfill the start/end/limit criteria
        start_date, end_date, limit = self._infer_date_parameters()

        # How many klines will be fetched?
        interval_msecs = interval_to_milliseconds(self.interval)

        # Create a list of tuples (start_msec, end_msec) for each request
        # Binance API currently allows for 1000 klines per request
        request_limit = 1000
        request_ranges = []
        step = interval_msecs * request_limit
        for i in range(start_date, end_date, step):
            # End date is exclusive, so stop one interval before that
            request_ranges.append((i, min(end_date - 1, i + step)))

        # Set the static request parameters
        req_params = {
            "symbol": self.symbol,
            "interval": self.interval,
            "limit": request_limit,
        }

        # Create a list to hold the responses
        futures = []
        session = FuturesSession()

        # Need to avoid exceeding API rate limits:
        last_query_time = pd.Timestamp.now()

        # Start creating parallel requests
        with tqdm(total=len(request_ranges)) as pbar:
            complete_reqs = 0
            for r in request_ranges:
                # Add params specific to this request
                req_params["startTime"], req_params["endTime"] = r

                # Throttle if needed
                now = pd.Timestamp.now()
                delta = (now - last_query_time).total_seconds()
                last_query_time = now
                if delta <= self.wait_time:
                    sleep(self.wait_time - delta)

                # Make the request and add it to the list of futures
                futures.append(session.get(self.base_url, params=req_params))

                # Update the progress bar
                dones = sum([1 for f in futures if f.done()])
                pbar.update(dones - complete_reqs)
                complete_reqs = dones

            # Parallel requests are all queued now, so just wait for all responses
            while any([f.running() for f in futures]):
                dones = sum([1 for f in futures if f.done()])
                pbar.update(dones - complete_reqs)
                complete_reqs = dones
            log.info("All requests responses received")

        # Get results
        all_results = [f.result().json() for f in futures]
        all_klines = [kl for sublist in all_results for kl in sublist]

        # Create a pandas DataFrame and then send to CSV
        column_names = [
            "Open time",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "Close time",
            "QAV",
            "Num trades",
            "TBBAV",
            "TBQAV",
            "Ignore",
        ]
        df = pd.DataFrame(all_klines, columns=column_names)

        # Fix dtypes
        df["Open time"] = pd.to_datetime(df["Open time"], unit="ms")
        df["Close time"] = pd.to_datetime(df["Close time"], unit="ms")
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = pd.to_numeric(df[col])

        # Write to csv
        with open(f"{output_filename}.csv", "w") as csv_file:
            df.to_csv(csv_file, index=False)

    def _infer_date_parameters(self, kwargs: dict) -> Tuple[int, int, int]:
        start_date = self.kwargs.get("startTime", None)
        end_date = self.kwargs.get("endTime", None)
        limit = self.kwargs.get("limit", None)
        default_limit = 1000
        if start_date is not None:
            if end_date is not None:
                # Have start and end, so ignore limit
                log.info("Found start and end date, getting all and ignoring limit")
                limit = None
            elif limit is not None:
                # Have start and limit, but no end
                end_date = start_date + limit * interval_to_milliseconds(self.interval)
                log.info(f"Found start date and limit, fetching {limit} klines")
            else:
                # Start, but no end or limit
                log.info("Found start date, but no end date or limit. Fetching to now")
                end_date = date_to_milliseconds("now UTC")
        else:
            if end_date is not None:
                # End date and limit, but no start
                limit = limit or default_limit
                log.info(f"Found end date and limit, fetching previous {limit} klines")
                start_date = end_date - limit * interval_to_milliseconds(self.interval)
            else:
                # No start or end date
                end_date = date_to_milliseconds("now UTC")
                start_date = end_date - limit * interval_to_milliseconds(self.interval)
                limit = limit or default_limit
                log.info(f"No start or end date, fetching previous {limit} klines")
        return start_date, end_date, limit or default_limit
