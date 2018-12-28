"""Module that makes the requests to the binance_downloader api"""
import logging
from multiprocessing.pool import ThreadPool
from typing import Tuple, Dict, Optional

import pandas as pd
from tqdm import tqdm

from binance_downloader.binance_utils import (
    get_request_freq,
    KLINE_INTERVALS,
    interval_to_milliseconds,
    date_to_milliseconds,
    get_klines,
    earliest_valid_timestamp,
    kline_df_from_flat_list,
)
from binance_downloader.utils import rate_limited, ensure_dir

log = logging.getLogger()
log.setLevel(logging.DEBUG)


class BinanceAPI:
    """Make call to Binance api"""

    wait_time = get_request_freq(1).total_seconds()

    def __init__(self, interval: str, symbol: str, kwargs: Dict):
        self.base_url: str = "https://api.binance_downloader.com/api/v1/klines"
        self.symbol: str = symbol
        if (
            not interval
            or not isinstance(interval, str)
            or interval not in self.valid_intervals
        ):
            raise ValueError(
                f"{interval} not recognized as valid Binance K-line interval."
            )
        self.interval = interval
        self.klines = []
        self.start_time, self.end_time, self.limit = self._infer_date_parameters(kwargs)
        if self.limit is None:
            self.limit = 1000

        self.kline_df: Optional[pd.DataFrame] = None

    @property
    def valid_intervals(self):
        """return the intervals"""
        return KLINE_INTERVALS

    @rate_limited(1.0 / wait_time)
    def get_klines_helper(self, start_end_times):
        start, end = start_end_times
        return get_klines(
            self.symbol, self.interval, start_time=start, end_time=end, limit=self.limit
        )

    def fetch_parallel(self):
        # Get earliest possible kline
        # TODO: Locally cache earliest valid timestamp for Binance pairs
        earliest = earliest_valid_timestamp(self.symbol, self.interval)
        ms_start = max(self.start_time, earliest)
        ms_end = min(self.end_time, date_to_milliseconds("now"))
        ms_interval = interval_to_milliseconds(self.interval)
        req_limit = max(self.limit, 1000)

        # Create list of all start and end timestamps
        ranges = []
        _start = _end = ms_start
        while _end < ms_end - ms_interval:
            # Add some overlap to allow for small changes in interval on
            # Binance's side
            _end = min(
                _start + (req_limit - 1) * ms_interval,  # Cover full interval in msec
                ms_end - ms_interval,  # Cover up to given end date
            )
            # Add to list of all intervals we need to request
            ranges.append((_start, _end))
            # Add overlap (duplicates filtered out later) to ensure we don't miss
            # any of the range if Binance screwed up some of their data
            _start = _end - ms_interval * 10

        # Create workers for all needed requests and start them
        pool = ThreadPool()
        # Fetch in parallel, but block until all requests are received

        flat_results = []
        it = pool.imap(self.get_klines_helper, ranges)
        # Prevent more tasks being added to the pool
        pool.close()

        # Show progress meter
        with tqdm(total=len(ranges) * req_limit) as pbar:
            for r in it:
                pbar.update(req_limit)
                flat_results.extend(r)

        # Block until all workers are done
        pool.join()

        print("\nDone fetching in parallel")
        self.kline_df = kline_df_from_flat_list(flat_results)

    def write_to_csv(self, output=None):
        if self.kline_df is None:
            raise ValueError("Must read in data from Binance before writing to disk!")
        if output is None:
            timestamp = pd.Timestamp("now").strftime("%Y-%m-%d_%H%M%S")
            output = f"./downloaded/{timestamp}_{self.symbol}_klines.csv"
            ensure_dir(output)  # Create the directory if it doesn't exist
        with open(output, "w") as csv_file:
            self.kline_df.to_csv(csv_file, index=False, float_format="%.9f")

    def _infer_date_parameters(self, kwargs: dict) -> Tuple[int, int, Optional[int]]:
        start_date = kwargs.get("startTime", None)
        end_date = kwargs.get("endTime", None)
        limit = kwargs.get("limit", None)
        default_limit = 1000
        if start_date is not None:
            if end_date is not None:
                # Have start and end, so ignore limit
                log.info("Found start and end date, getting all and ignoring limit")
                limit = None
            elif limit is not None:
                # Have start and limit, but no end
                end_date = start_date + limit * interval_to_milliseconds(self.interval)
                log.info(f"Found start date and limit, fetching max of {limit} klines")
            else:
                # Start, but no end or limit
                log.info("Found start date, but no end date or limit. Fetching to now")
                end_date = date_to_milliseconds("now UTC")
        else:
            if end_date is not None:
                # End date and limit, but no start
                limit = limit or default_limit
                log.info(
                    f"Found end date and limit, fetching {limit} klines up to end date"
                )
                start_date = end_date - limit * interval_to_milliseconds(self.interval)
            else:
                # No start or end date
                end_date = date_to_milliseconds("now UTC")
                start_date = end_date - limit * interval_to_milliseconds(self.interval)
                limit = limit or default_limit
                log.info(f"No start or end date, fetching most recent {limit} klines")
        return start_date, end_date, limit
