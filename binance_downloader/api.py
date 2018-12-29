from multiprocessing.pool import ThreadPool
from typing import Tuple, Optional

import pandas as pd
from logbook import Logger
from tqdm import tqdm

from .binance_utils import (
    max_request_freq,
    KLINE_INTERVALS,
    interval_to_milliseconds,
    date_to_milliseconds,
    get_klines,
    earliest_valid_timestamp,
    kline_df_from_flat_list,
    KLINE_URL,
)
from .utils import ensure_dir, rate_limited

# Set up LogBook logging
log = Logger(__name__)


class BinanceAPI:

    max_per_sec = max_request_freq(req_weight=1)

    def __init__(self, interval, symbol, start_date, end_date):
        self.base_url = KLINE_URL
        # Binance limit per request is 1000 items
        self.req_limit = 1000
        self.symbol: str = symbol
        if (
            not interval
            or not isinstance(interval, str)
            or interval not in KLINE_INTERVALS
        ):
            raise ValueError(
                f"'{interval}' not recognized as valid Binance k-line interval."
            )
        self.interval = interval

        self.start_time, self.end_time = self._fill_dates(start_date, end_date)

        self.kline_df: Optional[pd.DataFrame] = None
        self.download_successful = False

    @rate_limited(max_per_sec)
    def fetch_blocks(self, start_end_times):
        start, end = start_end_times
        return get_klines(
            self.symbol,
            self.interval,
            start_time=start,
            end_time=end,
            limit=self.req_limit,
        )

    def fetch_parallel(self):
        # Get earliest possible kline
        earliest = earliest_valid_timestamp(self.symbol, self.interval)
        ms_start = max(self.start_time, earliest)
        ms_end = min(self.end_time, date_to_milliseconds("now"))
        ms_interval = interval_to_milliseconds(self.interval)

        # Create list of all start and end timestamps
        ranges = []
        _start = _end = ms_start
        while _end < ms_end - ms_interval:
            # Add some overlap to allow for small changes in interval on
            # Binance's side
            _end = min(
                _start
                + (self.req_limit - 1) * ms_interval,  # Cover full interval in msec
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
        it = pool.imap(self.fetch_blocks, ranges)
        # Prevent more tasks being added to the pool
        pool.close()

        # Show progress meter
        with tqdm(total=len(ranges) * self.req_limit) as pbar:
            for r in it:
                pbar.update(self.req_limit)
                flat_results.extend(r)

        # Block until all workers are done
        pool.join()

        self.kline_df = kline_df_from_flat_list(flat_results)
        if len(self.kline_df) == 0:
            log.warn(f"there are no k-lines for {self.symbol} at {self.interval} "
                     f"intervals on Binance between {pd.to_datetime(self.start_time, unit='ms')} "
                     f"and {pd.to_datetime(self.end_time, unit='ms')}")
        else:
            log.info("Done fetching in parallel")
            self.download_successful = True

    def write_to_csv(self, output=None):
        """Write k-lines retrieved from Binance into a csv file

        :param output: output file path. If none, will be stored in ./downloaded
            directory with a timestamped filename based on symbol pair and interval
        :return: None
        """
        if not self.download_successful:
            log.warn("Not writing to output file since no data was received from API")
            return

        if self.kline_df is None:
            raise ValueError("Must read in data from Binance before writing to disk!")

        # Generate default file name/path if none given
        output = output or self.output_file

        with open(output, "w") as csv_file:
            # Ensure 9 decimal places  (most prices are to 8 places)

            self.kline_df.to_csv(csv_file, index=False, float_format="%.9f")
        log.notice(f"Done writing {output} for {len(self.kline_df)} lines")

    @property
    def output_file(self, extension="csv"):
        timestamp = pd.Timestamp("now").strftime("%Y-%m-%d_%H%M%S")
        outfile = (
            f"./downloaded/{timestamp}_{self.symbol}_{self.interval}_klines.{extension}"
        )

        # Create the subdirectory if not present:
        ensure_dir(outfile)
        return outfile

    def _fill_dates(self, start: Optional[int], end: Optional[int]) -> Tuple[int, int]:

        # Get interval (in milliseconds) for limit * interval
        # (i.e. 1000 * 1m = 60,000,000 milliseconds)
        span = int(self.req_limit) * interval_to_milliseconds(self.interval)

        if start and end:
            log.info("Found start and end dates. Fetching full interval")
            return start, end
        elif start:
            # No end date, so go forward by 1000 intervals
            log.notice(f"Found start date but no end: fetching {self.req_limit} klines")
            end = start + span
        elif end:
            # No start date, so go back 1000 intervals
            log.notice(
                f"Found end date but no start. Fetching previous {self.req_limit} klines"
            )
            start = end - span
        else:
            # Neither start nor end date. Get most recent 1000 intervals
            log.notice(
                f"Neither start nor end dates found. Fetching most recent {self.req_limit} klines"
            )
            end = date_to_milliseconds("now")
            start = end - span

        return start, end
