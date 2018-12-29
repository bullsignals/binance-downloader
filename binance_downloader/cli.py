import argparse

from logbook import Logger

from .api import BinanceAPI
from .binance_utils import date_to_milliseconds

log = Logger(__name__)


def main():
    log.info("*" * 80)
    log.info("***" + "Starting CLI Parser for binance-downloader".center(74) + "***")
    log.info("*" * 80)
    parser = argparse.ArgumentParser(
        description="CLI for downloading Binance Candlestick (k-line) data in bulk"
    )
    parser.add_argument("symbol", help="(Required) Binance symbol pair, e.g. ETHBTC")
    parser.add_argument(
        "interval",
        help="(Required) Frequency interval in minutes(m); hours(h); days(d); weeks(w); months(M);"
        " All possibles values: 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M",
    )
    parser.add_argument(
        "--start", help="Start date to get data (inclusive). Format: yyyy/mm/dd"
    )
    parser.add_argument(
        "--end", help="End date to get data (exclusive). Format: yyyy/mm/dd"
    )
    parser.add_argument(
        "--output",
        help="File name to write data. Default: ./downloaded/timestamp_symbol_interval",
    )
    # Allow to choose MM/DD/YYYY for date input
    parser.add_argument(
        "--dtfmt",
        metavar="DATE_FORMAT",
        help="Format to use for dates (DMY, MDY, YMD, etc). Default: YMD",
        default="YMD",
    )

    args = parser.parse_args()

    if args.dtfmt:
        if args.dtfmt in ["DMY", "MDY", "YMD"]:
            date_format = args.dtfmt
        else:
            log.warn(f"Date format given ({args.dtfmt}) not known. Using YMD")
            date_format = "YMD"
    else:
        date_format = "YMD"

    if args.start:
        start_date = date_to_milliseconds(args.start, date_format=date_format)
    else:
        start_date = None

    if args.end:
        end_date = date_to_milliseconds(args.end, date_format=date_format)
    else:
        end_date = None

    symbol = str(args.symbol)
    interval = str(args.interval)
    binance = BinanceAPI(interval, symbol, start_date, end_date)
    binance.fetch_parallel()
    binance.write_to_csv()
    log.notice("download finished successfully.")
