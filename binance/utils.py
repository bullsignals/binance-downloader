"""some date functions"""
import threading
import time
from datetime import datetime
from functools import wraps

DATE_STRINGS = {"DMY": "%d/%m/%Y", "MDY": "%m/%d/%Y", "YMD": "%Y/%m/%d"}


def timestamp_to_datetime(timestamp, date_format: str = "DMY"):
    """Transform timestamp to python datetime"""
    desired_format = DATE_STRINGS.get(date_format, "DMY")
    return datetime.utcfromtimestamp(timestamp // 1000).strftime(
        f"{desired_format} %H:%M:%S"
    )


def update_start_time(new_start_time):
    """update start time to a new request"""
    return str(int(str(new_start_time)[:10]) + 1) + "000"


def rate_limited(max_per_second):
    """Prevents the decorated function from being called more than
    `max_per_second` times per second, locally, for one process

    """
    lock = threading.Lock()
    min_interval = 1.0 / max_per_second

    def decorate(func):
        last_time_called = time.perf_counter()

        @wraps
        def rate_limited_function(*args, **kwargs):
            with lock:
                nonlocal last_time_called
                elapsed = time.perf_counter() - last_time_called
                left_to_wait = min_interval - elapsed
                if left_to_wait > 0:
                    time.sleep(left_to_wait)
                last_time_called = time.perf_counter()
            return func(*args, **kwargs)

        return rate_limited_function

    return decorate
