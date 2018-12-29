"""some date functions"""
import os
import threading
import time
from functools import wraps


def rate_limited(max_per_second):
    """Prevents the decorated function from being called more than
    `max_per_second` times per second, locally, for one process

    """
    lock = threading.Lock()
    min_interval = 1.0 / max_per_second

    def decorate(func):
        last_time_called = time.perf_counter()

        @wraps(func)
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


def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
