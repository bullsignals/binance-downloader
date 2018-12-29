import json
import os
import threading
import time
from functools import wraps
from typing import Optional, Dict

from logbook import Logger

CACHE_DIR = "cache/"

log = Logger(__name__)


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


def json_from_cache(file_name: str) -> Optional[Dict]:
    json_path = os.path.join(CACHE_DIR, file_name)
    prev_json = {}
    try:
        with open(json_path, "r") as infile:
            prev_json = json.load(infile)
    except IOError:
        log.warn(f"Error reading JSON from {json_path}")

    return prev_json


def json_to_cache(new_json: Dict, file_name: str) -> None:
    json_path = os.path.join(CACHE_DIR, file_name)
    ensure_dir(json_path)
    with open(json_path, "w") as outfile:
        json.dump(new_json, outfile, ensure_ascii=False)
