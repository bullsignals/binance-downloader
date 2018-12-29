#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Kline downloader for Binance API"""
import sys

from logbook import StreamHandler, TimedRotatingFileHandler
from .utils import ensure_dir

__version__ = "0.2"

# Log to file (date-based)
LOG_FILENAME = "./logs/bd_applog.log"
ensure_dir(LOG_FILENAME)
TimedRotatingFileHandler(LOG_FILENAME, bubble=True).push_application()

# Log to stdout for info and above
StreamHandler(sys.stdout, level='NOTICE', bubble=True).push_application()


