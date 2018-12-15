"""some date functions"""

from datetime import datetime

DATE_STRINGS = {'DMY': "%d/%m/%Y",
                'MDY': "%m/%d/%Y",
                'YMD': "%Y/%m/%d"}


def timestamp_to_datetime(timestamp, dateformat='DMY'):
    """Transform timestamp to python datetime"""
    desired_format = DATE_STRINGS.get(dateformat, 'DMY')
    return datetime.utcfromtimestamp(timestamp // 1000).strftime(
        f"{desired_format} %H:%M:%S")


def update_start_time(time):
    """update start time to a new request"""
    return str(int(str(time)[:10]) + 1) + '000'
