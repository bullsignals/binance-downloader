"""some date functions"""
from datetime import datetime


def timestamp_to_datetime(timestamp):
    return datetime.fromtimestamp(int(str(timestamp)[:10])).strftime("%d/%m/%Y %H:%M:%S")


def date_to_timestamp(date_str):
    timestamp = datetime.strptime(date_str, "%d/%m/%Y").timestamp()
    return str(int(timestamp)) + '000'


def update_start_time(time):
    return str(int(str(time)[:10]) + 1) + '000'
