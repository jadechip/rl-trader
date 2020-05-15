from datetime import datetime


def format_time(timestamp) -> datetime:
    return datetime.utcfromtimestamp(int(timestamp)/1000).strftime('%Y-%m-%d %H:%M:%S')