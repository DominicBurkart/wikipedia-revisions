import datetime


def timestr() -> str:
    return datetime.datetime.now().isoformat()
