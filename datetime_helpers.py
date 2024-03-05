from datetime import datetime


def get_month(timestamp: int) -> int:
    # timestamp is the number of milliseconds since the epoch
    return int(datetime.utcfromtimestamp(timestamp // 1000).strftime("%Y%m"))


def get_next_month(year_month: int) -> int:
    year: int = year_month // 100
    month: int = year_month % 100

    # Create a datetime object for the given year and month
    current_date: datetime = datetime(year, month, 1)

    # Calculate the start of the next month
    if month == 12:
        next_month: datetime = current_date.replace(year=year + 1, month=1)
    else:
        next_month: datetime = current_date.replace(month=month + 1)

    return int(next_month.strftime("%Y%m"))
