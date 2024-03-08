from datetime import datetime


def get_month(timestamp):
    # Timestamp is the number of milleseconds since the epoch
    return int(datetime.utcfromtimestamp(timestamp // 1000).strftime("%Y%m"))


def get_next_month(year_month):
    # year_month is 6 digit integer in the format YYYYMM
    year = year_month // 100
    month = year_month % 100

    current_date = datetime(year, month, 1)

    # Calculate the start of the next month
    if month == 12:
        next_month = current_date.replace(year=year + 1, month=1)
    else:
        next_month = current_date.replace(month=month + 1)

    return int(next_month.strftime("%Y%m"))
