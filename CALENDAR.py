import datetime as dt
from datetime import datetime
from datetime import date
import sys
import pandas as pd
import pandas_market_calendars as mcal
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, USColumbusDay


class USTradingCalendar(AbstractHolidayCalendar):
    rules = [
        USColumbusDay,
        Holiday('Veterans Day', month=11, day=11, observance=None),
    ]


def get_trading_close_holidays(year):
    inst = USTradingCalendar()

    return inst.holidays(dt.datetime(year-1, 12, 31), dt.datetime(year, 12, 31))


def get_next_day(prior_date):
    nyse = mcal.get_calendar('NYSE')
    business_days = nyse.valid_days(start_date='2017-01-01', end_date='2017-12-31')
    Fixed_Income_holidays = get_trading_close_holidays(2017)

    if Fixed_Income_holidays[0] in business_days:
        deleter = business_days.get_loc(Fixed_Income_holidays[0])
        business_days = business_days.delete(deleter)

    if Fixed_Income_holidays[1] in business_days:
        deleter = business_days.get_loc(Fixed_Income_holidays[1])
        business_days = business_days.delete(deleter)

    dater_loc = business_days.get_loc(prior_date)
    next_day = business_days[dater_loc+1]
    next_day = next_day.strftime('%Y-%m-%d')
    return next_day
