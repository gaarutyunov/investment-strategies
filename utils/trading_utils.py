from datetime import datetime

import pandas as pd
from trading_calendars import get_calendar


def volatility(ts, vola_window):
    return ts.pct_change().rolling(vola_window).std().iloc[-1]


def cumulative_returns(ts: pd.Series, first_date: datetime, last_date: datetime) -> pd.Series:
    new_ts = pd.Series([ts[first_date], ts[last_date]])
    r = new_ts.pct_change()
    r = r[1]
    return r


def sessions_in_range(first_date: datetime, last_date: datetime) -> pd.DatetimeIndex:
    return get_calendar('XMOS').sessions_in_range(first_date, last_date)
