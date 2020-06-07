from datetime import timedelta, datetime

import numpy as np
import pandas as pd
import pytz
from pandas import DataFrame
from trading_calendars import get_calendar
from zipline import run_algorithm
from zipline.api import symbol, order_target_percent, schedule_function
from zipline.utils.events import date_rules, time_rules

from strategies.momentum import Momentum
from utils.get_available_assets import get_available_assets
from utils.trading_utils import sessions_in_range, cumulative_returns, volatility


class TSMomentum(Momentum):
    counter = 0
    filter_members = None

    def __init__(self,
                 momentum_gap=1,
                 ranking_period=3,
                 holding_period=3,
                 filter_stocks=None,
                 vol_scale=0.4,
                 vola_window=20,
                 filter_file=None) -> None:
        super().__init__(momentum_gap=momentum_gap, ranking_period=ranking_period, holding_period=holding_period,
                         filter_stocks=filter_stocks)
        self.vol_scale = vol_scale
        self.vola_window = vola_window
        self.all_assets = get_available_assets()
        if filter_file is not None:
            self.filter_members = pd.read_csv(filter_file, parse_dates=['date'], index_col='date')

    def rebalance(self, context, data):
        # Momentum.output_progress(context)
        # get historic data
        if self.counter == self.holding_period:
            self.counter = 0

        if self.counter == 0:
            first_date, last_date, history = self.history(context, data)
            returns = history.apply(cumulative_returns, first_date=first_date, last_date=last_date)
            returns = returns.dropna()
            # calculate inverse volatility to scale
            vol = history.apply(volatility, vola_window=self.vola_window)
            vol = vol.where(vol != 0).dropna()
            inverse_vol = self.vol_scale / vol
            vol_sum = inverse_vol.sum()
            weights = (inverse_vol / vol_sum).fillna(0)

            for security in context.portfolio.positions:
                if data.can_trade(security):
                    order_target_percent(security, 0)

            for security, weight in weights.items():
                weight *= np.sign(returns[security])
                order_target_percent(security, weight)

        self.counter += 1

    def history(self, context, data) -> (datetime, datetime, DataFrame):
        today = context.get_datetime()

        from_date = today - timedelta(days=(self.ranking_period + self.momentum_gap) * 30)
        to_date = today - timedelta(days=self.momentum_gap * 30)

        sessions = sessions_in_range(from_date, to_date)

        first_date = sessions[0]
        last_date = sessions[-1]

        # get available stocks
        available_stocks = set(get_available_assets(first_date=first_date, last_date=last_date))
        available_stocks = available_stocks.difference(self.filter_stocks)

        if self.filter_members is not None:
            all_prior = self.filter_members.loc[self.filter_members.index < last_date]
            if all_prior.empty:
                latest_day = self.filter_members.iloc[0, 0]
            else:
                latest_day = all_prior.iloc[-1, 0]
            list_of_tickers = latest_day.split(';')
            list_of_tickers = set(self.all_assets).intersection(list_of_tickers)
            if context.portfolio.positions:
                list_of_symbols = [symbol(s) for s in list_of_tickers]
                diff = [k for k in context.portfolio.positions.keys() if k not in list_of_symbols]

                for security in diff:
                    if data.can_trade(security):
                        order_target_percent(security, 0)
            available_stocks = available_stocks.intersection(list_of_tickers)
        # get symbol info
        symbols = [symbol(s) for s in available_stocks]
        # get historic data
        return first_date, last_date, data.history(symbols, "close", len(sessions_in_range(first_date, today)), "1d") \
            .reindex(index=sessions) \
            .dropna(axis=1)

    def _sell_old_stocks(self, context, data):
        i = 0
        for port in context.portfolios:
            if port['holding_period'] == self.holding_period:
                for security, _ in context.portfolios.pop(i)['stocks'].items():
                    if data.can_trade(security):
                        order_target_percent(security, 0)
            else:
                port['holding_period'] += 1
            i += 1
