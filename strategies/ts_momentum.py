from datetime import timedelta, datetime
from pathlib import Path

import empyrical
import numpy as np
import pandas as pd
import pytz
from pandas import DataFrame
from trading_calendars import get_calendar
from zipline import run_algorithm
from zipline.api import symbol, order_target_percent

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
                 vola_window=242,
                 filter_file=None,
                 commission=None,
                 buy_sell_strategy=0) -> None:
        super().__init__(momentum_gap=momentum_gap, ranking_period=ranking_period, holding_period=holding_period,
                         filter_stocks=filter_stocks, commission=commission)
        self.vol_scale = vol_scale
        self.vola_window = vola_window
        self.all_assets = get_available_assets()
        self.buy_sell_strategy = buy_sell_strategy
        if filter_file is not None:
            self.filter_members = pd.read_csv(filter_file, parse_dates=['date'], index_col='date')

    def __str__(self):
        return """
                Time-Series Momentum
                - Ranking Period: {} months
                - Holding Period: {} months
                - Volatility window: {}
                - Rebalance technique: Volatility-weighted Scale({:.2%})
                """.format(self.ranking_period,
                           self.holding_period,
                           self.vola_window,
                           self.vol_scale)

    def rebalance(self, context, data):
        # Momentum.output_progress(context)
        # get historic data

        def vol(ts):
            logreturns = np.log(ts / ts.shift(1))
            return np.sqrt(self.vola_window * logreturns.var())

        if self.counter == 0:
            first_date, last_date, sessions, history = self.history(context, data)
            returns = history.reindex(sessions).dropna().apply(empyrical.simple_returns)
            cum_rets = empyrical.cum_returns(returns)
            returns = cum_rets.iloc[-1, :] \
                .dropna() \
                .sort_values()
            if self.buy_sell_strategy < 0:
                returns = returns.where(returns < 0)
            elif self.buy_sell_strategy > 0:
                returns = returns.where(returns > 0)
            returns = returns.dropna()
            # calculate inverse volatility to scale
            vol = history.apply(vol, axis=0)
            vol = vol.where(vol != 0).dropna()
            inverse_vol = self.vol_scale / vol
            vol_sum = inverse_vol.sum()
            weights = (inverse_vol / vol_sum).fillna(0)
            weights = weights.reindex(index=returns.index).fillna(0)

            for security, weight in weights.items():
                weight *= np.sign(returns[security])
                order_target_percent(security, weight)

        self.counter += 1

    def sell_stocks(self, context, data):
        if self.counter == self.holding_period:
            self.counter = 0

        if self.counter == 0:
            for security in context.portfolio.positions:
                if data.can_trade(security):
                    order_target_percent(security, 0)

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
        history_sessions = sessions_in_range(today - timedelta(days=400), today)
        # get historic data
        return first_date, last_date, sessions, data.history(symbols,
                                                             "close",
                                                             len(history_sessions),
                                                             "1d") \
            .dropna(axis=1)

    def file_name(self) -> str:
        if self.buy_sell_strategy > 0:
            b_s_type = 'L'
        elif self.buy_sell_strategy < 0:
            b_s_type = 'S'
        else:
            b_s_type = 'L_S'
        return 'TSMOM_{}_{}_{}.pickle'.format(b_s_type, self.ranking_period, self.holding_period)

    def to_pickle(self, perf):
        outname = self.file_name()
        outdir = Path('data/out/TSMOM')
        outdir.mkdir(parents=True, exist_ok=True)
        perf.to_pickle(outdir / outname)


if __name__ == "__main__":
    def initialize(context):
        strategy = TSMomentum(ranking_period=9,
                              holding_period=9,
                              momentum_gap=1,
                              vol_scale=0.4,
                              vola_window=242)
        strategy.initialize(context)
        context.strategy = strategy


    def rebalance(context, data):
        context.strategy.rebalance(context, data)


    def analyze(context, perf: pd.DataFrame) -> None:
        context.strategy.analyze(context, perf)


    start = datetime(2012, 1, 3, 7, 0, 0, tzinfo=pytz.timezone('Europe/Moscow'))
    end = datetime(2018, 12, 29, 7, 0, 0, tzinfo=pytz.timezone('Europe/Moscow'))
    results = run_algorithm(
        start=start,
        end=end,
        initialize=initialize,
        capital_base=1000000,
        analyze=analyze,
        bundle='database_bundle2',
        trading_calendar=get_calendar('XMOS')
    )
