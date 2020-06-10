import time
from datetime import timedelta, datetime
from pathlib import Path

import empyrical
import pytz
from pyfolio.utils import extract_rets_pos_txn_from_zipline
from trading_calendars import get_calendar
from zipline import run_algorithm
from zipline.api import symbol, order_target_percent, schedule_function, set_commission, set_slippage, set_benchmark
from zipline.finance.slippage import FixedSlippage
from zipline.utils.events import date_rules, time_rules

from utils.get_available_assets import get_available_assets
from utils.trading_utils import sessions_in_range, cumulative_returns
import pandas as pd
import pyfolio as pf


class Momentum:
    def __init__(self,
                 momentum_gap=1,
                 ranking_period=3,
                 holding_period=3,
                 losers_amount=10,
                 winners_amount=10,
                 filter_stocks=None,
                 commission=None) -> None:
        if filter_stocks is None:
            filter_stocks = ['FIVE', 'KZOSP', 'NSVZ', 'RKKE', 'TRNFP',
                             'TCSG', 'ENPG', 'KLSB', 'UNAC', 'TGKN',
                             'KRKNP', 'KROT', 'MSST', 'PRFN', 'DASB',
                             'TGKA', 'BANE']
        self.momentum_gap = momentum_gap
        self.ranking_period = ranking_period
        self.holding_period = holding_period
        self.losers_amount = losers_amount
        self.winners_amount = winners_amount
        self.filter_stocks = filter_stocks
        self.commission = commission
        self.start = None

    def __str__(self):
        return """
                Cross-Sectional Momentum
                - Ranking Period: {} months
                - Holding Period: {} months
                - Assets in losers portfolio: {}
                - Assets in winners portfolio: {}
                - Rebalance technique: Equally-weighted
                """.format(self.ranking_period, self.holding_period, self.losers_amount, self.winners_amount)

    def initialize(self, context):
        set_benchmark(symbol('MICEX'))
        context.portfolios = list()
        schedule_function(self.rebalance, date_rule=date_rules.month_start(), time_rule=time_rules.market_close())
        schedule_function(self.sell_stocks, date_rule=date_rules.month_end(1), time_rule=time_rules.market_close())
        if self.commission:
            set_commission(self.commission)

        set_slippage(FixedSlippage(spread=0.0))

        self.start = time.time()
        print('Initialized strategy {} at {:1.1f} seconds'.format(self.file_name(), time.time() - self.start))

    @staticmethod
    def output_progress(context):
        if hasattr(context, 'last_month'):
            # Get today's date
            today = context.get_datetime().date()

            # Calculate percent difference since last month
            perf_pct = (context.portfolio.portfolio_value / context.last_month) - 1

            # Print performance, format as percent with two decimals.
            print("{} - Last Month Result: {:.2%}".format(today, perf_pct))

        # Remember today's portfolio value for next month's calculation
        context.last_month = context.portfolio.portfolio_value

    def rebalance(self, context, data):
        # Momentum.output_progress(context)

        # get trading date
        today = context.get_datetime()

        sessions = sessions_in_range(today - timedelta(days=(self.ranking_period + self.momentum_gap) * 20),
                                     today - timedelta(days=self.momentum_gap * 20))

        first_date = sessions[0]
        last_date = sessions[-1]

        # get available stocks
        available_stocks = set(get_available_assets(first_date=first_date, last_date=last_date))
        if self.filter_stocks is not None:
            available_stocks = available_stocks.difference(self.filter_stocks)
        # get symbol info
        symbols = [symbol(s) for s in available_stocks]
        # get historic data
        history = data.history(symbols, "close", len(sessions_in_range(first_date, today)), "1d") \
            .reindex(index=sessions) \
            .dropna(axis=1)
        # get returns
        returns = history.reindex(sessions).dropna().apply(empyrical.simple_returns)
        cum_rets = empyrical.cum_returns(returns)
        returns = cum_rets.iloc[-1, :] \
            .dropna() \
            .sort_values()
        # get losers and winners
        if self.losers_amount > 0:
            losers = returns.loc[returns < 0][:self.losers_amount]
        else:
            losers = pd.Series()

        if self.winners_amount > 0:
            winners = returns.loc[returns > 0.01][-self.winners_amount:]
        else:
            winners = pd.Series()

        new_portfolio = {
            'stocks': losers.append(winners),
            'holding_period': 1
        }

        context.portfolios.append(new_portfolio)

        if self.losers_amount > 0:
            for loser in losers.items():
                if data.can_trade(loser[0]):
                    order_target_percent(loser[0], -1 / self.losers_amount / self.holding_period)

        if self.winners_amount > 0:
            for winner in winners.items():
                if data.can_trade(winner[0]):
                    order_target_percent(winner[0], 1 / self.winners_amount / self.holding_period)

    def sell_stocks(self, context, data):
        i = 0
        for port in context.portfolios:
            if port['holding_period'] == self.holding_period:
                for security in context.portfolios.pop(i)['stocks'].items():
                    if data.can_trade(security[0]):
                        order_target_percent(security[0], 0)
            else:
                port['holding_period'] += 1
            i += 1

    def analyze(self, context, perf):
        # returns, positions, transactions = extract_rets_pos_txn_from_zipline(perf)
        # pf.create_full_tear_sheet(returns=returns,
        #                           positions=positions,
        #                           transactions=transactions)
        self.to_pickle(perf)
        print('Finnished strategy {} at {:1.1f} seconds'.format(self.file_name(), time.time() - self.start))

    def to_pickle(self, perf):
        outname = self.file_name()
        outdir = Path('data/out/CSMOM')
        outdir.mkdir(parents=True, exist_ok=True)
        perf.to_pickle(outdir / outname)

    def file_name(self) -> str:
        if self.winners_amount > self.losers_amount:
            b_s_type = 'L'
        elif self.losers_amount > self.winners_amount:
            b_s_type = 'S'
        else:
            b_s_type = 'L_S'
        return 'CSMOM_{}_{}_{}.pickle'.format(b_s_type, self.ranking_period, self.holding_period)


if __name__ == "__main__":
    def initialize(context):
        strategy = Momentum(ranking_period=3,
                            holding_period=3,
                            momentum_gap=1,
                            losers_amount=15,
                            winners_amount=15)
        strategy.initialize(context)
        context.strategy = strategy


    def rebalance(context, data):
        context.strategy.rebalance(context, data)


    def analyze(context, perf: pd.DataFrame) -> None:
        context.strategy.analyze(context, perf)


    start = datetime(2012, 1, 3, 7, 0, 0, tzinfo=pytz.timezone('Europe/Moscow'))
    end = datetime(2018, 12, 31, 7, 0, 0, tzinfo=pytz.timezone('Europe/Moscow'))
    results = run_algorithm(
        start=start,
        end=end,
        initialize=initialize,
        capital_base=100000000,
        analyze=analyze,
        bundle='database_bundle2',
        trading_calendar=get_calendar('XMOS')
    )
