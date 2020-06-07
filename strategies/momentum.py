from datetime import timedelta

from pyfolio.utils import extract_rets_pos_txn_from_zipline
from zipline.api import symbol, order_target_percent

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
                 filter_stocks=None) -> None:
        if filter_stocks is None:
            filter_stocks = ['FIVE', 'KZOSP', 'NSVZ', 'RKKE', 'TRNFP', 'TCSG', 'ENPG', 'KLSB', 'UNAC', 'TGKN',
                             'KRKNP', 'KROT', 'MSST', 'PRFN', 'DASB', 'TGKA']
        self.momentum_gap = momentum_gap
        self.ranking_period = ranking_period
        self.holding_period = holding_period
        self.losers_amount = losers_amount
        self.winners_amount = winners_amount
        self.filter_stocks = filter_stocks

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

    @staticmethod
    def rebalance(context, data):
        # Momentum.output_progress(context)

        ranking_period = context.strategy.ranking_period
        holding_period = context.strategy.holding_period
        momentum_gap = context.strategy.momentum_gap
        losers_amount = context.strategy.losers_amount
        winners_amount = context.strategy.winners_amount
        filter_stocks = context.strategy.filter_stocks
        portfolio_size = winners_amount + losers_amount

        # get trading date
        today = context.get_datetime()

        sessions = sessions_in_range(today - timedelta(days=(ranking_period + momentum_gap) * 30),
                                     today - timedelta(days=momentum_gap * 30))

        first_date = sessions[0]
        last_date = sessions[-1]

        # get available stocks
        available_stocks = set(get_available_assets(first_date=first_date, last_date=last_date))
        if filter_stocks is not None:
            available_stocks = available_stocks.difference(filter_stocks)
        # get symbol info
        symbols = [symbol(s) for s in available_stocks]
        # get historic data
        history = data.history(symbols, "close", len(sessions_in_range(first_date, today)), "1d") \
            .reindex(index=sessions) \
            .dropna(axis=1)
        # get returns
        returns = history.apply(cumulative_returns, first_date=first_date, last_date=last_date)
        returns = returns \
            .dropna() \
            .sort_values()
        # get losers and winners
        if losers_amount > 0:
            losers = returns.loc[returns < 0][:losers_amount]
        else:
            losers = pd.Series()

        if winners_amount > 0:
            winners = returns.loc[returns > 0.01][-winners_amount:]
        else:
            winners = pd.Series()

        i = 0
        for port in context.portfolios:
            if port['holding_period'] == holding_period:
                for security in context.portfolios.pop(i)['stocks'].items():
                    if data.can_trade(security[0]):
                        order_target_percent(security[0], 0)
            else:
                port['holding_period'] += 1
            i += 1

        new_portfolio = {
            'stocks': losers.append(winners),
            'holding_period': 1
        }

        context.portfolios.append(new_portfolio)

        if losers_amount > 0:
            for loser in losers.items():
                if data.can_trade(loser[0]):
                    order_target_percent(loser[0], -(1 / (len(new_portfolio['stocks']) * holding_period)))

        if winners_amount > 0:
            for winner in winners.items():
                if data.can_trade(winner[0]):
                    order_target_percent(winner[0], 1 / (len(new_portfolio['stocks']) * holding_period))

    @staticmethod
    def analyze(context, perf):
        returns, positions, transactions = extract_rets_pos_txn_from_zipline(perf)
        pf.create_full_tear_sheet(returns=returns, positions=positions, transactions=transactions)
