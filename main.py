import functools
from concurrent.futures import ThreadPoolExecutor
import asyncio
from datetime import datetime

import pytz
from trading_calendars import get_calendar
from zipline import run_algorithm

from strategies.momentum import Momentum
from strategies.ts_momentum import TSMomentum


def run(strategy: Momentum) -> None:
    print('Running strategy {}'.format(strategy.file_name()))
    start = datetime(2012, 1, 3, 7, 0, 0, tzinfo=pytz.timezone('Europe/Moscow'))
    end = datetime(2018, 12, 29, 7, 0, 0, tzinfo=pytz.timezone('Europe/Moscow'))

    def initialize(context):
        strategy.initialize(context)
        context.strategy = strategy

    return run_algorithm(
        start=start,
        end=end,
        initialize=initialize,
        capital_base=1000000,
        bundle='database_bundle2',
        trading_calendar=get_calendar('XMOS')
    )


async def run_async(strategy: Momentum):
    strat = strategy

    def analyze(perf):
        strat.analyze(None, perf)
    res = await ioloop.run_in_executor(pool, run, strat)
    await ioloop.run_in_executor(pool2, analyze, res)


if __name__ == '__main__':
    pool = ThreadPoolExecutor(max_workers=50)
    pool2 = ThreadPoolExecutor(max_workers=50)
    ioloop = asyncio.get_event_loop()

    tasks = []

    for J in [1, 3, 6, 9, 12]:
        for K in [1, 3, 6, 9, 12]:
            for b_s_strategy in [-1, 0, 1]:
                tsmom = TSMomentum(
                    momentum_gap=1,
                    ranking_period=J,
                    holding_period=K,
                    vola_window=242,
                    vol_scale=0.4,
                    buy_sell_strategy=b_s_strategy
                )
                csmom = Momentum(
                    momentum_gap=1,
                    ranking_period=J,
                    holding_period=K,
                    winners_amount=20 if b_s_strategy > 0 else 0 if b_s_strategy < 0 else 10,
                    losers_amount=20 if b_s_strategy < 0 else 0 if b_s_strategy > 0 else 10
                )
                tasks.append(run_async(tsmom))
                tasks.append(run_async(csmom))

    ioloop.run_until_complete(asyncio.gather(*tasks))
    ioloop.close()
