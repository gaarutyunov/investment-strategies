from datetime import datetime

from sqlalchemy import create_engine
import pandas as pd

conn_str = 'postgres+psycopg2://postgres:postgres@localhost:5432/investment_strategies'

engine = create_engine(conn_str)


def get_available_assets(first_date: datetime = None, last_date: datetime = None) -> pd.Series:
    if first_date is None and last_date is None:
        symbol_query = "SELECT DISTINCT ticker FROM equity_history"
    else:
        symbol_query = "SELECT DISTINCT ticker FROM equity_history WHERE trade_date >= '{}' and trade_date <= '{}'"\
            .format(first_date, last_date)
    symbols = pd.read_sql_query(symbol_query, engine)
    return symbols.ticker


