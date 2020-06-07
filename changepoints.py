import matplotlib.pyplot as plt
import pandas as pd
import ruptures as rpt
from sqlalchemy import create_engine


def available_stocks():
    symbol_query = "select distinct ticker from equity_history order by ticker"
    symbols = pd.read_sql_query(symbol_query, engine)
    return symbols.ticker


if __name__ == "__main__":
    conn_str = 'postgres+psycopg2://postgres:postgres@localhost:5432/investment_strategies'

    engine = create_engine(conn_str)

    for _, symbol in available_stocks().iteritems():
        query = """select 
                            trade_date as date, close, ticker
                            from equity_history where ticker='{}' and trade_date >= '2012-01-03'
                            and trade_date <= '2018-12-31'
                            order by trade_date;
                    """.format(symbol)

        # Ask the database for the data
        df = pd.read_sql_query(query, engine, index_col='date', parse_dates=['date'])
        df = df.dropna()

        signals = df['close'].values
        if len(signals) > 0:
            model = rpt.Pelt(model='rbf').fit(signals)
            cps = model.predict(pen=100)
            print(cps)
            rpt.display(signals, cps, None)
            plt.title(symbol)
            plt.show()
        else:
            print("Empty data for: {}".format(symbol))
