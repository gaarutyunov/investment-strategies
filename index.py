import pandas
from sqlalchemy import create_engine

conn_str = 'postgres+psycopg2://postgres:postgres@localhost:5432/investment_strategies'

engine = create_engine(conn_str)

if __name__ == "__main__":
    cols = {
        'Date': 'time',
        'Price': 'close',
        'Open': 'open',
        'Vol.': 'volume',
        'Low': 'low',
        'High': 'high'
    }
    df = pandas.read_csv('data/micex.csv', parse_dates=['Date'], usecols=cols.keys())
    df = df.rename(columns=cols)
    df = df.set_index('time')
    df['close'] = df['close'].str.replace(',', '')
    df['open'] = df['open'].str.replace(',', '')
    df['low'] = df['low'].str.replace(',', '')
    df['high'] = df['high'].str.replace(',', '')

    df['close'] = pandas.to_numeric(df['close'])
    df['open'] = df['open'].astype(float)
    df['low'] = df['low'].astype(float)
    df['high'] = df['high'].astype(float)
    df['instrument_id'] = 260
    df['volume'] = 0
    df.to_sql('candles', con=engine, if_exists='append')
