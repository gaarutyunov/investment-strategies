# Investment strategies backtesting with [Zipline](https://github.com/quantopian/zipline)

## Getting started

### Create environment

1. Install [Conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/)
2. Create environment from strategies.yml by executing in root of project
```shell script
conda env create -f strategies.yml 
```

### Patch Zipline
1. Comment out these lines in zipline.data.loader.ensure_benchmark_data
```python
#... some code
filename = get_benchmark_filename(symbol)
    data = _load_cached_data(filename, first_date, last_date, now, 'benchmark',
                             environ)
    # if data is not None: <- Comment out this
    #     return data      <- And this
#... some code
```
2. Change zipline.data.benchmarks.get_benchmark_returns:
```python
def get_benchmark_returns(symbol):
    cal = get_calendar('NYSE')
    first_date = datetime(1930,1,1)
    last_date = datetime(2030,1,1)
    dates = cal.sessions_in_range(first_date, last_date)
    data = pd.DataFrame(0.0, index=dates, columns=['close'])
    data = data['close']
    return data.sort_index().iloc[1:]
```
3. Alter method to_ctable in site-packages/zipline/data/us_equity_pricing.py on line 412 [1572](https://github.com/quantopian/zipline/issues/1572)
```python
"""
Before
"""
    def to_ctable(self, raw_data, invalid_data_behavior):
        if isinstance(raw_data, ctable):
            # we already have a ctable so do nothing
            return raw_data

        winsorise_uint32(raw_data, invalid_data_behavior, 'volume', *OHLC)
        processed = (raw_data[list(OHLC)] * 1000).astype('uint32')
        dates = raw_data.index.values.astype('datetime64[s]')
        check_uint32_safe(dates.max().view(np.int64), 'day')
        processed['day'] = dates.astype('uint32')
        processed['volume'] = raw_data.volume.astype('uint32')
        return ctable.fromdataframe(processed)
"""
After
"""
    def to_ctable(self, raw_data, invalid_data_behavior):
        if isinstance(raw_data, ctable):
            # we already have a ctable so do nothing
            return raw_data

        processed = (raw_data[list(OHLC)] * 1000).astype('uint64')
        dates = raw_data.index.values.astype('datetime64[s]')
        processed['day'] = dates.astype('uint64')
        processed['volume'] = raw_data.volume.astype('uint64')
        return ctable.fromdataframe(processed)
```

## Data

### Fetching data

1. Clone repository [gaarutyunov/investment-strategies-data](https://github.com/gaarutyunov/investment-strategies-data)
2. Follow instructions inside the repository to fetch data

### Register bundle

1. Replace conn_str variable value by your connection string
2. Copy database_bundle.py to site-packages/zipline/data/bundles
3. Register bundle in ~./zipline/extension.py
```python
from zipline.data.bundles import register, database_bundle
register('database_bundle', database_bundle.database_bundle, calendar_name='XMOS')
```

### Ingest Bundle

1. Execute in inside project root
```shell script
zipline ingest -b database_bundle
```
