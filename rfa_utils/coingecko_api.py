import polars as pl
from time import sleep
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone

cg = CoinGeckoAPI()


def unix_time(date: str) -> int:
    """Take in date in MM/DD/YYYY format and return UNIX timestamp int"""
    stamp = datetime.strptime(date, '%Y/%m/%d %H:%M:%S')
    return int(stamp.replace(tzinfo=timezone.utc).timestamp())


def api_call(name: str, currency: str, start: str, end: str) -> dict:
    """Make a call to the CoinGecko API and return the JSON response"""
    # Set start and end times to 00:00:00 and 23:59:59
    unix_start = unix_time(f'{start} 00:00:00')
    unix_end = unix_time(f'{end} 23:59:59')
    data = cg.get_coin_market_chart_range_by_id(
        id=name, 
        vs_currency=currency, 
        from_timestamp=unix_start, 
        to_timestamp=unix_end
    )
    sleep(3)
    return data


def extract_json(data: dict) -> dict:
    """"Extracts data from JSON and returns a dictionary without the timestamps"""
    data_points = {}
    timestamps_added = False  # Flag for adding timestamps to dict
    for key, value in data.items():
        if not timestamps_added:
            data_points['timestamps'] = [x[0] for x in value]
            timestamps_added = True
        # Use list comprehension to get the second element of each sublist in value
        data_points[key] = [x[1] for x in value]
    return data_points


def fill_date(df: pl.DataFrame):
    """Fill in missing date row with null values"""
    df = df.set_sorted('date')
    df = df.upsample(time_column='date', every='1d')
    return df


def create_df(name: str, currency: str, start: str, end: str) -> pl.DataFrame:
    """Create Polars dataframe from API data"""
    json = api_call(name, currency, start, end)
    data = extract_json(json)
    df = pl.DataFrame(data)
    # Apply lambda function to the timestamps column and create a date column
    date_series = df['timestamps'].apply(lambda x: datetime.fromtimestamp(x/1000).date())
    df = df.with_columns([date_series.alias('date')])
    df = df.drop('timestamps')
    # Reorder date column to the first position
    df = df.select(['date', *df.columns[:-1]])
    # Fill in any missing rows
    df = fill_date(df)
    return df


def create_api_dict(symbols: list, ids: list, currency: str, start: str, end: str) -> dict:
    """Create a dictionary of Polars dataframes for each crypto id"""
    crypto_dict = {}
    for symbol, id in zip(symbols, ids):
        df = create_df(id, currency, start, end)
        crypto_dict[symbol] = df
    return crypto_dict