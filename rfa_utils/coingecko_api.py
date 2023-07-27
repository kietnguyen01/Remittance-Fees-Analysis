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


def create_new_row(date: datetime.date) -> pl.DataFrame:
    """Create a new row with the given date and null values for the other columns"""
    new_row = pl.DataFrame({
        'date': [date],
        'prices': [None],
        'market_caps': [None],
        'total_volumes': [None]
    })
    # Cast all columns except date to f64
    new_row = new_row.select([pl.col('date'), *[pl.col(c).cast(pl.Float64) for c in new_row.columns[1:]]])
    return new_row


def prepend_new_row(df: pl.DataFrame, start: str) -> pl.DataFrame:
    """Prepend a new row with the desired start date if the first date is not equal to the desired start date"""
    # Get the date series from the dataframe
    date_series = df['date']
    # Check if first date is equal to desired start date
    desired_date = datetime.strptime(start, '%Y/%m/%d').date()
    if date_series[0] != desired_date:
        # Create a new row with desired date
        new_row = create_new_row(desired_date)
        # Prepend new row to dataframe
        df = pl.concat([new_row, df])
    return df


def fill_date(df: pl.DataFrame):
    """Fill in missing date row with null values"""
    df = df.set_sorted('date')
    df = df.upsample(time_column='date', every='1d')
    return df


def replace_zeros_with_nulls(df: pl.DataFrame, column: str) -> pl.DataFrame:
    """Replace any zeros in the given column with null values"""
    # Get the series from the dataframe
    series = df[column]
    # Apply a lambda function to replace zeros with nulls
    series = series.apply(lambda x: None if x == 0 else x)
    # Replace the column in the dataframe with the modified series
    df = df.with_columns(series.alias(column))
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
    # Prepend a new row if needed
    df = prepend_new_row(df, start)
    # Fill in any missing rows
    df = fill_date(df)
    # Replace any zeros with nulls in every column except date
    for column in df.columns[1:]:
        df = replace_zeros_with_nulls(df, column)
    return df


def create_api_dict(symbols: list, ids: list, currency: str, start: str, end: str) -> dict:
    """Create a dictionary of Polars dataframes for each crypto id"""
    crypto_dict = {}
    for symbol, id in zip(symbols, ids):
        df = create_df(id, currency, start, end)
        crypto_dict[symbol] = df
    return crypto_dict