import os
import requests
import polars as pl
from dotenv import load_dotenv
from datetime import datetime, timezone
from rfa_utils.coingecko_api import fill_date

# Load environment variables from .env file
load_dotenv(".env")


def unix_time(date: str) -> float:
    """Take in date in string format and return UNIX timestamp float"""
    stamp = datetime.strptime(date, '%Y/%m/%d %H:%M:%S')
    return int(stamp.replace(tzinfo=timezone.utc).timestamp())


def dates_between(start: str, end: str) -> list:
    """Return a list of first dates of each year, given a start and end"""
    start_date = datetime.strptime(start, '%Y/%m/%d')
    end_date = datetime.strptime(end, '%Y/%m/%d')
    date_list = []

    # Check if the start date is the first day of the year
    if start_date.month == 1 and start_date.day == 1:
        current_date = start_date
    else:
        # If not, use the first day of current year
        current_date = start_date.replace(month=1, day=1)

    while end_date > current_date:
        # Add current date to the list of dates and replace the month and day to 1
        date_list.append(current_date)
        current_date = current_date.replace(month=1, day=1)
        # Add one year from the current date and replace the month and day to 1
        current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
        
    # Add first day of next year for end date
    date_list.append(end_date.replace(year=end_date.year + 1, month=1, day=1))

    # Return list of dates as strings
    return [date.strftime('%Y/%m/%d') + ' 00:00:00' for date in date_list]


def api_call(dates_list: list) -> dict:
    """Make a call to the Owlracle API and return historical gas JSON"""
    key = os.getenv('OAPI')
    results = {}    # Dict of JSON

    # Loop over the list of dates in pairs using zip
    for start_date, end_date in zip(dates_list, dates_list[1:]):
        unix_start = unix_time(start_date)
        unix_end = unix_time(end_date)
    
        # Make the API call with the start and end dates
        res = requests.get(f'https://api.owlracle.info/v4/eth/history?apikey={key}&from={unix_start}&to={unix_end}&candles=365&timeframe=1d&txfee=true')
        # Add the response JSON to the results dictionary with the year as the key
        year = datetime.strptime(start_date, '%Y/%m/%d %H:%M:%S').year
        results[year] = res.json()
        
    return results


def extract_json(json_data: dict) -> dict:
    """Extract the txFee and the gasPrice from the JSON data, calculate the average
    Return a dict containing three lists: dates, transaction_fees, and gas_prices"""
    result = {}
    dates = []
    transaction_fees = []
    gas_prices = []
    for year, data in json_data.items():
        for candle in data['candles']:
            # Extract closing txFee and gasPrice from candles
            fee = candle['txFee']['close']
            price = candle['gasPrice']['close']
            
            # Only add in data from the current year
            date = candle['timestamp'].split('T')[0]
            if date.startswith(str(year)):
                dates.append(date)
                transaction_fees.append(fee)
                gas_prices.append(price)
        
    # Add the sorted dates, fees, and prices lists to the result dict
    result["date"] = sorted([datetime.strptime(date, '%Y-%m-%d').date() for date in dates])
    result["transaction_fees"] = [x for _, x in sorted(zip(dates, transaction_fees))]
    result["gas_prices"] = [x for _, x in sorted(zip(dates, gas_prices))]
    
    return result


def create_df(start: str, end: str) -> pl.DataFrame:
    """Create a Polars DataFrame from the Owlracle API data between the given start and end dates"""
    between = dates_between(start, end)
    json = api_call(between)
    extracted = extract_json(json)
    df = pl.DataFrame(extracted)
    return fill_date(df)