import os
import xlsxwriter
import polars as pl
from typing import Any, Callable


def create_excel(path: str, data: dict or pl.DataFrame):
    """Creates an Excel file with dict of dataframes or a single dataframe"""
    with xlsxwriter.Workbook(path) as workbook:
        # Check if the data is a dict or a dataframe
        if isinstance(data, dict):
            # Loop over the dict and write each dataframe to a worksheet
            for key, df in data.items():
                workbook.add_worksheet(key)
                df.write_excel(workbook=workbook, worksheet=key)
        elif isinstance(data, pl.DataFrame):
            # Write the dataframe to the default worksheet
            data.write_excel(workbook=workbook)
        else:
            # Raise an error if the data is not a dict or a dataframe
            raise TypeError("Data must be a dictionary of Polars dataframes or a single Polars dataframe.")
        

def import_excel(path: str, names: list):
    """Create dict of dataframes from Excel sheets"""
    data = {}
    for name in names:
        data[name] = pl.read_excel(
            path, 
            sheet_name=name,
            read_csv_options={
                'infer_schema_length': 2000,
                'dtypes': {'date': pl.Date}
            }
        )
    return data


def check_and_create(file_path: str, create_dict_fn: Callable, *args: Any) -> None:
    # Check if file already exists
    if not os.path.exists(file_path):
        # Call the function to create dict of dataframes
        data_dict = create_dict_fn(*args)
        # Create Excel file with data
        create_excel(file_path, data_dict)