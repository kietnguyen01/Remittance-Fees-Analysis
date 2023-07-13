import re
import polars as pl
from datetime import datetime, timedelta
from urllib.parse import unquote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def scrape_source(url: str, driver: webdriver, start_date: str, end_date: str) -> str:
    """Scrape token data from BitInfoCharts with headless Selenium"""
    # Wait for the chart to load and get page source
    driver.get(url)
    wait = WebDriverWait(driver, 1)
    wait.until(EC.presence_of_element_located((By.ID, 'container')))
    html = driver.page_source
    # Find the intended start and end dates using regex
    start = re.search(rf'\[new Date\("{start_date}"\)', html, re.DOTALL)
    end = re.search(rf'\[new Date\("{end_date}"\)[^\]]*', html, re.DOTALL)
    # Use a try-except block to handle the case when end is None
    try:
        # Extract the graph data in between
        graph_data = html[start.start():end.end() + 1]
    except AttributeError:
        # Use the last date found as the end index
        end_str = re.findall(r'\[new Date\("[\d/]*"\)[^\]]*\]', html, re.DOTALL)[-1]
        end = re.search(end_str, html, re.DOTALL)
        graph_data = html[start.start():end.end() + 1]
    return graph_data


def extract_vals(stats: list, driver: webdriver, start: str, end: str) -> dict:
    """Extract stat values from scraped page source into polars dataframe"""
    url_start = 'https://bitinfocharts.com/comparison/'
    url_end = '.html#alltime'
    # Dict with empty list for each stat key
    token_dict = {stat: [] for stat in stats}
    for stat in stats:
        full_url = unquote(url_start + stat + url_end)  # Decode url with special chars
        data = scrape_source(full_url, driver, start, end)
        # Extract stat values from scraped data
        pattern = r'new Date\("(.+?)"\),([\d.]+)'
        matches = re.findall(pattern, data)
        for match in matches:
            token_dict[stat].append(float(match[1]))
    return token_dict


def create_dataframe(token_dict: dict, start: str, end: str) -> pl.DataFrame:
    """Create polars dataframe with date column and token data"""
    start = datetime.strptime(start, '%Y/%m/%d').date()
    end = datetime.strptime(end, '%Y/%m/%d').date()
    # Create separate dataframe for each stat, concat them and extend the shorter columns
    df = pl.concat(
        items=[pl.DataFrame({_name: _values})
            for _name, _values in token_dict.items()],
        how="horizontal",
    )
    # Create polars dataframe with date column
    delta = end - start
    dates = [start + timedelta(days=i) for i in range(delta.days + 1)]
    df = df.with_columns(pl.Series("date", dates))
    # Reorder date column to the first position
    df = df.select(['date', *df.columns[:-1]])
    return df


def create_scrape_dict(token_stats: dict, driver: webdriver, start: str, end: str) -> dict:
    """Create a dictionary of Polars dataframes for each crypto id"""
    # Build a dataframe for each token and its stats list
    token_dfs = {}
    for token, stats in token_stats.items():
        token_dict = extract_vals(stats, driver, start, end)
        token_df = create_dataframe(token_dict, start, end)
        token_dfs[token] = token_df
    return token_dfs