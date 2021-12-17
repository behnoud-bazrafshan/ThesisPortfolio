"""Parse limit order book data of sample stocks
1st stage: Create URLs that contain ticker code
and limit order book date to make requests
2nd stage: Make requests and parse limit order book data
3rd stage: Write parsed data to CSV file
"""
import concurrent.futures
import threading
import time
import re
import io

import requests
import pandas as pd
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

thread_local = threading.local()


def get_session():
    """Reuse a connection rather than establishing a new one."""
    if not hasattr(thread_local, 'session'):
        thread_local.session = requests.Session()
    return thread_local.session


def scrape_data(urls: list):
    """Use Multithreading for scraping."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(parse_lob_data, urls)


def parse_lob_data(url: str) -> list:
    """Parse and extract limit order book data
    and append it to the lob_list.
    """
    ticker_code = re.search(r"/BestLimits/(.*)/(.*)", url)[1]
    lob_date = re.search(r"/BestLimits/(.*)/(.*)", url)[2]
    script = make_requests(url)
    json_data = script['bestLimitsHistory']
    for item in json_data:
        lob_list.append([
            ticker_code, lob_date,
            item['hEven'], item['number'],
            item['qTitMeDem'], item['zOrdMeDem'],
            item['pMeDem'], item['pMeOf'],
            item['zOrdMeOf'], item['qTitMeOf']
        ])


def make_requests(url: str) -> str:
    """Manage requests and pass reponses to parse_lob_data function."""
    session = get_session()
    retries = Retry(
        total=5,
        backoff_factor=10,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    try:
        response = session.get(url)
    except requests.exceptions.ConnectionError as errc:
        print(errc)
    return response.json()


def get_lob_urls(ticker_id: str, start_date: int, end_date: int) -> list:
    """Create urls' list from the ticker ID."""
    url = f'http://www.tsetmc.com/tsev2/data/'\
        f'Export-txt.aspx?t=i&a=1&b=0&i={ticker_id}'
    response = requests.get(url).content
    ticker = pd.read_csv(
        io.StringIO(response.decode('utf-8')),
        usecols=[1],
        header=0,
        names=['date']
    )
    ticker_dates = [
        x for x in ticker.date.tolist() if end_date >= x >= start_date
    ]
    urls_list = [
        f'http://cdn.tsetmc.com/api/BestLimits/'
        f'{ticker_id}/{date}' for date
        in ticker_dates
    ]
    return urls_list


def write_to_csv(file_number: int):
    """Wrtite outputs in the csv file by only entering the number.

    titles = [
        ticker code, date, time, order place, buyer numbers,
        bid volume, bid price, ask price, ask volume, seller numbers
    ]
    """
    lob_df = pd.DataFrame(
        lob_list,
        columns=[
            'ticker_code', 'lob_date', 'ticker_time', 'order_place',
            'buyer_numbers', 'bid_vol', 'bid', 'ask',
            'ask_vol', 'seller_numbers'
        ]
    )
    lob_df.to_csv(f'order-{file_number}.csv', index=False)


# Create a dictionary of sample ticker IDs
sample_df = pd.read_excel(
    r'D:\University\Master\Thesis\TSETMC\Sampling\final_sample.xlsx',
    usecols=[0, 3]
)
sample_dict = dict(zip(sample_df.number, sample_df.ticker_id))

# Scrape each stocks' limit order book data and write them to a CSV
for sample_number in range(1, 76):
    start = time.time()
    all_lob_urls = get_lob_urls(
        ticker_id=str(sample_dict[sample_number]),
        start_date=20150221,
        end_date=20200318
    )
    lob_list = []
    scrape_data(all_lob_urls)
    write_to_csv(sample_number)
    print(
        f'ticker number: {sample_number}, '
        f'runtime: {round(time.time() - start, 3)}'
    )
