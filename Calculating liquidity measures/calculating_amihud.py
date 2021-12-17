"""Calculating amihud illiquidity measure for each ticker"""
import pandas as pd

for i in range(1, 76):
    file_number = i
    amihud_headers = {
        '<TICKER>': 'ticker_name',
        '<CLOSE>': 'adj_close',
        '<VALUE>': 'D_volume'
    }
    # Read daily data, set date column as index
    amihud_df = pd.read_excel(
        f'C:/Users/behnood/Desktop/Thesis/TSETMC/Dataset/'
        f'New Sampling/Daily Data - TseClient (only traded days)/'
        f'{file_number}.xls',
        usecols=[0, 5, 7, 8],
        dtype={'<DATE>': str},
        parse_dates=[3],
        index_col=3
    )
    # Change headers name
    amihud_df.rename(columns=amihud_headers, inplace=True)
    # Alter ticker_name from فارس-ت to فارس
    amihud_df['ticker_name'] = amihud_df['ticker_name'].str.extract(r'(.+)\-')
    # Change index name
    amihud_df.index.names = ['date']
    # Calculate daily return
    amihud_df['return'] = amihud_df['adj_close'].pct_change()
    # Make data fram upside down
    amihud_df = amihud_df.iloc[::-1]
    # Select desired timespan
    amihud_df = amihud_df['2020-03-19':'2015-02-21']
    # Calculate amihud measure
    amihud_df['amihud'] = (amihud_df['return'].abs() / amihud_df['D_volume'])
    # Write to csv
    amihud_df.to_csv(
        f'amihud-{file_number}.csv',
        encoding='utf-8-sig',
    )
