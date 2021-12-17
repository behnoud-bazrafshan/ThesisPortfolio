"""Calculating price impact for each trade."""
from datetime import timedelta
import time

import pandas as pd
import numpy as np

pd.options.mode.chained_assignment = None


def lee_ready(row):
    """Calculate trades directions with LR algorithm"""
    if row['trade_price'] > row['mid_quote']:
        value = 1
    elif row['trade_price'] < row['mid_quote']:
        value = -1
    elif row['trade_price'] == row['mid_quote']:
        index = pi_df.index.get_loc(row.name)
        if index == 0:
            return None
        # Get last values of trade_price column in reversed order
        previous_data = pi_df['trade_price'][0:index].values[::-1]
        # Get first different value
        last_different_value = previous_data[
            (previous_data != row['trade_price']).argmax()
        ]
        if row['trade_price'] > last_different_value:
            value = 1
        elif row['trade_price'] < last_different_value:
            value = -1
        else:
            value = None
    return value


def get_mid_quotes_plus_five(df):
    """Find mid quote five minute after"""
    output_list = []
    for idx, day in df.groupby(df.index.date):
        for n in range(len(day.index)):
            plus_five = (day.index[n] + timedelta(minutes=5))
            try:
                indicator = day.index.get_loc(plus_five, method='bfill')
                mid_plus_five = day.iloc[indicator, 0]
                output_list.append(mid_plus_five)
            except KeyError:
                mid_plus_five = np.nan
                output_list.append(mid_plus_five)
    return output_list


for file_number in range(74, 75):
    start = time.time()
    # Import limit order book
    quotes_headers = [
        'lob_date', 'lob_time', 'order_place',
        'buyer_numbers', 'bid_vol', 'bid', 'ask',
        'ask_vol', 'seller_numbers'
    ]
    quotes_path = f'C:/Users/behnood/Desktop/order-{file_number}.csv'
    quotes = pd.read_csv(
        quotes_path,
        usecols=[1, 2, 3, 4, 5, 6, 7, 8, 9],
        header=0,
        names=quotes_headers
    )
    # Keep only quotes
    quotes = quotes.loc[quotes.order_place == 1]
    # Keep only open market time
    quotes = quotes.loc[
        (quotes.lob_time <= 123000) & (quotes.lob_time >= 90000)
    ]
    # Convert to datetime
    quotes['date'] = (
        quotes['lob_date'].astype(str)
        + quotes['lob_time'].astype(str)
    )
    quotes['date'] = pd.to_datetime(quotes.date, format='%Y%m%d%H%M%S')
    quotes.drop(columns=['lob_date', 'lob_time'], inplace=True)
    # Import trades
    trades_headers = [
        'trade_date', 'trade_number', 'trade_time',
        'trade_vol', 'trade_price', 'trade_validity'
    ]
    trades_path = f'C:/Users/behnood/Desktop/trade-{file_number}.csv'
    trades = pd.read_csv(
        trades_path,
        usecols=[1, 2, 3, 4, 5, 6],
        header=0,
        names=trades_headers
    )
    # Keep only open market time
    trades = trades.loc[
        (trades.trade_time <= 123000) & (trades.trade_time >= 90000)
    ]
    # Convert to datetime
    trades['date'] = (
        trades['trade_date'].astype(str)
        + trades['trade_time'].astype(str)
    )
    trades['date'] = pd.to_datetime(trades.date, format='%Y%m%d%H%M%S')
    trades.drop(columns=['trade_date', 'trade_time'], inplace=True)
    # Delete invalid trades
    trades = trades[trades.trade_validity != 1]
    trades.drop(columns=['trade_validity'], inplace=True)
    # Drop duplicates
    quotes.drop_duplicates(inplace=True)
    trades.drop_duplicates(inplace=True)
    # Drop unnecessary columns and change date cloumn position
    quotes = quotes[['date', 'bid', 'ask']]
    # Consolidate all trades at a same time and price
    # Into a single trade (by adding trade volumes)
    grouped = trades.groupby(['date', 'trade_price'], as_index=False)
    agg_vol = grouped[['trade_vol']].sum()
    first_appearance = grouped[['trade_number']].first()
    trades = pd.merge(agg_vol, first_appearance)
    # Consolidate all trades at a same time and different price-
    # -into a single trade (by calculating volume weighted average price)
    vwa_prices = trades.groupby('date').apply(
        lambda x: np.average(x['trade_price'], weights=x['trade_vol'])
    ).to_frame('trade_price').reset_index()
    first_appearance = trades.groupby('date', as_index=False)[[
        'trade_number'
    ]].first()
    trades = pd.merge(first_appearance, vwa_prices)
    # Merge two data frames
    pi_df = pd.merge(trades, quotes, how='outer', on='date')
    # Drop trades in saf (also quotes)
    pi_df = pi_df.loc[(pi_df['bid'] != 0) & (pi_df['ask'] != 0)]
    # Seperate date and time since we want to-
    # -sort dates descending and time ascending
    pi_df['date_only'] = pi_df['date'].dt.date
    pi_df['time_only'] = pi_df['date'].dt.time
    # Sort data frame by 1)date, 2)time, and 3)trade number
    pi_df.sort_values(
        by=['date_only', 'time_only', 'trade_number'],
        ascending=[False, True, True],
        inplace=True,
        ignore_index=True
    )
    # Check if the fist bid value is empty or not
    if pd.isna(pi_df.iloc[0, 3]):
        # find the first index number of not-NaN value in bid column
        first_notna_quote = pi_df['bid'].first_valid_index()
        # fill first empty values with the fist not-NaN value
        pi_df[['bid', 'ask']] = pi_df[[
            'bid', 'ask'
        ]].fillna(method='bfill', limit=first_notna_quote)
    # Drop trades that have more than one quotes
    subset = ['date', 'date_only', 'time_only', 'trade_number']
    pi_df.drop_duplicates(subset=subset, keep='first', inplace=True)
    # Fix trades with no quotes by filling empty values with previous one
    pi_df[['bid', 'ask']] = pi_df[[
        'bid', 'ask'
    ]].fillna(method='ffill')
    # Drop nan in trades
    pi_df.dropna(subset=['trade_price'], axis=0, inplace=True)
    pi_df.reset_index(drop=True, inplace=True)
    pi_df['trade_number'] = pi_df['trade_number'].astype(int)
    pi_df.sort_values(
        by=['date_only', 'time_only', 'trade_number'],
        ascending=[False, True, True],
        inplace=True,
        ignore_index=True
    )
    # Delete first trades
    grouped = pi_df.groupby(pi_df.date.dt.date)
    first_trades = grouped[['date']].first()['date'].tolist()
    pi_df.drop(
        pi_df.loc[pi_df['date'].isin(first_trades)].index,
        inplace=True
    )
    # Drop trades with equal bid and ask
    pi_df = pi_df.loc[pi_df['bid'] != pi_df['ask']]
    # Calculate mid quote
    pi_df['mid_quote'] = (pi_df['ask'] + pi_df['bid']) / 2
    # Reduce dataframe memory usage
    pi_df[
        ['trade_number', 'bid', 'ask']
    ] = pi_df[
        ['trade_number', 'bid', 'ask']
    ].apply(pd.to_numeric, downcast="float")
    pi_df.reset_index(drop=True, inplace=True)
    # Discern directions
    small_df = pi_df[['trade_price', 'bid', 'ask', 'mid_quote']]
    col = small_df.apply(lee_ready, axis=1)
    lr_df = pi_df.assign(trade_direction=col.values)
    # Reduce dataframe memory usage
    lr_df['trade_direction'] = pd.to_numeric(
        lr_df['trade_direction'],
        downcast='integer'
    )
    # Change date sort for groupby
    lr_df.sort_values(
        by=['date', 'trade_number'],
        ascending=[True, True],
        inplace=True,
        ignore_index=True
    )
    lr_df.drop(['bid', 'ask'], axis=1, inplace=True)
    lr_df.set_index('date', inplace=True)
    # Find mid quotes after five minutes
    small_df_2 = lr_df[['mid_quote']]
    mid_plus_five_lst = get_mid_quotes_plus_five(small_df_2)
    # Add mid quotes after five minute to data frame
    lr_df.insert(5, 'mid_plus_five', mid_plus_five_lst)
    # Sort back dates again
    lr_df.sort_values(
        by=['date_only', 'time_only', 'trade_number'],
        ascending=[False, True, True],
        inplace=True
    )
    # Drop date_only and time_only columns
    lr_df.drop(['date_only', 'time_only'], axis=1, inplace=True)
    # Remove trades that don't have price impact
    lr_df.dropna(subset=['mid_plus_five'], axis=0, inplace=True)
    # Calculate simple percent price impact
    lr_df['simple_percent_price_impact'] = (
        2
        * lr_df['trade_direction']
        * (
            (lr_df['mid_plus_five'] - lr_df['mid_quote'])
            / lr_df['mid_quote']
        )
    )
    # Add file number to df
    lr_df.insert(0, 'ticker_num', file_number)
    # Write to csv
    lr_df.to_csv(f'price_impact_{file_number}.csv')
    print(
        f'ticker number: {file_number}, '
        f'runtime: {round(time.time() - start, 3)}'
    )
