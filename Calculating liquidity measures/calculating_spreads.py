"""Calculating effective spread for each trade."""
import time

import pandas as pd
import numpy as np

pd.options.mode.chained_assignment = None


def lee_ready(row):
    """Calculate trades directions with LR algorithm."""
    if row['trade_price'] > row['mid_quote']:
        value = 1
    elif row['trade_price'] < row['mid_quote']:
        value = -1
    elif row['trade_price'] == row['mid_quote']:
        index = spread_df.index.get_loc(row.name)
        if index == 0:
            return None
        # Get values of trade_price column in a reversed order
        previous_data = spread_df['trade_price'][0:index].values[::-1]
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


for file_number in range(1, 76):
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
    spread_df = pd.merge(trades, quotes, how='outer', on='date')
    # Seperate date and time since we want to
    # sort dates descending and time ascending
    spread_df['date_only'] = spread_df['date'].dt.date
    spread_df['time_only'] = spread_df['date'].dt.time
    # Drop trades in saf (also quotes)
    spread_df = spread_df.loc[
        (spread_df['bid'] != 0) & (spread_df['ask'] != 0)
    ]
    # Sort data frame by 1)date, 2)time, and 3)trade number
    spread_df.sort_values(
        by=['date_only', 'time_only', 'trade_number'],
        ascending=[False, True, True],
        inplace=True,
        ignore_index=True
    )
    # Check if the fist bid value is empty ot not
    if pd.isna(spread_df.iloc[0, 3]):
        # find the first index number of not-NaN value in bid column
        first_notna_quote = spread_df['bid'].first_valid_index()
        # fill first empty values with the fist not-NaN value
        spread_df[['bid', 'ask']] = spread_df[[
            'bid', 'ask'
        ]].fillna(method='bfill', limit=first_notna_quote)
    # Drop trades that have more than one quotes
    subset = ['date', 'date_only', 'time_only', 'trade_number']
    spread_df.drop_duplicates(subset=subset, keep='first', inplace=True)
    # Fix trades with no quotes by filling empty values with previous one
    spread_df[['bid', 'ask']] = spread_df[[
        'bid', 'ask'
    ]].fillna(method='ffill')
    # Drop nan in trades
    spread_df.dropna(subset=['trade_price'], axis=0, inplace=True)
    spread_df.reset_index(drop=True, inplace=True)
    spread_df['trade_number'] = spread_df['trade_number'].astype(int)
    spread_df.sort_values(
        by=['date_only', 'time_only', 'trade_number'],
        ascending=[False, True, True],
        inplace=True,
        ignore_index=True
    )
    # Drop date_only and time_only columns
    spread_df.drop(['date_only', 'time_only'], axis=1, inplace=True)
    # Delete first trades
    grouped = spread_df.groupby(spread_df.date.dt.date)
    first_trades = grouped[['date']].first()['date'].tolist()
    spread_df.drop(
        spread_df.loc[spread_df['date'].isin(first_trades)].index,
        inplace=True
    )
    # Drop trades with equal bid and ask
    spread_df = spread_df.loc[spread_df['bid'] != spread_df['ask']]
    # Calculate mid quote
    spread_df['mid_quote'] = (spread_df['ask'] + spread_df['bid']) / 2
    # Calculate Quoted Spread
    spread_df['quoted_spread'] = spread_df['ask'] - spread_df['bid']
    # Calculate Simple Percent Quoted Spread
    spread_df['percent_quoted_spread'] = (
        spread_df['quoted_spread'] / spread_df['mid_quote']
    )
    # Locate trade position
    conditions = [
        (spread_df.trade_price == spread_df.bid)
        | (spread_df.trade_price == spread_df.ask),
        (spread_df.trade_price < spread_df.bid)
        | (spread_df.trade_price > spread_df.ask),
        (spread_df.trade_price > spread_df.bid)
        & (spread_df.trade_price < spread_df.ask),
        spread_df.ask.isnull() & spread_df.bid.isnull()
    ]
    choices = ['at quotes', 'outside quotes', 'inside quotes', 'no quotes']
    spread_df['trade_position'] = np.select(conditions, choices, default=None)
    # Reduce dataframe memory usage
    spread_df[
        ['trade_number', 'trade_price', 'bid', 'ask']
    ] = spread_df[
        ['trade_number', 'trade_price', 'bid', 'ask']
    ].apply(pd.to_numeric, downcast='float')
    spread_df['quoted_spread'] = pd.to_numeric(
        spread_df['quoted_spread'],
        downcast='float'
    )
    # Discern directions with lee_ready
    col = spread_df.apply(lee_ready, axis=1)
    lr_df = spread_df.assign(trade_direction=col.values)
    # Calculate simple percent effective spread for LR directions
    lr_df['percent_effective_spread'] = (
        (
            2
            * lr_df['trade_direction']
            * (lr_df['trade_price'] - lr_df['mid_quote'])
        )
        / lr_df['mid_quote']
    )
    # Add tickers' names to data frame
    lr_df.insert(0, 'ticker_num', file_number)
    # Write to csv
    lr_df.to_csv(
        f'spread-{file_number}.csv', index=False
    )
    print(
        f'ticker number: {file_number}, '
        f'runtime: {round(time.time() - start, 3)}'
    )
