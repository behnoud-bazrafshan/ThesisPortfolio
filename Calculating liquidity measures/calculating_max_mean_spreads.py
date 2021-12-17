"""Find the maximum intraday spread during each month,
and calculate monthly average spreads.
"""
import time

import pandas as pd
import jdatetime

for file_number in range(1, 76):
    start = time.time()
    # Read effective spread data sets
    spread_file_path = f'C:/Users/behnood/Desktop/spread-{file_number}.csv'
    df = pd.read_csv(
        spread_file_path,
        usecols=[0, 1, 11],
        parse_dates=[1],
        header=0,
        names=[
            'ticker_num', 'date', 'percent_effective_spread'
        ]
    )
    # Convert gregorian date to persian date
    df['j_date'] = df['date'].dt.date.apply(
        lambda x: jdatetime.date.fromgregorian(date=x).strftime('%Y%m%d')
    )
    # Find the maximum intraday effective spread during each month,
    # And calculate monthly average effective spread for each month
    max_mean_df = df.groupby(df['j_date'].str[:6])[[
        'ticker_num', 'percent_effective_spread'
    ]].agg(
        tricker_num=pd.NamedAgg(
            column='ticker_num', aggfunc='max'
        ),
        max_percent_effective_spread=pd.NamedAgg(
            column='percent_effective_spread', aggfunc='max'
        ),
        mean_percent_effective_spread=pd.NamedAgg(
            column='percent_effective_spread', aggfunc='mean'
        )
    ).reset_index()
    # Write to csv
    max_mean_df.to_csv(
        f'max-mean-spread-{file_number}.csv',
        index=False
    )
    print(file_number, f'runtime: {round(time.time() - start, 3)}')
