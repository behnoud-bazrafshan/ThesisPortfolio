"""Find the maximum intraday price impact during each month,
and calculate monthly average price impacts.
"""
import time

import pandas as pd
import jdatetime

for file_number in range(1, 76):
    start = time.time()
    # Read price impact data sets
    spread_file_path = f'C:/Users/behnood/Desktop/'\
        f'price_impact_{file_number}.csv'
    df = pd.read_csv(
        spread_file_path,
        usecols=[0, 1, 11],
        parse_dates=[1],
        header=0,
        names=[
            'ticker_num', 'date', 'simple_percent_price_impact'
        ]
    )
    # Convert gregorian date to persian date
    df['j_date'] = df['date'].dt.date.apply(
        lambda x: jdatetime.date.fromgregorian(date=x).strftime('%Y%m%d')
    )
    # Find the maximum intraday price impact during each month,
    # And calculate monthly average price impact for each month
    max_mean_df = df.groupby(df['j_date'].str[:6])[[
        'ticker_num', 'simple_percent_price_impact'
    ]].agg(
        tricker_num=pd.NamedAgg(
            column='ticker_num', aggfunc='max'
        ),
        max_price_impact=pd.NamedAgg(
            column='simple_percent_price_impact', aggfunc='max'
        ),
        mean_price_impact=pd.NamedAgg(
            column='simple_percent_price_impact', aggfunc='mean'
        )
    ).reset_index()
    # Write to csv
    max_mean_df.to_csv(
        f'max-mean-price-impact-{file_number}.csv',
        index=False
    )
    print(file_number, f'runtime: {round(time.time() - start, 3)}')
