"""Find the maximum amihud illiquidity measure during each month,
and calculate monthly average amihud illiquidity measure.
"""
import time

import pandas as pd
import jdatetime

for file_number in range(1, 76):
    start = time.time()
    # Read amihud data sets
    file_path = f'E:/Thesis/New Sampling/Amihud/amihud-{file_number}.csv'
    df = pd.read_csv(
        file_path,
        usecols=[0, 5],
        parse_dates=[0],
        header=0,
        names=['date', 'amihud']
        )
    df.insert(0, 'ticker_num', file_number)
    # Scaling by 10^6
    df['amihud'] = df['amihud'] * 1000000
    # Convert gregorian date to persian date
    df['j_date'] = df['date'].dt.date.apply(
        lambda x: jdatetime.date.fromgregorian(date=x).strftime('%Y%m%d')
    )
    # Find the maximum amihud illiquidity measure during each month,
    # And calculate monthly average amihud illiquidity measure.
    max_df = df.groupby(df['j_date'].str[:6])[[
        'ticker_num', 'amihud'
    ]].agg(
        tricker_num=pd.NamedAgg(
            column='ticker_num', aggfunc='max'
        ),
        max_amihud=pd.NamedAgg(
            column='amihud', aggfunc='max'
        ),
        mean_amihud=pd.NamedAgg(
            column='amihud', aggfunc='mean'
        )
    ).reset_index()
    # Write to csv
    max_df.to_csv(
        f'max-mean-amihud-{file_number}.csv',
        index=False
    )
    print(file_number, f'Runtime: {round(time.time() - start, 3)}')
