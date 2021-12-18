import pandas as pd
import numpy as np
import jdatetime

pd.options.mode.chained_assignment = None

# Read Bourseview data for market cap
# Concat all 75 tickers' data
me_list = []
for file_number in range(1, 76):
    print(file_number)
    me_path = f'E:/Thesis/New Sampling/Daily Data - Bourseview/'\
        f'{file_number}.xlsx'
    me_df = pd.read_excel(
        me_path,
        skiprows=7,
        usecols=[2, 3, 11],
        names=['date', 'open', 'market_cap'],
        na_values='-'
    )
    # Change order from old to new dates
    me_df = me_df[::-1].reset_index(drop=True)
    me_df['date'] = me_df['date'].str.replace('-', '')
    # Delete non-traded days
    me_df.dropna(subset=['open'], inplace=True)
    me_df.drop(columns='open', inplace=True)
    # Create monthly dataframe
    me_df = me_df.groupby(me_df['date'].str[:6]).last()
    me_df = me_df.drop(columns=['date']).reset_index()
    me_df.insert(1, 'ticker_num', file_number)
    me_list.append(me_df)
me_df = pd.concat(me_list, ignore_index=True)
me_df = me_df.loc[(me_df['date'] >= '139212') & (me_df['date'] <= '139900')]
me_df.reset_index(drop=True, inplace=True)
# Read rahavard 365 data for calculating returns
close_list = []
for file_number in range(1, 76):
    rahavard_path = f'E:/Thesis/New Sampling/Daily Data - Rahavard 365/'\
        f'{file_number}.txt'
    df = pd.read_csv(
        rahavard_path,
        usecols=[2, 7],
        names=['date', 'close'],
        header=0,
        dtype={'date': str},
        parse_dates=[0]
    )
    # Solve index reading problem, pandas add 2 index to the df
    df.reset_index(drop=True, inplace=True)
    # Convert to shamsi dates
    df['date'] = df['date'].apply(
        lambda x: jdatetime.date.fromgregorian(date=x).strftime('%Y%m%d')
    )
    # Create monthly dataframe
    df = df.groupby(df['date'].str[:6]).last()
    df = df.drop(columns=['date']).reset_index()
    df.insert(1, 'ticker_num', file_number)
    df['monthly_return'] = df['close'].pct_change()
    close_list.append(df)
df = pd.concat(close_list, ignore_index=True)
df = df.loc[(df['date'] >= '139212') & (df['date'] <= '139900')]
# Read index df for indicating open market days
index_path = r'E:\Thesis\New Sampling\TEDPIX\شاخص كل6.xls'
index_df = pd.read_excel(
    index_path,
    usecols=[1],
    names=['date'],
    dtype={'date': str}
)
index_df.dropna(inplace=True)
# The list of all months
months = index_df['date'].str[:6].unique().tolist()
# The list of months that we need for calculating market cap
me_months = [
    '139312', '139401', '139402', '139403', '139404', '139405', '139406',
    '139407', '139408', '139409', '139410', '139411', '139412', '139501',
    '139502', '139503', '139504', '139505', '139506', '139507', '139508',
    '139509', '139510', '139511', '139512', '139601', '139602', '139603',
    '139604', '139605', '139606', '139607', '139608', '139609', '139610',
    '139611', '139612', '139701', '139702', '139703', '139704', '139705',
    '139706', '139707', '139708', '139709', '139710', '139711', '139712',
    '139801', '139802', '139803', '139804', '139805', '139806', '139807',
    '139808', '139809', '139810', '139811', '139812'
]
# The list of months that we need for camculating MOM
mom_months = me_months[1:]
# Merge market cap and price dfs
merged_df = pd.merge(df, me_df, on=['ticker_num', 'date'])
# First, create a NaN column, and then add t-13 prices
merged_df.insert(5, 't-13 price', np.nan)
for month in mom_months:
    # Find t-13 prices
    for ticker in range(1, 76):
        t_13 = months[months.index(month) - 13]
        t_13_condtion = (merged_df['date'] == t_13)
        ticker_condition = (merged_df['ticker_num'] == ticker)
        try:
            t_13_price = merged_df.loc[
                t_13_condtion
                & ticker_condition
            ]['close'].values[0]
            previous_month = me_months[me_months.index(month) - 1]
            t_1_condtion = (merged_df['date'] == previous_month)
            merged_df.loc[
                (t_1_condtion & ticker_condition), 't-13 price'
            ] = t_13_price
        except:
            pass
# Calculate last 12 months return for month t (t-1, t-12)
merged_df['past_year_return'] = (
    (merged_df['close'] / merged_df['t-13 price'])
    - 1
)
mom_list = []
for month in mom_months:
    # Check t-13 price condition and t-1 market cap condition
    previous_month = months[months.index(month) - 1]
    me_condition = (merged_df['date'] == previous_month)
    mom_condition = (merged_df['past_year_return'].notna())
    portfo_const_df = merged_df.loc[me_condition & mom_condition]
    # Split each month ME into two groups
    conditions = [
        (
            portfo_const_df['market_cap']
            > portfo_const_df['market_cap'].median()
        ),
        (
            portfo_const_df['market_cap']
            <= portfo_const_df['market_cap'].median()
        )
    ]
    portfolio_size = np.select(conditions, ['B', 'S']).tolist()
    portfo_const_df.insert(6, 'size', portfolio_size)
    # Split each me portfolio into 3 MOM group
    q = [0, .3, .7, 1]
    labels = ['L', 'M', 'H']
    x_b = portfo_const_df.loc[
        portfo_const_df['size'] == 'B'
    ]['past_year_return']
    b_mom = pd.qcut(x=x_b, q=q, labels=labels).to_dict()
    x_s = portfo_const_df.loc[
        portfo_const_df['size'] == 'S'
    ]['past_year_return']
    s_mom = pd.qcut(x=x_s, q=q, labels=labels).to_dict()
    portfo_const_df['mom'] = pd.Series(b_mom)
    portfo_const_df['mom'].update(pd.Series(s_mom))
    # Extrect portfolio ticker numbers
    portfo_const_df['portfolio'] = (
        portfo_const_df['size'] + portfo_const_df['mom']
    )
    bh = portfo_const_df.loc[
        portfo_const_df['portfolio'] == 'BH'
        ]['ticker_num'].tolist()
    bl = portfo_const_df.loc[
        portfo_const_df['portfolio'] == 'BL'
        ]['ticker_num'].tolist()
    sh = portfo_const_df.loc[
        portfo_const_df['portfolio'] == 'SH'
        ]['ticker_num'].tolist()
    sl = portfo_const_df.loc[
        portfo_const_df['portfolio'] == 'SL'
        ]['ticker_num'].tolist()
    # Calculating value-weighted return for each portfolio in month t
    # Set conditions
    month_condition = (merged_df['date'] == month)
    bh_condition = merged_df['ticker_num'].isin(bh)
    bl_condition = merged_df['ticker_num'].isin(bl)
    sh_condition = merged_df['ticker_num'].isin(sh)
    sl_condition = merged_df['ticker_num'].isin(sl)
    # Construct portfolios
    bh_portfolio = merged_df.loc[month_condition & bh_condition]
    bl_portfolio = merged_df.loc[month_condition & bl_condition]
    sh_portfolio = merged_df.loc[month_condition & sh_condition]
    sl_portfolio = merged_df.loc[month_condition & sl_condition]
    # Calculate value-weighted returns
    bh_return = np.average(
        bh_portfolio.monthly_return,
        weights=bh_portfolio.market_cap
    )
    bl_return = np.average(
        bl_portfolio.monthly_return,
        weights=bl_portfolio.market_cap
    )
    sh_return = np.average(
        sh_portfolio.monthly_return,
        weights=sh_portfolio.market_cap
    )
    sl_return = np.average(
        sl_portfolio.monthly_return,
        weights=sl_portfolio.market_cap
    )
    # Calculate MOM, and add it to a list
    mom = (
        ((sh_return + bh_return) / 2)
        - ((sl_return + bl_return) / 2)
    )
    mom_list.append(mom)
mom_df = pd.Series(mom_list).to_excel('mom.xlsx')
