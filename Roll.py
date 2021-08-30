import requests
import json
import pandas as pd
import datetime

# assumes we roll futures every quarter on the first of the roll month (Mar, Jun, Sep, Dec)
CONTRACT_MONTHS = [3, 3, 6, 6, 6, 9, 9, 9, 12, 12, 12, 3]


def get_historical_data(contract):
    raw = requests.get(f'https://ftx.com/api/markets/{contract}/candles?resolution=86400').content
    raw = json.loads(raw)['result']
    df = pd.DataFrame(raw)
    df['startTime'] = df['startTime'].apply(lambda x: x[:10])
    df.index = df['startTime']
    df = df.drop(['time', 'startTime', 'volume'], axis=1)

    return df


def calc_carry(future, spot, expiry):
    days_to_expiry = (datetime.datetime.strptime(expiry, '%Y-%m-%d') - spot.index).days
    carry = (future.close / spot.close) ** (1/(days_to_expiry/365)) - 1
    return carry


def update_data():
    # update with dec 2021 futures
    btc_1231 = get_historical_data('BTC-1231')
    btc_spot = get_historical_data('BTC/USD')
    btc_1231.to_csv('BTC_dec2021.csv')
    btc_spot.to_csv('BTC_SPOT.csv')

    eth_1231 = get_historical_data('ETH-1231')
    eth_spot = get_historical_data('ETH/USD')
    eth_1231.to_csv('ETH_dec2021.csv')
    eth_spot.to_csv('ETH_SPOT.csv')


def get_portfolio(date, spot):
    # returns a notionally weighted portfolio of BTC and ETH contracts (long spot, short future)
    # could also return a vol adjusted basket
    cur_month = date.month
    future_month = CONTRACT_MONTHS[cur_month-1]
    future_year = date.year

    if cur_month == 12:
        future_year += 1

    btc_future = 'BTC-' + str(future_year) + '_' + '{:02d}'.format(future_month)
    eth_future = 'ETH-' + str(future_year) + '_' + '{:02d}'.format(future_month)

    try:
        prices = spot.loc[date.strftime('%Y-%m-%d')]
        ratio = round(prices['BTC']/prices['ETH'], 0)
    except:
        print('Spot data not availalbe')
        return {'BTC': 0, btc_future: 0, 'ETH': 0, eth_future: 0}

    return {'BTC': 1, btc_future: -1, 'ETH': ratio, eth_future: -ratio}

update_data() #comment to stop updating data


btc_1231 = pd.read_csv('BTC_dec2021.csv', parse_dates=True, index_col='startTime')
btc_spot = pd.read_csv('BTC_SPOT.csv', parse_dates=True, index_col='startTime')

eth_1231 = pd.read_csv('ETH_dec2021.csv', parse_dates=True, index_col='startTime')
eth_spot = pd.read_csv('ETH_SPOT.csv', parse_dates=True, index_col='startTime')

spot = pd.DataFrame([btc_spot.close, eth_spot.close]).T.dropna()
spot.columns = ['BTC', 'ETH']

btc_carry = calc_carry(btc_1231, btc_spot, '2021-12-31')
eth_carry = calc_carry(eth_1231, eth_spot, '2021-12-31')

carry = pd.DataFrame([btc_carry, eth_carry]).T.dropna()
carry.columns = ['BTC', 'ETH']
carry.to_csv('carry.csv')

portfolio = get_portfolio(datetime.datetime.strptime('2021-08-30', '%Y-%m-%d'), spot)
print(portfolio)