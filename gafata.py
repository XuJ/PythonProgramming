import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import numpy as np
import datetime as dt
<<<<<<< HEAD
import
=======
>>>>>>> test
import os

data_dir = 'data/gafata'
fig_dir = 'image/gafata'

if not os.path.exists(fig_dir):
    os.makedirs(fig_dir)

def clean_data(csv_dir):
    data = pd.read_csv(os.path.join(data_dir, csv_dir))
    data['Year'] = data['Date'].apply(lambda x: x[:4])
    data['Month'] = data['Date'].apply(lambda x: x[5:7])
    data['Day'] = data['Date'].apply(lambda x: x[8:])

    data_sub = data.drop_duplicates(subset=['Year', 'Month'], keep='last')[:-1]
    data_sub = data_sub[['Date','Adj Close']]

    company_name = csv_dir.split('.')[0]
    data_sub['{} Return Rate'.format(company_name)] = data_sub['Adj Close'].pct_change()/12*100
    data_sub.rename(columns={'Adj Close': '{} Adj Close'.format(company_name)}, inplace=True)
    return data_sub

goog = clean_data('GOOG.csv')
amzn = clean_data('AMZN.csv')
fb = clean_data('FB.csv')
aapl = clean_data('AAPL.csv')
baba = clean_data('BABA.csv')

data = pd.merge(goog, amzn, on='Date')
data = pd.merge(data, fb, on='Date')
data = pd.merge(data, aapl, on='Date')
data = pd.merge(data, baba, on='Date')
data = data[1:]

fig = plt.figure()
ax = fig.add_subplot(211)
plt.plot_date(data['Date'], data['GOOG Adj Close'], label='GOOG', ls='-', marker=None)
plt.plot_date(data['Date'], data['AMZN Adj Close'], label='AMZN', ls='-', marker=None)
plt.legend(loc='upper left')
ax2 = fig.add_subplot(212)
# ax2 = ax.twinx()
plt.plot_date(data['Date'], data['FB Adj Close'], label='FB', ls='-', marker=None)
plt.plot_date(data['Date'], data['AAPL Adj Close'], label='AAPL', ls='-', marker=None)
plt.plot_date(data['Date'], data['BABA Adj Close'], label='BABA', ls='-', marker=None)
plt.legend(loc='lower right')
plt.show()

fig = plt.figure()
ax = fig.add_subplot(111)
plt.plot_date(data['Date'], data['GOOG Return Rate'], label='GOOG', ls='-', marker=None)
plt.plot_date(data['Date'], data['AMZN Return Rate'], label='AMZN', ls='-', marker=None)
plt.plot_date(data['Date'], data['FB Return Rate'], label='FB', ls='-', marker=None)
plt.plot_date(data['Date'], data['AAPL Return Rate'], label='AAPL', ls='-', marker=None)
plt.plot_date(data['Date'], data['BABA Return Rate'], label='BABA', ls='-', marker=None)
plt.legend(loc='best')
plt.show()

data.to_csv(os.path.join(data_dir, 'gafata.csv'), index=False)
