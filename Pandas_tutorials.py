import pandas as pd
from pandas import DataFrame
import datetime
from pandas_datareader import data
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import random

# sp500 = data.get_data_yahoo('%5EGSPC',start=datetime.datetime(2000,10,1),end=datetime.datetime(2012,1,1))
# print(sp500.head())
# sp500.to_csv('sp500_ohlc.csv')
df = pd.read_csv('sp500_ohlc.csv', index_col='Date', parse_dates=True)
df = pd.read_csv('sp500_ohlc.csv', parse_dates=True)
print(df.head())

df2 = df['Open']
print(df2.head())

df3 = df[['Close', 'High']]
print(df3.head())

del df3['High']

df3.rename(columns={'Close': 'CLOSE!!'}, inplace=True)
print(df3.head())

df4 = df3[(df3['CLOSE!!'] > 1400)]
print(df4.head())

df['H-L'] = df.High - df.Low
print(df.head())

df['100MA'] = pd.rolling_mean(df['Close'], 100)
print(df[200:210])

df['Difference'] = df.Close.diff()
print(df.head())

df.plot()
plt.show()

plt.plot(df['100MA'])
plt.show()
# matplotlib is dummy, no date on the x axis
# however, pandas.plot is smart
df['Close'].plot()
plt.show()

df[['Open', 'Close', 'High', 'Low', '100MA']].plot()
plt.show()
# pandas.plot creates index as default and it makes sure that the index cannot affect the graph

threedee = plt.figure().gca(projection='3d')
threedee.scatter(df.index, df['H-L'], df['Close'])
threedee.set_xlabel('Date')
threedee.set_ylabel('H-L')
threedee.set_zlabel('Close')
plt.show()

df['STD'] = pd.rolling_std(df['Close'], 25, min_periods=1)
axis1 = plt.subplot(2, 1, 1)
df['Close'].plot()
axis2 = plt.subplot(2, 1, 2, sharex=axis1)
df['STD'].plot()
plt.show()

print(df.describe())
print(df.corr())
print(df.cov())
print(df[['Volume', 'H-L']].corr())




def function(data):
    x = random.randrange(1, 5)
    return data * x


df['Multiple'] = list(map(function, df['Close']))
print(df.head())
