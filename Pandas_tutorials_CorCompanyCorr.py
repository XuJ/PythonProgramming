import datetime
from pandas_datareader import data
import pandas as pd

C = data.get_data_yahoo('C',
                                 start=datetime.datetime(2011, 10, 1),
                                 end=datetime.datetime(2014, 1, 1))
AAPL = data.get_data_yahoo('AAPL',
                                 start=datetime.datetime(2011, 10, 1),
                                 end=datetime.datetime(2014, 1, 1))
MSFT = data.get_data_yahoo('MSFT',
                                 start=datetime.datetime(2011, 10, 1),
                                 end=datetime.datetime(2014, 1, 1))
TSLA = data.get_data_yahoo('TSLA',
                                 start=datetime.datetime(2011, 10, 1),
                                 end=datetime.datetime(2014, 1, 1))
BAC = data.get_data_yahoo('BAC',
                                 start=datetime.datetime(2011, 10, 1),
                                 end=datetime.datetime(2014, 1, 1))
BBRY = data.get_data_yahoo('BBRY',
                                 start=datetime.datetime(2011, 10, 1),
                                 end=datetime.datetime(2014, 1, 1))
CMG = data.get_data_yahoo('CMG',
                                 start=datetime.datetime(2011, 10, 1),
                                 end=datetime.datetime(2014, 1, 1))
EBAY = data.get_data_yahoo('EBAY',
                                 start=datetime.datetime(2011, 10, 1),
                                 end=datetime.datetime(2014, 1, 1))
JPM = data.get_data_yahoo('JPM',
                                 start=datetime.datetime(2011, 10, 1),
                                 end=datetime.datetime(2014, 1, 1))
SBUX = data.get_data_yahoo('SBUX',
                                 start=datetime.datetime(2011, 10, 1),
                                 end=datetime.datetime(2014, 1, 1))
TGT = data.get_data_yahoo('TGT',
                                 start=datetime.datetime(2011, 10, 1),
                                 end=datetime.datetime(2014, 1, 1))
WFC = data.get_data_yahoo('WFC',
                                 start=datetime.datetime(2011, 10, 1),
                                 end=datetime.datetime(2014, 1, 1))
print(C.head())

del C['Open']
del C['High']
del C['Low']
del C['Close']
del C['Volume']

corComp = C

corComp.rename(columns={'Adj Close': 'C'}, inplace=True)

corComp['BAC'] = BAC['Adj Close']
corComp['MSFT'] = MSFT['Adj Close']
corComp['TSLA'] = TSLA['Adj Close']

corComp['AAPL'] = AAPL['Adj Close']
corComp['BBRY'] = BBRY['Adj Close']
corComp['CMG'] = CMG['Adj Close']
corComp['EBAY'] = EBAY['Adj Close']
corComp['JPM'] = JPM['Adj Close']
corComp['SBUX'] = SBUX['Adj Close']
corComp['TGT'] = TGT['Adj Close']
corComp['WFC'] = WFC['Adj Close']

print(corComp.head())

print(corComp.corr())











