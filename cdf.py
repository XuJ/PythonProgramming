import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import scipy

data_dir = 'data/cdf'
fig_dir = 'image/cdf_2'

if not os.path.exists(fig_dir):
    os.makedirs(fig_dir)

# N = 1000000
N = 100000
np.random.seed(11)
x1 = np.random.randint(0, 101, size=N)
x2 = np.random.randint(0, 101, size=N)
x3 = np.random.randint(0, 101, size=N)
x4 = np.random.randint(0, 101, size=N)
x5 = np.random.randint(0, 101, size=N)
x6 = np.random.randint(0, 101, size=N)

x1 = x1 - x1 % 5
x2 = x2 - x2 % 5
x3 = x3 - x3 % 5
x4 = x4 - x4 % 5

data = pd.DataFrame({
    'x1': x1,
    'x2': x2,
    'x3': x3,
    'x4': x4,
    'x5': x5,
    'x6': x6,
    })

IC_map = {
    0: 29.41,
    5: 46.97,
    10: 52.53,
    15: 56.16,
    20: 58.67,
    25: 63.21,
    30: 65.28,
    35: 67.82,
    40: 71.61,
    45: 73.81,
    50: 76.75,
    55: 78.79,
    60: 80.81,
    65: 82.57,
    70: 84.91,
    75: 87.08,
    80: 89.15,
    85: 92.18,
    90: 97.33,
    95: 100.00,
    100: 100.00
    }

IG_map = {
    0: 27.07,
    5: 39.04,
    10: 46.30,
    15: 49.10,
    20: 50.62,
    25: 53.53,
    30: 54.88,
    35: 58.29,
    40: 59.37,
    45: 60.13,
    50: 62.17,
    55: 62.89,
    60: 63.57,
    65: 65.04,
    70: 66.76,
    75: 68.16,
    80: 69.06,
    85: 71.22,
    90: 73.28,
    95: 76.04,
    100: 82.17
    }

BTM_map = {
    0: 0.64,
    5: 33.33,
    10: 33.33,
    15: 34.17,
    20: 35.45,
    25: 38.93,
    30: 43.67,
    35: 46.16,
    40: 50.63,
    45: 55.38,
    50: 58.93,
    55: 63.59,
    60: 67.70,
    65: 70.05,
    70: 73.01,
    75: 75.64,
    80: 79.51,
    85: 86.40,
    90: 92.00,
    95: 96.79,
    100: 100.00
    }

EG_map = {
    0: 28.73,
    5: 37.68,
    10: 38.64,
    15: 40.16,
    20: 41.32,
    25: 42.93,
    30: 44.46,
    35: 46.32,
    40: 48.60,
    45: 50.39,
    50: 51.90,
    55: 55.46,
    60: 58.16,
    65: 61.50,
    70: 63.09,
    75: 64.29,
    80: 64.58,
    85: 64.58,
    90: 64.58,
    95: 64.58,
    100: 82.29
    }

EC_df = pd.DataFrame({
    'low_score': [0, 10, 20, 30, 40, 50, 60, 70, 80, 90],
    'high_score': [10, 20, 30, 40, 60, 60, 70, 80, 90, 100],
    'low_pct': [0, 0, 0, 0, 0.3071672355, 1.843003413, 53.174061433, 56.72354949, 69.38566553, 93.41296928],
    'high_pct': [0, 0, 0, 0.3071672355, 1.843003413, 53.174061433, 56.72354949, 69.38566553, 93.41296928, 100],
    })

RP_df = pd.DataFrame({
    'low_score': [0, 10, 20, 30, 40, 50, 60, 70, 80, 90],
    'high_score': [10, 20, 30, 40, 60, 60, 70, 80, 90, 100],
    'low_pct': [0, 0.614334471, 0.614334471, 0.614334471, 0.614334471, 46.89419795, 80.03412969, 91.87713311,
                97.06484642, 99.93174061],
    'high_pct': [0.614334471, 0.614334471, 0.614334471, 0.614334471, 46.89419795, 80.03412969, 91.87713311, 97.06484642,
                 99.93174061, 100],
    })


def score(x, map_df):
    if x == 100:
        y = np.random.uniform(map_df['low_score'].iloc[-1], map_df['high_score'].iloc[-1])
    else:
        tmp_df = EC_df[(x < EC_df['high_pct']) & (x >= EC_df['low_pct'])]
        y = np.random.uniform(tmp_df['low_score'].iloc[-1], tmp_df['high_score'].iloc[-1])
    return y


data['IC'] = data['x1'].map(IC_map)
data['IG'] = data['x2'].map(IG_map)
data['BTM'] = data['x3'].map(BTM_map)
data['EG'] = data['x4'].map(EG_map)
data['EC'] = data['x5'].apply(lambda x: score(x, EC_df))
data['RP'] = data['x6'].apply(lambda x: score(x, RP_df))
data['Score'] = data['IC'] * 0.2976 + data['EC'] * 0.0953 + data['IG'] * 0.0969 + data['EG'] * 0.0984 + data[
    'RP'] * 0.0370 + data['BTM'] * 0.3748

for column in ['IC', 'EC', 'IG', 'EG', 'BTM', 'Score']:
    info = data[column].describe()
    msg = 'min: {}\n25%: {}\n50%: {}\n75%: {}\nmax: {}'.format(round(info['min'], 2), round(info['25%'], 2),
                                                               round(info['50%'], 2), round(info['75%'], 2),
                                                               round(info['max'], 2))
    fig = plt.figure()
    ax = fig.add_subplot(111)
    sns.distplot(data[column])
    plt.title('{}_displot'.format(column))
    plt.text(0.1, 0.7, msg, transform=ax.transAxes, bbox=dict(facecolor='white', edgecolor='k', alpha=0.5))

    fig.savefig(os.path.join(fig_dir, '{}_displot.jpg'.format(column)))
    plt.close('all')

data.to_csv(os.path.join(data_dir, 'data_2.csv'))

pct_list = []
value_list = []
for step in np.linspace(0, 100, 21):
    pct_list.append(step)
    value_list.append(round(np.percentile(data['Score'], step), 2))
pct_df = pd.DataFrame({
    '分位数': pct_list,
    '得分': value_list
    })
pct_df.to_csv(os.path.join(data_dir, 'pct_2.csv'))

print(scipy.stats.percentileofscore(data['Score'], 55.95))
