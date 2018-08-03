import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import ticker

data_dir = 'data/pool_attack'
fig_dir = 'image/pool_attack'

if not os.path.exists(fig_dir):
    os.makedirs(fig_dir)

N = 100
M = 100
optimal_x12_list = []
max_RR1_list = []
min_RR2_list = []
m1_list = []
for m1 in np.arange(0, 1, 1 / N):
    if m1 == 0 or m1 == 1:
        continue
    # consider only 2 pools A and B, m1 and m2
    m2 = 1 - m1
    m = 1

    max_RR1 = 0
    min_RR2 = 0
    optimal_x12 = 0
    for x12 in np.arange(0, m1, 1 / M):

        R1 = (m1 - x12) / (m - x12)
        R2 = m2 / (m - x12)
        r2 = R2 / (m2 + x12)
        r1 = (R1 + x12 * r2) / m1

        RR1 = r1 * m1
        RR2 = r2 * m2

        if RR1 > max_RR1:
            max_RR1 = RR1
            min_RR2 = RR2
            optimal_x12 = x12

    optimal_x12_list.append(optimal_x12)
    max_RR1_list.append(max_RR1)
    min_RR2_list.append(min_RR2)
    m1_list.append(m1)

data = pd.DataFrame({
    'm1': m1_list,
    'optimal_x12': optimal_x12_list,
    'max_RR1': max_RR1_list,
    'min_RR2': min_RR2_list
    })
fig = plt.figure()
ax = fig.add_subplot(111)
# plt.plot(data['m1'], data['optimal_x12'], label='optimal_x12')
plt.plot(data['m1'], data['max_RR1'], label='pool 1 rewards')
plt.plot(data['m1'], data['min_RR2'], label='pool 2 rewards')
plt.axhline(0.5, c='g', ls='--', lw=1)
plt.axvline(0.5, c='r', ls='--', lw=1)
plt.legend(loc='lower right', bbox_to_anchor=(1, 0.6))
plt.xlabel('pool 1 proportion')
plt.ylabel('relative rewards')
plt.xlim([0, 1])
plt.ylim([0, 1])
ax.set_xticks(list(ax.get_xticks()) + [0.5])
ax.get_xticklabels()[-1].set_color('red')
ax.get_xticklines()[12].set_color('red')
ax.set_yticks(list(ax.get_xticks()) + [0.5])
ax.get_yticklabels()[-1].set_color('g')
ax.get_yticklines()[14].set_color('g')
formatter = ticker.FormatStrFormatter('%.1f')
ax.xaxis.set_major_formatter(formatter)
ax.yaxis.set_major_formatter(formatter)
max_RR1_middle = data.loc[data['m1']==0.50, 'max_RR1'].iloc[0]
min_RR2_middle = data.loc[data['m1']==0.50, 'min_RR2'].iloc[0]
m1_middle = data.loc[data['max_RR1']>=0.50, 'm1'].iloc[0]
plt.plot([0.5, 0.5],[max_RR1_middle, min_RR2_middle],'ro')
plt.plot([m1_middle],[0.5],'go')
bbox_props = dict(boxstyle="round", fc="w", ec="0.5", alpha=0.5)
plt.annotate('pool 1 rewards: %.2f'%max_RR1_middle, xy=(0.5, max_RR1_middle), xytext=(0.6, 0.56),color='r', arrowprops=dict(facecolor='r', frac=0.2,lw=0.1),ha='left',va='center',bbox=bbox_props)
plt.annotate('pool 2 rewards: %.2f'%min_RR2_middle, xy=(0.5, min_RR2_middle), xytext=(0.6, 0.44),color='r', arrowprops=dict(facecolor='r', frac=0.2,lw=0.1),ha='left',va='center',bbox=bbox_props)
plt.annotate('equal rewards pool 1 size: %.2f'%m1_middle, xy=(m1_middle, 0.5), xytext=(0.6, 0.3),color='g', arrowprops=dict(facecolor='g', frac=0.2,lw=0.1),ha='right',va='center',bbox=bbox_props)
plt.title('p1 attacks p2 rewards plot')
fig.savefig(os.path.join(fig_dir, 'p1 attacks p2 rewards plot.jpg'), dpi=200)
plt.close('all')
