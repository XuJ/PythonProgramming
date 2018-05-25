import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

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
for m1 in np.linspace(0, 1, N):
    if m1 ==0 or m1==1:
        continue
    # consider only 2 pools A and B, m1 and m2
    m2 = 1-m1
    m = 1

    max_RR1 = 0
    min_RR2 = 0
    optimal_x12 = 0
    for x12 in np.linspace(0,m1,M):

        R1 = (m1-x12)/(m-x12)
        R2 = m2/(m-x12)
        r2 = R2/(m2+x12)
        r1 = (R1+x12*r2)/m1

        RR1 = r1*m1
        RR2 = r2*m2

        if RR1>max_RR1:
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
plt.axhline(0.5, c='r', ls='--')
plt.axvline(0.5, c='r', ls='--')
plt.legend(loc='best')
plt.xlabel('pool 1 proportion')
plt.ylabel('relative rewards')
plt.title('p1 attacks p2 rewards plot')
fig.savefig(os.path.join(fig_dir, 'p1 attacks p2 rewards plot.jpg'))
plt.close('all')

