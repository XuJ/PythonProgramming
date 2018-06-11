import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def get_max_down(data, interval):
    def method(x):
        cummax = x[-interval - 1:].cummax()
        return np.max(((cummax - x) / cummax)[-interval:])

    return get_time_dependent_indicator(data, '净值日期', '单位净值', interval, method)


def get_time_dependent_indicator(data, date, value, interval, method):
    total_num = len(data)
    end_index = 1
    result = []
    while end_index <= total_num:
        start_index = end_index - interval if end_index - interval >= 0 else 0
        temp_data = data.iloc[start_index:end_index]
        result.append((temp_data.iloc[-1][date], method(temp_data[value])))
        end_index += 1
    return pd.DataFrame(result, columns=['净值日期', '最大回撤'])


year = 246
data = pd.read_csv('data/fund/110011/data.csv', encoding='gbk')
data.sort_values(by='净值日期', inplace=True)

for i, interval in enumerate([year//12, year // 4, year // 2, year, year * 2, year * 3]):
    fig = plt.figure(figsize=(16, 9))
    ax = fig.add_subplot(111)
    a = get_max_down(data, interval)
    ax.plot_date(a['净值日期'], a['最大回撤'], ls='-', marker=None)
    plt.title('{} days max down'.format(interval))
    fig.savefig('image/fund/110011_{}_days_maxdown.jpg'.format(interval))
