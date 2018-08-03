import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

other_cost_total = pd.read_table('data/finance_cost/other_cost_total.txt', encoding='gbk')[:-1]
other_cost_total.rename(columns={
    '报销金额': '金额'
    }, inplace=True)
flight_cost_total = pd.read_table('data/finance_cost/flight_cost_total.txt', encoding='gbk')[:-1]
flight_cost_total.rename(columns={
    '机票总额': '金额'
    }, inplace=True)
hotel_cost_total = pd.read_table('data/finance_cost/hotel_cost_total.txt', encoding='gbk')[:-1]
hotel_cost_total.rename(columns={
    '酒店金额': '金额'
    }, inplace=True)
name_list = list(set(other_cost_total['姓名']) | set(flight_cost_total['姓名']) | set(hotel_cost_total['姓名']))
name_list.sort()


def name_cost(name, data):
    name_sub_list = data['姓名'].unique()
    if name not in name_sub_list:
        cost = 0
    else:
        data_sub = data[data['姓名'] == name]
        cost = data_sub['金额'].sum()
    return cost


other_cost_list = []
flight_cost_list = []
hotel_cost_list = []
for name in name_list:
    other_cost_list.append(name_cost(name, other_cost_total))
    flight_cost_list.append(name_cost(name, flight_cost_total))
    hotel_cost_list.append(name_cost(name, hotel_cost_total))

name_cost_total = pd.DataFrame({
    'name': name_list,
    'other_cost': other_cost_list,
    'flight_cost': flight_cost_list,
    'hotel_cost': hotel_cost_list
    })
name_cost_total['total_cost'] = name_cost_total[['other_cost', 'flight_cost', 'hotel_cost']].sum(1)
name_cost_total = name_cost_total[['name', 'other_cost', 'flight_cost', 'hotel_cost', 'total_cost']]
name_cost_total.sort_values(by='total_cost', ascending=False, inplace=True)
name_cost_total.reset_index(drop=True, inplace=True)
name_cost_total['total_pct'] = name_cost_total['total_cost'] / name_cost_total['total_cost'].sum(0)

name_cost_total['total_pct_agg'] = np.cumsum(name_cost_total['total_pct'])
name_cost_total.to_csv('data/finance_cost/name_cost_total.csv', encoding='gbk')

# fig = plt.figure()
# ax = fig.add_subplot()
# plt.plot(name_cost_total['total_pct_agg'])
# plt.show()
# sns.distplot(name_cost_total['total_cost'])
# sns.violinplot(name_cost_total['total_cost'])
#
# name_cost_total[name_cost_total['total_cost'] >= 15000]
# name_cost_total['total_cost'].describe()

other_cost_category = pd.read_table('data/finance_cost/other_cost_category.txt', encoding='gbk')[:-1]
other_cost_category.rename(columns={
    '报销总额': '金额'
    }, inplace=True)
flight_cost_category = pd.read_table('data/finance_cost/flight_cost_category.txt', encoding='gbk')[:-1]
flight_cost_category.rename(columns={
    '机票总额': '金额'
    }, inplace=True)


def project_cost(data):
    cost_list2 = []
    project_list2 = []
    name_list2 = []
    j = 0
    for i, row in data.iterrows():
        if row['姓名'] in name_list:
            j = i
        else:
            name_list2.append(data.iloc[j]['姓名'])
            project_list2.append(row['姓名'])
            cost_list2.append(row['金额'])

    data_2 = pd.DataFrame({
        'name': name_list2,
        'project': project_list2,
        'cost': cost_list2
        })
    return data_2


other_cost_category_2 = project_cost(other_cost_category)
flight_cost_category_2 = project_cost(flight_cost_category)
other_cost_category_2.to_csv('data/finance_cost/other_cost_category_2.csv', encoding='gbk')
flight_cost_category_2.to_csv('data/finance_cost/flight_cost_category_2.csv', encoding='gbk')

# a = flight_cost_category_2.groupby(by='project', as_index=False).sum()
# a.sort_values(by='other_cost', ascending=False, inplace=True)


other_cost_month = pd.read_table('data/finance_cost/other_cost_month.txt', encoding='gbk')[:-1]
other_cost_month.rename(columns={
    '报销总额': '金额'
    }, inplace=True)
hotel_cost_month = pd.read_table('data/finance_cost/hotel_cost_month.txt', encoding='gbk')[:-1]
hotel_cost_month.rename(columns={
    '酒店金额': '金额'
    }, inplace=True)


def month_cost(data):
    if data is other_cost_month:
        cost_list2 = []
        month_list2 = []
        name_list2 = []
        j = 0
        for i, row in data.iterrows():
            if row['姓名'] in name_list:
                j = i
            else:
                name_list2.append(data.iloc[j]['姓名'])
                month_list2.append(row['姓名'])
                cost_list2.append(row['金额'])

        data_2 = pd.DataFrame({
            'name': name_list2,
            'month': month_list2,
            'cost': cost_list2
            })
        return data_2
    elif data is hotel_cost_month:
        month_list = ['2018年1月', '2018年2月', '2018年3月', '2018年4月', '2018年5月']
        cost_list2 = []
        month_list2 = []
        place_list2 = []
        name_list2 = []
        j = 0
        k = 0
        for i, row in data.iterrows():
            if row['姓名'] in name_list:
                j = i
            elif row['姓名'] in month_list:
                k = i
            else:
                name_list2.append(data.iloc[j]['姓名'])
                month_list2.append(data.iloc[k]['姓名'])
                place_list2.append(row['姓名'])
                cost_list2.append(row['金额'])

        data_2 = pd.DataFrame({
            'name': name_list2,
            'month': month_list2,
            'place': place_list2,
            'cost': cost_list2
            })
        return data_2


other_cost_month_2 = month_cost(other_cost_month)
hotel_cost_month_2 = month_cost(hotel_cost_month)
other_cost_month_2.to_csv('data/finance_cost/other_cost_month_2.csv', encoding='gbk')
hotel_cost_month_2.to_csv('data/finance_cost/hotel_cost_month_2.csv', encoding='gbk')

# b = hotel_cost_month_2.groupby(by='place', as_index=False).sum()
# b.sort_values(by='cost', ascending=False, inplace=True)
