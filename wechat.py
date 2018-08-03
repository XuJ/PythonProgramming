import os
import re

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

data_dir = 'data/wechat'
fig_dir = 'image/wechat'

if not os.path.exists(fig_dir):
    os.makedirs(fig_dir)


def only_number(x):
    try:
        pattern = re.compile('-*\d+')
        y = x[pattern.search(x).start():]
        return int(y)
    except:
        return np.nan


grey_df = pd.read_excel('{}/wechat.xlsx'.format(data_dir), sheetname='Sheet1')
black_df = pd.read_excel('{}/wechat.xlsx'.format(data_dir), sheetname='Sheet2')

grey_df.rename(columns={
    'found:1': 'found',
    'risk_score:87': 'risk_score',
    'risk_code:5': 'risk_code_1',
    'risk_value:2': 'risk_value_1',
    'risk_code:8': 'risk_code_2',
    'risk_value:3': 'risk_value_2',
    'Unnamed: 8': 'risk_code_3',
    'Unnamed: 9': 'risk_value_3',
    'Unnamed: 10': 'risk_code_4',
    'Unnamed: 11': 'risk_value_4',
    'Unnamed: 12': 'risk_code_5',
    'Unnamed: 13': 'risk_value_5',
    }, inplace=True)
del grey_df['id_found:-1']
grey_df['id'] = grey_df['id'].astype(object)
for column in grey_df.columns[1:]:
    grey_df[column] = grey_df[column].apply(lambda x: only_number(x))
grey_df['label'] = 'grey'

black_df.rename(columns={
    'pname_id': 'id',
    'found:-1': 'found',
    'risk_score:99': 'risk_score',
    'Unnamed: 5': 'risk_code_1',
    'Unnamed: 6': 'risk_value_1',
    'Unnamed: 7': 'risk_code_2',
    'Unnamed: 8': 'risk_value_2',
    'Unnamed: 9': 'risk_code_3',
    'Unnamed: 10': 'risk_value_3'
    }, inplace=True)
del black_df['id_found:-1']
del black_df['pname']

black_df['id'] = black_df['id'].astype(object)
for column in black_df.columns[1:]:
    black_df[column] = black_df[column].apply(lambda x: only_number(x))
black_df['label'] = 'black'

id_df = grey_df.append(black_df)

risk_code_dict = {
    1: '信贷中介',
    2: '不法分子',
    3: '虚假资料',
    4: '羊毛党',
    5: '身份认证失败',
    6: '疑似恶意欺诈',
    7: '失信名单',
    8: '异常支付行为',
    301: '恶意环境',
    503: '其他异常行为'
    }

risk_value_dict = {
    1: '低风险',
    2: '中风险',
    3: '高风险'
    }


def label_found_countplot(id_df):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    sns.countplot(id_df['label'], hue=id_df['found'])
    for bar in ax.patches:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2., height, '%.1f' % (height * 100 / 2000) + "%", ha='center',
                va='bottom')
    ax.set_title('Label vs. found count plot')
    fig.savefig('{}/label_found_countplot.jpg'.format(fig_dir))
    plt.close('all')


id_df_sub = id_df[id_df['found'] == 1]


def risk_score_distplot(id_df_sub):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    sns.distplot(id_df_sub.loc[id_df_sub['label'] == 'black', 'risk_score'], label='black')
    sns.distplot(id_df_sub.loc[id_df_sub['label'] == 'grey', 'risk_score'], label='grey')
    plt.legend(loc='best')
    plt.title('Risk score distribution')
    # plt.show()
    plt.savefig('{}/risk score distribution.jpg'.format(fig_dir))
    plt.close('all')


id_df_sub['risk_code_num'] = 1 + (id_df_sub['risk_code_1'] > 0) + (id_df_sub['risk_code_2'] > 0) + (
        id_df_sub['risk_code_3'] > 0) + (id_df_sub['risk_code_4'] > 0) + (id_df_sub['risk_code_5'] > 0) - 1
id_df_sub['risk_value_sum'] = np.nansum(
    id_df_sub[['risk_value_1', 'risk_value_2', 'risk_value_3', 'risk_value_4', 'risk_value_5']], axis=1)


def risk_code_number_per_id_countplot(id_df_sub):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    sns.countplot(id_df_sub['risk_code_num'], hue=id_df_sub['label'])
    for bar in ax.patches:
        if bar.get_height() > 0:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2., height, '%.1f' % (height * 100 / 1520) + "%", ha='center',
                    va='bottom')
    plt.title('Risk code number per id distribution')
    # plt.show()
    plt.savefig('{}/risk code number per id distribution.jpg'.format(fig_dir))
    plt.close('all')


def risk_value_sum_per_id_countplot(id_df_sub):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    sns.countplot(id_df_sub['risk_value_sum'], hue=id_df_sub['label'])
    for bar in ax.patches:
        if bar.get_height() > 0:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2., height, '%.1f' % (height * 100 / 1520) + "%", ha='center',
                    va='bottom')
    plt.title('Risk value sum per id distribution')
    # plt.show()
    plt.savefig('{}/risk value sum per id distribution.jpg'.format(fig_dir))
    plt.close('all')


# cate 3
id_df_sub = id_df_sub[(id_df_sub['risk_score']<=85)&(id_df_sub['risk_score']>=45)]

risk_code_list = []
risk_value_list = []
label_list = []
id_list = []
n = 0
for i, row in id_df_sub.iterrows():
    for risk_code_column, risk_value_column in zip(
            ['risk_code_1', 'risk_code_2', 'risk_code_3', 'risk_code_4', 'risk_code_5'],
            ['risk_value_1', 'risk_value_2', 'risk_value_3', 'risk_value_4', 'risk_value_5']):
        if row[risk_code_column] > 0:
            risk_code_list.append(row[risk_code_column])
            risk_value_list.append(row[risk_value_column])
            label_list.append(row['label'])
            id_list.append(n)
            n = n + 1
risk_df = pd.DataFrame({
    'id': id_list,
    'label': label_list,
    'risk_code': risk_code_list,
    'risk_value': risk_value_list
    })

risk_df['risk_code_cn'] = risk_df['risk_code'].map(risk_code_dict)
risk_df['risk_value_cn'] = risk_df['risk_value'].map(risk_value_dict)


def risk_code_countplot(risk_df):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    sns.countplot(risk_df['risk_code_cn'], hue=risk_df['label'])
    for bar in ax.patches:
        if bar.get_height() > 0:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2., height, '%.1f' % (height * 100 / 1520) + "%", ha='center',
                    va='bottom')
    plt.title('Risk code distribution')
    plt.legend(loc='upper right')
    plt.xticks(rotation=20)
    # plt.show()
    plt.savefig('{}/risk code distribution.jpg'.format(fig_dir))
    plt.close('all')


def risk_value_all_pieplot(risk_df):
    def risk_value_pieplot(risk_df, risk_code):
        fig = plt.figure()
        ax = fig.add_subplot(121)
        # plt.rcParams['font.sans-serif'] = ['SimHei']
        plt.rcParams['axes.unicode_minus'] = False
        plt.pie(risk_df.loc[(risk_df['risk_code_cn'] == risk_code) & (
                risk_df['label'] == 'grey'), 'risk_value_cn'].value_counts().values, labels=risk_df.loc[
            (risk_df['risk_code_cn'] == risk_code) & (
                    risk_df['label'] == 'grey'), 'risk_value_cn'].value_counts().index, autopct='%1.1f%%')
        plt.axis('equal')
        plt.title('{}_灰名单'.format(risk_code))

        ax1 = fig.add_subplot(122)
        plt.rcParams['font.sans-serif'] = ['SimHei']
        plt.rcParams['axes.unicode_minus'] = False
        plt.pie(risk_df.loc[(risk_df['risk_code_cn'] == risk_code) & (
                risk_df['label'] == 'black'), 'risk_value_cn'].value_counts().values, labels=risk_df.loc[
            (risk_df['risk_code_cn'] == risk_code) & (
                    risk_df['label'] == 'black'), 'risk_value_cn'].value_counts().index, autopct='%1.1f%%')
        plt.axis('equal')
        plt.title('{}_黑名单'.format(risk_code))

        # plt.show()
        plt.savefig('{}/risk code {} value pie.jpg'.format(fig_dir, risk_code))
        plt.close('all')

    for risk_code in risk_df['risk_code_cn'].unique():
        risk_value_pieplot(risk_df, risk_code)


risk_score_distplot(id_df_sub)
risk_code_number_per_id_countplot(id_df_sub)
risk_value_sum_per_id_countplot(id_df_sub)
risk_code_countplot(risk_df)
