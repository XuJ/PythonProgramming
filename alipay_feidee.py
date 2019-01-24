import pandas as pd
import os
import re

input_dir = 'data/alipay'
ouput_dir = 'data/alipay/result'


def convert_datetime(datetime_str):
    date_str, time_str = datetime_str.strip().split(' ')
    new_date_str = '.'.join(date_str.split('-'))
    new_time_str = time_str[:-3]
    new_datetime_str = ' '.join([new_date_str, new_time_str])
    return new_datetime_str


def clean_str(input_str):
    input_str = input_str.strip()
    input_str = re.sub('\s+', ' ', input_str)
    output_str = re.sub(' +', ' ', input_str)
    return output_str


def fund_debit_eliminate(product_name):
    p = re.compile('余额宝-\d\d\d\d.\d\d.\d\d-收益发放')
    m = p.match(product_name)
    if m:
        return False
    else:
        return True


def fund_transfer_eliminate(product_name):
    p1 = re.compile('余额宝-自动转入')
    p2 = re.compile('余额宝-单次转入')
    m1 = p1.match(product_name)
    m2 = p2.match(product_name)
    if m1 or m2:
        return False
    else:
        return True


if __name__ == '__main__':
    for csv_file in os.listdir(input_dir):
        if csv_file.endswith('.csv'):
            df = pd.read_csv(os.path.join(input_dir, csv_file), encoding='gbk', header=4, skipfooter=7,
                             usecols=[2, 7, 8, 9, 11, 15], engine='python')
            df.columns = ['交易时间', '交易对方', '商品名称', '金额', '交易状态', '资金状态']
            df['交易时间'] = df['交易时间'].apply(convert_datetime)
            df['交易对方'] = df['交易对方'].apply(clean_str)
            df['商品名称'] = df['商品名称'].apply(clean_str)
            df['金额'] = df['金额'].apply(float)
            df['交易状态'] = df['交易状态'].apply(clean_str)
            df['资金状态'] = df['资金状态'].apply(clean_str)
            df = df[df['商品名称'].apply(fund_debit_eliminate)]
            df = df[df['商品名称'].apply(fund_transfer_eliminate)]
            df.to_csv(os.path.join(ouput_dir, csv_file), encoding='gbk', index=False)
