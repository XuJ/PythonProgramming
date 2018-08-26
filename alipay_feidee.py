import pandas as pd
import os

input_dir = 'data/alipay'
ouput_dir = 'data/alipay/result'


def convert_datetime(datetime_str):
    date_str, time_str = datetime_str.strip().split(' ')
    new_date_str = '.'.join(date_str.split('-'))
    new_time_str = time_str[:-3]
    new_datetime_str = ' '.join([new_date_str, new_time_str])
    return new_datetime_str


for csv_file in os.listdir(input_dir):
    if csv_file.endswith('.csv'):
        df = pd.read_csv(os.path.join(input_dir, csv_file), encoding='gbk', header=4, skipfooter=7,
                         usecols=[2, 7, 8, 9, 10, 11, 15], engine='python')
        columns = []
        for column in df.columns:
            columns.append(column.strip())
        df.columns = columns
        df['交易创建时间'] = df['交易创建时间'].apply(convert_datetime)
        df.to_csv(os.path.join(ouput_dir, csv_file), encoding='gbk', index=False)
