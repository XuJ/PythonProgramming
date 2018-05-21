import pandas as pd
import json
import numpy as np
import datetime as dt

with open('data.txt', 'r', encoding='utf8') as f:
    file = json.load(f)

data = file['data']
relationships = data['relationships']
nodes = data['nodes']

relationships_df = pd.DataFrame(relationships,columns=['source_bbd_id', 'destination_bbd_id','relation_type','position_old'])
relationships_df['position'] = relationships_df['position_old'].apply(lambda x: x['position'] if len(x)!=0 else np.nan)
nodes_df = pd.DataFrame(nodes, columns=['company_id','company_name','isperson','degree'])

df = relationships_df.copy(deep=True)
del df['position_old']
df1 = pd.merge(df, nodes_df, left_on='source_bbd_id', right_on='company_id', how='left')
del df1['company_id']
df1.rename(columns={
    'company_name': 'source_name',
    'isperson': 'source_isperson',
    'degree': 'source_degree'
    }, inplace=True)
df2 = pd.merge(df1, nodes_df, left_on='destination_bbd_id', right_on='company_id', how='left')
del df2['company_id']
df2.rename(columns={
    'company_name': 'destination_name',
    'isperson': 'destination_isperson',
    'degree': 'destination_degree'
    }, inplace=True)

df2['company_name']='核新产融（深圳）有限公司'
df2['bbd_qyxx_id']=nodes_df.loc[nodes_df['company_name']=='核新产融（深圳）有限公司','company_id'].iloc[0]
df2['dt']=dt.datetime.now()
df2.to_csv('data.csv', index=False, encoding='utf8')
df2.to_csv('data1.csv', index=False, encoding='gbk')


