#!/usr/bin/python
# -*- coding: utf-8 -*-
#############固定格式 begin##################
from bbdSubModuleSdk.module import *

dsc = getDsc()
#############固定格式 end####################


#############自定义begin####################
import numpy as np
import pandas as pd

##定义固定变量
company_industry_dict = {
    'A': '01',  # 农、林、牧、渔业
    'B': '02',  # 采矿业
    'C': '02',  # 制造业
    'D': '04',  # 电力、热力、燃气及水生产和供应业
    'E': '02',  # 建筑业
    'F': '03',  # 批发和零售业
    'G': '02',  # 交通运输、仓储和邮政业
    'H': '03',  # 住宿和餐饮业
    'I': '02',  # 信息传输、软件和信息技术服务业
    'J': '04',  # 金融业
    'K': '04',  # 房地产业
    'L': '02',  # 租赁和商务服务业
    'M': '02',  # 科学研究和技术服务业
    'N': '02',  # 水利、环境和公共设施管理业
    'O': '03',  # 居民服务、修理和其他服务业
    'P': '03',  # 教育
    'Q': '03',  # 卫生和社会工作
    'R': '03',  # 文化、体育和娱乐业
    'Z': '03'  # 其他
    }
default_company_industry = company_industry_dict['Z']
company_scale_df = pd.DataFrame({
    'company_industry': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'Z'],
    'unreg_amount_low': [360, 800, 500, 2000, 1000, 200, 500, 200, 500, 1000, 2000, 1000, 1000, 1000, 200, 200, 200,
                         200, 200],
    'unreg_amount_high': [2000, 10000, 5000, 30000, 10000, 5000, 5000, 5000, 5000, 30000, 20000, 10000, 10000, 6800,
                          2000, 2000, 2000, 2000, 2000],
    't14count_low': [6, 4, 4, 5, 4, 4, 4, 4, 4, 4, 5, 4, 4, 4, 4, 4, 4, 4, 4],
    't14count_high': [38, 15, 12, 17, 10, 10, 10, 10, 10, 19, 12, 14, 12, 10, 10, 10, 10, 10, 10]
    })
company_scale_df.set_index('company_industry', inplace=True)
company_scale_dict = company_scale_df.to_dict('items')
# company_scale_dict = {
#     'A': {
#         't14count_high': 38,
#         't14count_low': 6,
#         'unreg_amount_high': 2000,
#         'unreg_amount_low': 360
#     },
#     'B': {
#         't14count_high': 15,
#         't14count_low': 4,
#         'unreg_amount_high': 10000,
#         'unreg_amount_low': 800
#     },
#     'C': {
#         't14count_high': 12,
#         't14count_low': 4,
#         'unreg_amount_high': 5000,
#         'unreg_amount_low': 500
#     },
#     'D': {
#         't14count_high': 17,
#         't14count_low': 5,
#         'unreg_amount_high': 30000,
#         'unreg_amount_low': 2000
#     },
#     'E': {
#         't14count_high': 10,
#         't14count_low': 4,
#         'unreg_amount_high': 10000,
#         'unreg_amount_low': 1000
#     },
#     'F': {
#         't14count_high': 10,
#         't14count_low': 4,
#         'unreg_amount_high': 5000,
#         'unreg_amount_low': 200
#     },
#     'G': {
#         't14count_high': 10,
#         't14count_low': 4,
#         'unreg_amount_high': 5000,
#         'unreg_amount_low': 500
#     },
#     'H': {
#         't14count_high': 10,
#         't14count_low': 4,
#         'unreg_amount_high': 5000,
#         'unreg_amount_low': 200
#     },
#     'I': {
#         't14count_high': 10,
#         't14count_low': 4,
#         'unreg_amount_high': 5000,
#         'unreg_amount_low': 500
#     },
#     'J': {
#         't14count_high': 19,
#         't14count_low': 4,
#         'unreg_amount_high': 30000,
#         'unreg_amount_low': 1000
#     },
#     'K': {
#         't14count_high': 12,
#         't14count_low': 5,
#         'unreg_amount_high': 20000,
#         'unreg_amount_low': 2000
#     },
#     'L': {
#         't14count_high': 14,
#         't14count_low': 4,
#         'unreg_amount_high': 10000,
#         'unreg_amount_low': 1000
#     },
#     'M': {
#         't14count_high': 12,
#         't14count_low': 4,
#         'unreg_amount_high': 10000,
#         'unreg_amount_low': 1000
#     },
#     'N': {
#         't14count_high': 10,
#         't14count_low': 4,
#         'unreg_amount_high': 6800,
#         'unreg_amount_low': 1000
#     },
#     'O': {
#         't14count_high': 10,
#         't14count_low': 4,
#         'unreg_amount_high': 2000,
#         'unreg_amount_low': 200
#     },
#     'P': {
#         't14count_high': 10,
#         't14count_low': 4,
#         'unreg_amount_high': 2000,
#         'unreg_amount_low': 200
#     },
#     'Q': {
#         't14count_high': 10,
#         't14count_low': 4,
#         'unreg_amount_high': 2000,
#         'unreg_amount_low': 200
#     },
#     'R': {
#         't14count_high': 10,
#         't14count_low': 4,
#         'unreg_amount_high': 2000,
#         'unreg_amount_low': 200
#     },
#     'Z': {
#         't14count_high': 10,
#         't14count_low': 4,
#         'unreg_amount_high': 2000,
#         'unreg_amount_low': 200
#     }
# }
default_company_scale = company_scale_dict['Z']
scale_dict = {
    1: 'xw',
    2: 'zx',
    3: 'd'
    }
null_list = ['', '/', 'nan', 'null', '--', ' ', 'NaN', 'Null', '-', np.nan]


def method(data, low, high):
    if data in null_list:
        return 1
    elif data < low:
        return 1
    elif data >= high:
        return 3
    else:
        return 2


def method_merge(scale1, scale2):
    if scale1 == scale2:
        scale = scale1
    else:
        scale_max = max(scale1, scale2)
        scale = scale_max - 1
    return scale_dict[scale]


def get_company_scale(onecomp):
    company_industry = onecomp['company_industry'],
    unregcap_amount = onecomp['unregcap_amount'],
    t14count = onecomp['t14count'],
    company_type = onecomp['company_companytype']
    if company_type == '9300':
        return 'gt'
    else:
        company_scale_dict_sub = company_scale_dict.get(company_industry, default_company_scale)
        unregcap_amount_low = company_scale_dict_sub['unregcap_amount_low']
        unreg_amount_high = company_scale_dict_sub['unreg_amount_high']
        t14count_low = company_scale_dict_sub['t14count_low']
        t14count_high = company_scale_dict_sub['t14count_high']
        unregcap_amount_scale = method(unregcap_amount, unregcap_amount_low, unreg_amount_high)
        t14count_scale = method(t14count, t14count_low, t14count_high)
        return method_merge(unregcap_amount_scale, t14count_scale)


def get_company_industry(onecomp):
    company_industry = onecomp['company_industry']
    return company_industry_dict.get(company_industry, default_company_industry)


def grid():
    ##调取指标
    onecomp = {
        "company_industry": dsc.getIndex("basic.company_industry"),  # 行业类型
        "unregcap_amount": dsc.getIndex("risk.unregcap_amount") / 10000,  # 注册资本
        "t14count": dsc.getIndex("risk.t14count"),  # 一度关联方数量
        "company_companytype": dsc.getIndex("basic.company_companytype"),  # 企业类型
    }
    industry = get_company_industry(onecomp)  # 获得企业行业分类
    scale = get_company_scale(onecomp)  # 获得企业规模分类
    scale_industry_dict = {
        'scale': scale,
        'industry': industry
    }
    return scale_industry_dict
#############自定义end####################


#############导出指标 begin##################
dsc.build("SampleModule","示例模块").addIndex("regcap_amount",grid(),"注册金额").create()
#############导出指标 end##################
