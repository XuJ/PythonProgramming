import numpy as np
import pandas as pd
import os

insurance_df = pd.DataFrame({
    '养老保险': [0.19, 0.08, 3000],
    '医疗保险（含大病医疗）': [0.075, 0.02, 3255],
    '失业保险': [0.006, 0.004, 3255],
    '工伤保险': [0.001, 0, 3255],
    '公积金': [0.06, 0.06, 1500]
}, index=['个人缴纳', '公司缴纳', '缴费基数'])

tax_df = pd.DataFrame({
    '税率': [0.03, 0.1, 0.2, 0.25, 0.3, 0.35, 0.45],
    '速算扣除数': [0, 105, 555, 1005, 2755, 5505, 13505],
    '应纳税额下限': [0, 1500, 4500, 9000, 35000, 55000, 80000],
    '应纳税额上限': [1500, 4500, 9000, 35000, 55000, 80000, np.inf]
}, index=['0~1500', '1500~4500', '4500~9000', '9000~35000', '35000~55000', '55000~80000', '80000~inf', ])
