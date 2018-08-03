import re

import pandas as pd


class IndicatorGroup(object):
    def __init__(self):
        self.data = pd.read_csv('data/chenwen/TF_allIndex.csv', index_col=0, encoding='utf8')
        self.groups = pd.read_table('data/chenwen/indicator_groups_20180614.txt', encoding='gbk', index_col=0)
        self.columns = list(set(self.data.columns) - set(['bbd_qyxx_id', 'company_name']))
        self.group_dict = {
            '综合实力风险': ['company_companytype', 'company_county', 'company_county', 'company_industry', 'ipo_company',
                       'is_so_company', 't30count', 't31count', 't32count', 't33count', 't34count', 't35count',
                       'feature_1_r_3', 'feature_1_r_4', 'feature_15_x_1', 'feature_15_x_2', 'feature_15_y_1',
                       'feature_15_y_2', 'unregcap_amount', 'regcap_currency', 'feature_2_x_1', 'feature_2_y',
                       'feature_1_x', 'feature_1_y', 'feature_1_n', 'feature_1_w', 't37count', 't38count', 'xzxk_count',
                       't108count', 't109count', 't110count', 'feature_5_c_i', 'feature_5_p_i'],
            '企业经营风险': ['operate_scope_count', 'gdxx_count', 'baxx_count', 'address_count', 'frname_count',
                       'regcap_count', 'regcap_count_1', 'regcap_count_2', 'company_type_count', 'realcap_count',
                       'others_change_count', 't_112', 't_113', 't_114', 't_115', 't_116', 't_117', 't_118', 't_119',
                       't_120', 't_121', 't_122', 't_123', 't_124', 't_125', 't_126', 't_127', 't_128', 't_129',
                       't_130', 't_131', 't_132', 't_133', 't_134', 't_135', 't_136', 't_137', 't_138', 't_139',
                       't_140', 't_141', 't_142', 'feature_7_e', 'salary_avg', 'feature_7_e_1', 'feature_7_e_2',
                       'feature_7_e_3', 'feature_7_e_4', 'feature_7_e_5', 'feature_7_e_6', 'esdate', 'feature_8_t_3',
                       'feature_8_t_2', 'feature_9_n_1', 'feature_9_n_2', 'jy_yq_xwsl'],
            '企业诚信风险': ['cx_ss_ktggsl', 'cx_ss_ygktggsl', 'cx_ss_bgktggsl', 'cx_ss_yjfxktggsl', 'cx_ss_ejfxktggsl',
                       'cx_ss_sjfxktggsl', 'cpws_count', 'cpws_pro_count', 'cpws_def_count', 'cpws_1l_count',
                       'cpws_2l_count', 'cpws_3l_count', 'cx_ss_bzxsl', 'cx_ss_bzxzje', 'dishonesty_count',
                       'gqdj_count', 'xzcf_count', 'feature_14_circxzcf', 't73count', 't97count', 't98count',
                       't99count'],
            '企业发展风险': ['feature_17_x', 'feature_24_x_0', 'feature_24_x_1', 'feature_24_x_2', 'feature_24_x_3',
                       'feature_18_d', 'zx_fzjg_count', 'dx_fzjg_count ', 'feature_25_y_0', 'feature_25_y_1',
                       'feature_25_y_2', 'feature_25_y_3', 't8count', 't9count', 'gqcz_count_history',
                       'gqcz_count_current', 'dcdy_count_history', 'dcdy_count_current', 'mortgage_count_history',
                       'mortgage_count_current', 'mortgage_area_history', 'mortgage_area_current',
                       'mortgage_price_history', 'mortgage_price_current', 'feature_7_e'],
            '关联方风险': ['t11count', 't12count', 't13count', 't14count', 't15count', 't16count', 't17count', 't18count',
                      't19count', 't20count', 't21count', 't22count', 't23count', 't24count', 't25count', 't26count',
                      't27count', 't28count', 't29count', 't111count', 'rs01_val', 'rs02_val', 'rs03_val', 'rs05_val',
                      'rs06_val', 'rs07_val', 'rs08_val', 'rs09_val', 'rs10_val', 'rs11_val', 'rs12_val', 'rs13_val',
                      'rs14_val', 'rs15_val', 'rs16_val', 'rs17_val', 'rs18_val', 'rs19_val', 'rs20_val', 'rs21_val',
                      'rs22_val', 'feature_20_x_1', 'feature_20_x_2', 'feature_20_x_3', 'feature_20_y_1',
                      'feature_20_y_2', 'feature_20_y_3', 'feature_20_z_1', 'feature_20_z_2', 'feature_20_z_3',
                      't104count', 't105count', 'feature_21_n', 'feature_22_d', 'feature_10_1_ktgg',
                      'feature_10_1_ktgg_1', 'feature_10_1_lending', 'feature_10_1_lending_1', 'feature_10_1_rmfygg',
                      'feature_10_1_fygg_1', 'feature_10_1_zgcpwsw', 'feature_10_1_zgcpwsw_1', 'feature_10_1_zgcpwsw_1',
                      'feature_10_2_ktgg', 'feature_10_2_ktgg_1', 'feature_10_2_lending', 'feature_10_2_lending_1',
                      'feature_10_2_rmfygg', 'feature_10_2_fygg_1', 'feature_10_2_zgcpwsw', 'feature_10_2_zgcpws_1',
                      'feature_10_2_zgcpwsw_1', 'feature_10_3_ktgg', 'feature_10_3_ktgg_1', 'feature_10_3_lending',
                      'feature_10_3_lending_1', 'feature_10_3_rmfygg', 'feature_10_3_fygg_3', 'feature_10_3_zgcpwsw',
                      'feature_10_3_zgcpws_3', 'feature_10_3_zgcpwsw_1', 'feature_11_1_xzcf', 'feature_11_1_xzcf_1',
                      'feature_11_2_xzcf', 'feature_11_2_xzcf_1', 'feature_11_3_xzcf', 'feature_11_3_xzcf_1',
                      't93count', 't94count', 't95count', 't96count', 't100count', 't101count', 't102count',
                      't103count', 'feature_16_v_1', 'feature_16_v_2', 'feature_16_v_3', 'feature_16_w_1',
                      'feature_16_w_2', 'feature_16_w_3', 't80count', 't81count', 't82count', 't83count', 't84count',
                      't85count', 't86count', 't141count', 't87count', 't88count', 't89count', 't90count', 't142count',
                      't91count', 't92count', 'feature_23_c_1', 'feature_23_c_2', 'feature_23_c_3', 'feature_23_d_1',
                      'feature_23_d_2', 'feature_23_d_3']
            }
        self.new_group_dict = dict()
        self.new_group_dict['没有找到的指标'] = []

    def group_indicators(self):
        for group_name in ['综合实力风险', '企业经营风险', '企业诚信风险', '企业发展风险', '关联方风险']:
            self.new_group_dict[group_name] = []
            indicator_name_list = self.group_dict[group_name]
            for indicator in indicator_name_list:
                p = re.compile('{}*'.format(indicator))
                n = 0
                for possible_indicator in self.columns:
                    if p.match(possible_indicator):
                        self.new_group_dict[group_name].append(possible_indicator)
                        n = 1
                if n == 0:
                    self.new_group_dict['没有找到的指标'].append(indicator)
        return self.new_group_dict

    def check_duplicates(self):
        group_list = ['综合实力风险', '企业经营风险', '企业诚信风险', '企业发展风险', '关联方风险', '没有找到的指标']
        for dict_key in group_list:
            other_group_list = list(set(group_list) - set([dict_key]))
            for indicator in self.new_group_dict[dict_key]:
                for other_dict_key in other_group_list:
                    if indicator in self.new_group_dict[other_dict_key]:
                        print(indicator, dict_key, other_dict_key)

    def check_remain_indicators(self):
        for indicator in self.columns:
            n = 0
            for dict_key in self.new_group_dict.keys():
                if indicator in self.new_group_dict[dict_key]:
                    n = 1
                    break
            if n == 0:
                print(indicator)


if __name__ == '__main__':
    a = IndicatorGroup()
    b = a.group_indicators()
    # a.check_duplicates()
    a.check_remain_indicators()
    c = {
        '没有找到的指标1': ['is_so_company', 'regcap_currency', 'xzxk_count', 't_112', 't_113', 't_114', 't_115', 't_116',
                     't_117', 't_118', 't_119', 't_120', 't_121', 't_122', 't_123', 't_124', 't_125', 't_126', 't_127',
                     't_128', 't_129', 't_130', 't_131', 't_132', 't_133', 't_134', 't_135', 't_136', 't_137', 't_138',
                     't_139', 't_140', 't_141', 't_142', 'salary_avg', 'cx_ss_ktggsl', 'feature_14_circxzcf',
                     'feature_25_y_0', 'feature_25_y_1', 'feature_25_y_2', 'feature_25_y_3', 'gqcz_count_history',
                     'gqcz_count_current', 'dcdy_count_history', 'dcdy_count_current', 'mortgage_count_history',
                     'mortgage_count_current', 'mortgage_area_history', 'mortgage_area_current',
                     'mortgage_price_history', 'mortgage_price_current', 'feature_10_1_fygg_1', 'feature_10_2_fygg_1',
                     'feature_10_2_zgcpws_1', 'feature_10_3_fygg_3', 'feature_10_3_zgcpws_3'],
        '没有找到的指标2': ['t120count', 't114count', 't131count', 't124count', 't138count', 't119count', 't118count',
                     't136count', 't127count', 't134count', 't130count', 't140count', 't129count', 't128count',
                     't122count', 't125count', 't121count', 't123count', 't117count', 't126count', 't116count',
                     't132count', 't135count', 't115count', 't139count', 't113count', 't133count', 't137count'],
        '综合实力风险': ['company_companytype', 'company_county', 'company_county', 'company_industry', 'ipo_company',
                   't30count', 't30count_max', 't30count_timeavg', 't30count_1yyir', 't30count_avg', 't30count_3mmir',
                   't30count_min', 't30count_6mmir', 't31count_3mmir', 't31count_avg', 't31count_max',
                   't31count_timeavg', 't31count', 't31count_6mmir', 't31count_1yyir', 't31count_min', 't32count_avg',
                   't32count_1yyir', 't32count_max', 't32count_timeavg', 't32count', 't32count_6mmir', 't32count_min',
                   't32count_3mmir', 't33count_6mmir', 't33count_avg', 't33count_min', 't33count_1yyir',
                   't33count_3mmir', 't33count_timeavg', 't33count_max', 't33count', 't34count_6mmir', 't34count_avg',
                   't34count', 't34count_3mmir', 't34count_max', 't34count_1yyir', 't34count_timeavg', 't34count_min',
                   't35count_6mmir', 't35count_min', 't35count_max', 't35count_1yyir', 't35count_3mmir',
                   't35count_timeavg', 't35count', 't35count_avg', 'feature_1_r_3_timeavg', 'feature_1_r_3',
                   'feature_1_r_4', 'feature_1_r_4_timeavg', 'feature_1_r_3_timeavg', 'feature_1_r_3', 'feature_1_r_4',
                   'feature_1_r_4_timeavg', 'feature_15_x_1_timeavg', 'feature_15_x_2', 'feature_15_x_1',
                   'feature_15_x_2_timeavg', 'feature_15_x_1_timeavg', 'feature_15_x_2', 'feature_15_x_1',
                   'feature_15_x_2_timeavg', 'feature_15_y_1_timeavg', 'feature_15_y_1', 'feature_15_y_2_timeavg',
                   'feature_15_y_2', 'feature_15_y_1_timeavg', 'feature_15_y_1', 'feature_15_y_2_timeavg',
                   'feature_15_y_2', 'unregcap_amount_min', 'unregcap_amount_avg', 'unregcap_amount_6mmir',
                   'unregcap_amount_1yyir', 'unregcap_amount_max', 'unregcap_amount_3mmir', 'unregcap_amount',
                   'feature_2_x_1', 'feature_2_x_1', 'feature_2_y', 'feature_1_r_3_timeavg', 'feature_1_x',
                   'feature_1_n_timeavg', 'feature_1_y_timeavg', 'feature_1_r_3', 'feature_1_r_4', 'feature_1_n',
                   'feature_1_r_4_timeavg', 'feature_1_w', 'feature_1_y', 'feature_1_w_timeavg', 'feature_1_x_timeavg',
                   'feature_1_r_3_timeavg', 'feature_1_x', 'feature_1_n_timeavg', 'feature_1_y_timeavg',
                   'feature_1_r_3', 'feature_1_r_4', 'feature_1_n', 'feature_1_r_4_timeavg', 'feature_1_w',
                   'feature_1_y', 'feature_1_w_timeavg', 'feature_1_x_timeavg', 'feature_1_r_3_timeavg', 'feature_1_x',
                   'feature_1_n_timeavg', 'feature_1_y_timeavg', 'feature_1_r_3', 'feature_1_r_4', 'feature_1_n',
                   'feature_1_r_4_timeavg', 'feature_1_w', 'feature_1_y', 'feature_1_w_timeavg', 'feature_1_x_timeavg',
                   'feature_1_r_3_timeavg', 'feature_1_x', 'feature_1_n_timeavg', 'feature_1_y_timeavg',
                   'feature_1_r_3', 'feature_1_r_4', 'feature_1_n', 'feature_1_r_4_timeavg', 'feature_1_w',
                   'feature_1_y', 'feature_1_w_timeavg', 'feature_1_x_timeavg', 't37count_timeavg', 't37count',
                   't38count', 't38count_timeavg', 't108count_last3mct', 't108count_last1yct', 't108count_1yyir',
                   't108count', 't108count_6myir', 't108count_3myir', 't108count_3mmir', 't108count_last2yct',
                   't108count_timeavg', 't108count_last6mct', 't108count_6mmir', 't109count_1yyir', 't109count_3myir',
                   't109count_timeavg', 't109count_3mmir', 't109count_last6mct', 't109count_6myir', 't109count_6mmir',
                   't109count', 't109count_last1yct', 't109count_last2yct', 't109count_last3mct', 't110count_last2yct',
                   't110count_last6mct', 't110count_last3mct', 't110count_timeavg', 't110count', 't110count_1yyir',
                   't110count_3mmir', 't110count_last1yct', 't110count_6myir', 't110count_3myir', 't110count_6mmir',
                   'feature_5_c_i', 'feature_5_p_i'],
        '企业经营风险': ['operate_scope_count', 'gdxx_count', 'baxx_count', 'address_count', 'frname_count', 'regcap_count_2',
                   'regcap_count_1', 'regcap_count', 'regcap_count_2', 'regcap_count_1', 'regcap_count_2',
                   'regcap_count_1', 'company_type_count', 'realcap_count', 'others_change_count', 'feature_7_e_4',
                   'feature_7_e_2', 'feature_7_e_timeavg', 'feature_7_e_1', 'feature_7_e_5', 'feature_7_e_6_timeavg',
                   'feature_7_e_4_timeavg', 'feature_7_e_3_timeavg', 'feature_7_e_6', 'feature_7_e_3', 'feature_7_e',
                   'feature_7_e_2_timeavg', 'feature_7_e_5_timeavg', 'feature_7_e_1_timeavg', 'feature_7_e_4',
                   'feature_7_e_2', 'feature_7_e_timeavg', 'feature_7_e_1', 'feature_7_e_5', 'feature_7_e_6_timeavg',
                   'feature_7_e_4_timeavg', 'feature_7_e_3_timeavg', 'feature_7_e_6', 'feature_7_e_3',
                   'feature_7_e_2_timeavg', 'feature_7_e_5_timeavg', 'feature_7_e_1_timeavg', 'feature_7_e_4',
                   'feature_7_e_2', 'feature_7_e_timeavg', 'feature_7_e_1', 'feature_7_e_5', 'feature_7_e_6_timeavg',
                   'feature_7_e_4_timeavg', 'feature_7_e_3_timeavg', 'feature_7_e_6', 'feature_7_e_3',
                   'feature_7_e_2_timeavg', 'feature_7_e_5_timeavg', 'feature_7_e_1_timeavg', 'feature_7_e_4',
                   'feature_7_e_2', 'feature_7_e_timeavg', 'feature_7_e_1', 'feature_7_e_5', 'feature_7_e_6_timeavg',
                   'feature_7_e_4_timeavg', 'feature_7_e_3_timeavg', 'feature_7_e_6', 'feature_7_e_3',
                   'feature_7_e_2_timeavg', 'feature_7_e_5_timeavg', 'feature_7_e_1_timeavg', 'feature_7_e_4',
                   'feature_7_e_2', 'feature_7_e_timeavg', 'feature_7_e_1', 'feature_7_e_5', 'feature_7_e_6_timeavg',
                   'feature_7_e_4_timeavg', 'feature_7_e_3_timeavg', 'feature_7_e_6', 'feature_7_e_3',
                   'feature_7_e_2_timeavg', 'feature_7_e_5_timeavg', 'feature_7_e_1_timeavg', 'feature_7_e_4',
                   'feature_7_e_2', 'feature_7_e_timeavg', 'feature_7_e_1', 'feature_7_e_5', 'feature_7_e_6_timeavg',
                   'feature_7_e_4_timeavg', 'feature_7_e_3_timeavg', 'feature_7_e_6', 'feature_7_e_3',
                   'feature_7_e_2_timeavg', 'feature_7_e_5_timeavg', 'feature_7_e_1_timeavg', 'feature_7_e_4',
                   'feature_7_e_2', 'feature_7_e_timeavg', 'feature_7_e_1', 'feature_7_e_5', 'feature_7_e_6_timeavg',
                   'feature_7_e_4_timeavg', 'feature_7_e_3_timeavg', 'feature_7_e_6', 'feature_7_e_3',
                   'feature_7_e_2_timeavg', 'feature_7_e_5_timeavg', 'feature_7_e_1_timeavg', 'esdate', 'feature_8_t_2',
                   'feature_8_t_3', 'feature_8_t_2', 'feature_8_t_3', 'feature_9_n_2_timeavg', 'feature_9_n_1_timeavg',
                   'feature_9_n_1', 'feature_9_n_2', 'feature_9_n_2_timeavg', 'feature_9_n_1_timeavg', 'feature_9_n_1',
                   'feature_9_n_2', 'jy_yq_xwsl_timeavg', 'jy_yq_xwsl_last2yct', 'jy_yq_xwsl_last3mct',
                   'jy_yq_xwsl_last6mct', 'jy_yq_xwsl_6mmir', 'jy_yq_xwsl', 'jy_yq_xwsl_3myir', 'jy_yq_xwsl_3mmir',
                   'jy_yq_xwsl_6myir', 'jy_yq_xwsl_1yyir', 'jy_yq_xwsl_last1yct'],
        '企业诚信风险': ['cx_ss_ygktggsl_last3mct', 'cx_ss_ygktggsl', 'cx_ss_ygktggsl_3myir', 'cx_ss_ygktggsl_3mmir',
                   'cx_ss_ygktggsl_1yyir', 'cx_ss_ygktggsl_6myir', 'cx_ss_ygktggsl_last2yct', 'cx_ss_ygktggsl_timeavg',
                   'cx_ss_ygktggsl_last6mct', 'cx_ss_ygktggsl_last1yct', 'cx_ss_ygktggsl_6mmir',
                   'cx_ss_bgktggsl_last3mct', 'cx_ss_bgktggsl_last1yct', 'cx_ss_bgktggsl', 'cx_ss_bgktggsl_6myir',
                   'cx_ss_bgktggsl_last6mct', 'cx_ss_bgktggsl_3myir', 'cx_ss_bgktggsl_last2yct', 'cx_ss_bgktggsl_3mmir',
                   'cx_ss_bgktggsl_1yyir', 'cx_ss_bgktggsl_6mmir', 'cx_ss_bgktggsl_timeavg', 'cx_ss_yjfxktggsl_6myir',
                   'cx_ss_yjfxktggsl_last6mct', 'cx_ss_yjfxktggsl_last2yct', 'cx_ss_yjfxktggsl_last3mct',
                   'cx_ss_yjfxktggsl_3myir', 'cx_ss_yjfxktggsl_3mmir', 'cx_ss_yjfxktggsl_1yyir',
                   'cx_ss_yjfxktggsl_timeavg', 'cx_ss_yjfxktggsl_last1yct', 'cx_ss_yjfxktggsl',
                   'cx_ss_yjfxktggsl_6mmir', 'cx_ss_ejfxktggsl_3myir', 'cx_ss_ejfxktggsl_6myir',
                   'cx_ss_ejfxktggsl_6mmir', 'cx_ss_ejfxktggsl_last2yct', 'cx_ss_ejfxktggsl_1yyir',
                   'cx_ss_ejfxktggsl_last1yct', 'cx_ss_ejfxktggsl_last6mct', 'cx_ss_ejfxktggsl_timeavg',
                   'cx_ss_ejfxktggsl_last3mct', 'cx_ss_ejfxktggsl_3mmir', 'cx_ss_ejfxktggsl',
                   'cx_ss_sjfxktggsl_last6mct', 'cx_ss_sjfxktggsl_last3mct', 'cx_ss_sjfxktggsl_3mmir',
                   'cx_ss_sjfxktggsl_6mmir', 'cx_ss_sjfxktggsl_timeavg', 'cx_ss_sjfxktggsl_1yyir',
                   'cx_ss_sjfxktggsl_last1yct', 'cx_ss_sjfxktggsl_3myir', 'cx_ss_sjfxktggsl_6myir',
                   'cx_ss_sjfxktggsl_last2yct', 'cx_ss_sjfxktggsl', 'cpws_count_6mmir', 'cpws_count_3myir',
                   'cpws_count', 'cpws_count_last2yct', 'cpws_count_1yyir', 'cpws_count_6myir', 'cpws_count_last1yct',
                   'cpws_count_last6mct', 'cpws_count_3mmir', 'cpws_count_last3mct', 'cpws_count_timeavg',
                   'cpws_pro_count_last1yct', 'cpws_pro_count_6myir', 'cpws_pro_count_6mmir', 'cpws_pro_count_3myir',
                   'cpws_pro_count', 'cpws_pro_count_3mmir', 'cpws_pro_count_1yyir', 'cpws_pro_count_last6mct',
                   'cpws_pro_count_timeavg', 'cpws_pro_count_last3mct', 'cpws_pro_count_last2yct', 'cpws_def_count',
                   'cpws_def_count_6myir', 'cpws_def_count_6mmir', 'cpws_def_count_last1yct', 'cpws_def_count_last6mct',
                   'cpws_def_count_last2yct', 'cpws_def_count_timeavg', 'cpws_def_count_1yyir',
                   'cpws_def_count_last3mct', 'cpws_def_count_3mmir', 'cpws_def_count_3myir', 'cpws_1l_count_last1yct',
                   'cpws_1l_count', 'cpws_1l_count_last3mct', 'cpws_1l_count_last2yct', 'cpws_1l_count_6mmir',
                   'cpws_1l_count_3myir', 'cpws_1l_count_timeavg', 'cpws_1l_count_last6mct', 'cpws_1l_count_6myir',
                   'cpws_1l_count_3mmir', 'cpws_1l_count_1yyir', 'cpws_2l_count_last1yct', 'cpws_2l_count_1yyir',
                   'cpws_2l_count_last3mct', 'cpws_2l_count_last6mct', 'cpws_2l_count_6mmir', 'cpws_2l_count_last2yct',
                   'cpws_2l_count_3myir', 'cpws_2l_count_6myir', 'cpws_2l_count_timeavg', 'cpws_2l_count_3mmir',
                   'cpws_2l_count', 'cpws_3l_count_last2yct', 'cpws_3l_count_3myir', 'cpws_3l_count_last1yct',
                   'cpws_3l_count_1yyir', 'cpws_3l_count_6myir', 'cpws_3l_count_last6mct', 'cpws_3l_count',
                   'cpws_3l_count_6mmir', 'cpws_3l_count_last3mct', 'cpws_3l_count_3mmir', 'cpws_3l_count_timeavg',
                   'cx_ss_bzxsl_last2yct', 'cx_ss_bzxsl_last6mct', 'cx_ss_bzxsl_3myir', 'cx_ss_bzxsl_1yyir',
                   'cx_ss_bzxsl_last3mct', 'cx_ss_bzxsl_last1yct', 'cx_ss_bzxsl', 'cx_ss_bzxsl_timeavg',
                   'cx_ss_bzxsl_6myir', 'cx_ss_bzxsl_3mmir', 'cx_ss_bzxsl_6mmir', 'cx_ss_bzxzje', 'cx_ss_bzxzje_3mmir',
                   'cx_ss_bzxzje_last3mct', 'cx_ss_bzxzje_last2yct', 'cx_ss_bzxzje_timeavg', 'cx_ss_bzxzje_last6mct',
                   'cx_ss_bzxzje_1yyir', 'cx_ss_bzxzje_last1yct', 'cx_ss_bzxzje_6myir', 'cx_ss_bzxzje_6mmir',
                   'cx_ss_bzxzje_3myir', 'dishonesty_count_1yyir', 'dishonesty_count_last6mct',
                   'dishonesty_count_last1yct', 'dishonesty_count_last3mct', 'dishonesty_count_6mmir',
                   'dishonesty_count', 'dishonesty_count_last2yct', 'dishonesty_count_6myir',
                   'dishonesty_count_timeavg', 'dishonesty_count_3myir', 'dishonesty_count_3mmir', 'gqdj_count_timeavg',
                   'gqdj_count', 'xzcf_count_last3mct', 'xzcf_count_1yyir', 'xzcf_count_last1yct', 'xzcf_count_timeavg',
                   'xzcf_count_last6mct', 'xzcf_count_3myir', 'xzcf_count_3mmir', 'xzcf_count_last2yct',
                   'xzcf_count_6mmir', 'xzcf_count_6myir', 'xzcf_count', 't73count_1yyir', 't73count_last3mct',
                   't73count_3mmir', 't73count_last1yct', 't73count', 't73count_last2yct', 't73count_timeavg',
                   't73count_6myir', 't73count_3myir', 't73count_6mmir', 't73count_last6mct', 't97count',
                   't98count_timeavg', 't98count', 't99count'],
        '企业发展风险': ['feature_17_x', 'feature_17_x_timeavg', 'feature_24_x_2', 'feature_24_x_3', 'feature_24_x_0',
                   'feature_24_x_1', 'feature_24_x_2', 'feature_24_x_3', 'feature_24_x_0', 'feature_24_x_1',
                   'feature_24_x_2', 'feature_24_x_3', 'feature_24_x_0', 'feature_24_x_1', 'feature_24_x_2',
                   'feature_24_x_3', 'feature_24_x_0', 'feature_24_x_1', 'feature_18_d', 'feature_18_d_timeavg',
                   'zx_fzjg_count_avg', 'zx_fzjg_count_max', 'zx_fzjg_count_3mmir', 'zx_fzjg_count',
                   'zx_fzjg_count_6mmir', 'zx_fzjg_count_min', 'zx_fzjg_count_1yyir', 'zx_fzjg_count_timeavg',
                   'dx_fzjg_count_1yyir', 'dx_fzjg_count', 'dx_fzjg_count_3mmir', 'dx_fzjg_count_min',
                   'dx_fzjg_count_avg', 'dx_fzjg_count_max', 'dx_fzjg_count_6mmir', 'dx_fzjg_count_timeavg',
                   't8count_timeavg', 't8count_3mmir', 't8count_avg', 't8count_max', 't8count_6mmir', 't8count',
                   't8count_min', 't8count_1yyir', 't9count_max', 't9count', 't9count_6mmir', 't9count_1yyir',
                   't9count_3mmir', 't9count_avg', 't9count_min', 't9count_timeavg', 'feature_7_e_4', 'feature_7_e_2',
                   'feature_7_e_timeavg', 'feature_7_e_1', 'feature_7_e_5', 'feature_7_e_6_timeavg',
                   'feature_7_e_4_timeavg', 'feature_7_e_3_timeavg', 'feature_7_e_6', 'feature_7_e_3', 'feature_7_e',
                   'feature_7_e_2_timeavg', 'feature_7_e_5_timeavg', 'feature_7_e_1_timeavg'],
        '关联方风险': ['t11count', 't12count', 't12count_max', 't12count_1yyir', 't12count_avg', 't12count_3mmir',
                  't12count_timeavg', 't12count_min', 't12count_6mmir', 't13count_6mmir', 't13count_1yyir', 't13count',
                  't13count_avg', 't13count_max', 't13count_timeavg', 't13count_3mmir', 't13count_min',
                  't14count_6mmir', 't14count_3mmir', 't14count_avg', 't14count_min', 't14count_1yyir',
                  't14count_timeavg', 't14count', 't14count_max', 't15count', 't16count_avg', 't16count_6mmir',
                  't16count_1yyir', 't16count', 't16count_max', 't16count_min', 't16count_timeavg', 't16count_3mmir',
                  't17count_timeavg', 't17count_min', 't17count_6mmir', 't17count_1yyir', 't17count_avg',
                  't17count_3mmir', 't17count', 't17count_max', 't18count_3mmir', 't18count_1yyir', 't18count',
                  't18count_max', 't18count_min', 't18count_6mmir', 't18count_avg', 't18count_timeavg', 't19count',
                  't20count_min', 't20count_avg', 't20count_3mmir', 't20count_6mmir', 't20count_1yyir', 't20count_max',
                  't20count_timeavg', 't20count', 't21count_avg', 't21count_timeavg', 't21count_6mmir',
                  't21count_1yyir', 't21count', 't21count_3mmir', 't21count_max', 't21count_min', 't22count_1yyir',
                  't22count_6mmir', 't22count_min', 't22count', 't22count_timeavg', 't22count_avg', 't22count_3mmir',
                  't22count_max', 't23count_6mmir', 't23count_3mmir', 't23count', 't23count_timeavg', 't23count_min',
                  't23count_1yyir', 't23count_avg', 't23count_max', 't24count_timeavg', 't24count_avg', 't24count_min',
                  't24count_max', 't24count_3mmir', 't24count_1yyir', 't24count', 't24count_6mmir', 't25count',
                  't25count_6mmir', 't25count_1yyir', 't25count_max', 't25count_avg', 't25count_min', 't25count_3mmir',
                  't25count_timeavg', 't26count_max', 't26count', 't26count_3mmir', 't26count_1yyir',
                  't26count_timeavg', 't26count_6mmir', 't26count_min', 't26count_avg', 't27count_6mmir',
                  't27count_1yyir', 't27count', 't27count_avg', 't27count_min', 't27count_max', 't27count_3mmir',
                  't27count_timeavg', 't28count_avg', 't28count_3mmir', 't28count_6mmir', 't28count_1yyir',
                  't28count_min', 't28count_timeavg', 't28count', 't28count_max', 't29count', 't29count_3mmir',
                  't29count_timeavg', 't29count_min', 't29count_max', 't29count_6mmir', 't29count_avg',
                  't29count_1yyir', 't111count', 'rs01_val', 'rs02_val', 'rs03_val', 'rs05_val', 'rs06_val', 'rs07_val',
                  'rs08_val', 'rs09_val', 'rs10_val', 'rs11_val', 'rs12_val', 'rs13_val', 'rs14_val', 'rs15_val',
                  'rs16_val', 'rs17_val', 'rs18_val', 'rs19_val', 'rs20_val', 'rs21_val', 'rs22_val', 'feature_20_x_1',
                  'feature_20_x_2', 'feature_20_x_3', 'feature_20_x_1_timeavg', 'feature_20_x_1', 'feature_20_x_2',
                  'feature_20_x_3', 'feature_20_x_1_timeavg', 'feature_20_x_1', 'feature_20_x_2', 'feature_20_x_3',
                  'feature_20_x_1_timeavg', 'feature_20_y_2', 'feature_20_y_1', 'feature_20_y_3',
                  'feature_20_y_3_timeavg', 'feature_20_y_2', 'feature_20_y_1', 'feature_20_y_3',
                  'feature_20_y_3_timeavg', 'feature_20_y_2', 'feature_20_y_1', 'feature_20_y_3',
                  'feature_20_y_3_timeavg', 'feature_20_z_1_timeavg', 'feature_20_z_3_timeavg', 'feature_20_z_1',
                  'feature_20_z_3', 'feature_20_z_2_timeavg', 'feature_20_z_2', 'feature_20_z_1_timeavg',
                  'feature_20_z_3_timeavg', 'feature_20_z_1', 'feature_20_z_3', 'feature_20_z_2_timeavg',
                  'feature_20_z_2', 'feature_20_z_1_timeavg', 'feature_20_z_3_timeavg', 'feature_20_z_1',
                  'feature_20_z_3', 'feature_20_z_2_timeavg', 'feature_20_z_2', 't104count_avg', 't104count_max',
                  't104count_min', 't104count_1yyir', 't104count', 't104count_6mmir', 't104count_3mmir',
                  't104count_timeavg', 't105count_timeavg', 't105count_1yyir', 't105count', 't105count_6mmir',
                  't105count_max', 't105count_min', 't105count_avg', 't105count_3mmir', 'feature_21_n', 'feature_22_d',
                  'feature_10_1_ktgg_timeavg', 'feature_10_1_ktgg_1', 'feature_10_1_ktgg_1_timeavg',
                  'feature_10_1_ktgg', 'feature_10_1_ktgg_timeavg', 'feature_10_1_ktgg_1',
                  'feature_10_1_ktgg_1_timeavg', 'feature_10_1_lending_1', 'feature_10_1_lending_1_timeavg',
                  'feature_10_1_lending_timeavg', 'feature_10_1_lending', 'feature_10_1_lending_1',
                  'feature_10_1_lending_1_timeavg', 'feature_10_1_lending_timeavg', 'feature_10_1_rmfygg_timeavg',
                  'feature_10_1_rmfygg', 'feature_10_1_zgcpwsw_timeavg', 'feature_10_1_zgcpwsw',
                  'feature_10_1_zgcpwsw_1_timeavg', 'feature_10_1_zgcpwsw_1', 'feature_10_1_zgcpwsw_timeavg',
                  'feature_10_1_zgcpwsw_1_timeavg', 'feature_10_1_zgcpwsw_1', 'feature_10_1_zgcpwsw_timeavg',
                  'feature_10_1_zgcpwsw_1_timeavg', 'feature_10_1_zgcpwsw_1', 'feature_10_2_ktgg_1_timeavg',
                  'feature_10_2_ktgg_1', 'feature_10_2_ktgg_timeavg', 'feature_10_2_ktgg',
                  'feature_10_2_ktgg_1_timeavg', 'feature_10_2_ktgg_1', 'feature_10_2_ktgg_timeavg',
                  'feature_10_2_lending', 'feature_10_2_lending_1', 'feature_10_2_lending_timeavg',
                  'feature_10_2_lending_1', 'feature_10_2_lending_timeavg', 'feature_10_2_rmfygg',
                  'feature_10_2_rmfygg_timeavg', 'feature_10_2_zgcpwsw_1', 'feature_10_2_zgcpwsw',
                  'feature_10_2_zgcpwsw_1_timeavg', 'feature_10_2_zgcpwsw_timeavg', 'feature_10_2_zgcpwsw_1',
                  'feature_10_2_zgcpwsw_1_timeavg', 'feature_10_2_zgcpwsw_timeavg', 'feature_10_3_ktgg_1_timeavg',
                  'feature_10_3_ktgg', 'feature_10_3_ktgg_1', 'feature_10_3_ktgg_timeavg',
                  'feature_10_3_ktgg_1_timeavg', 'feature_10_3_ktgg_1', 'feature_10_3_ktgg_timeavg',
                  'feature_10_3_lending_1', 'feature_10_3_lending_timeavg', 'feature_10_3_lending_1_timeavg',
                  'feature_10_3_lending', 'feature_10_3_lending_1', 'feature_10_3_lending_timeavg',
                  'feature_10_3_lending_1_timeavg', 'feature_10_3_rmfygg_timeavg', 'feature_10_3_rmfygg',
                  'feature_10_3_zgcpwsw_1', 'feature_10_3_zgcpwsw_1_timeavg', 'feature_10_3_zgcpwsw',
                  'feature_10_3_zgcpwsw_1', 'feature_10_3_zgcpwsw_1_timeavg', 'feature_11_1_xzcf_1',
                  'feature_11_1_xzcf_1_timeavg', 'feature_11_1_xzcf', 'feature_11_1_xzcf_1',
                  'feature_11_1_xzcf_1_timeavg', 'feature_11_2_xzcf_timeavg', 'feature_11_2_xzcf_1',
                  'feature_11_2_xzcf_1_timeavg', 'feature_11_2_xzcf', 'feature_11_2_xzcf_timeavg',
                  'feature_11_2_xzcf_1', 'feature_11_2_xzcf_1_timeavg', 'feature_11_3_xzcf_1',
                  'feature_11_3_xzcf_timeavg', 'feature_11_3_xzcf_1_timeavg', 'feature_11_3_xzcf',
                  'feature_11_3_xzcf_1', 'feature_11_3_xzcf_timeavg', 'feature_11_3_xzcf_1_timeavg', 't93count_max',
                  't93count_avg', 't93count_timeavg', 't93count_min', 't93count_3mmir', 't93count', 't93count_6mmir',
                  't93count_1yyir', 't94count_timeavg', 't94count_6mmir', 't94count_max', 't94count_avg', 't94count',
                  't94count_1yyir', 't94count_3mmir', 't94count_min', 't95count_6mmir', 't95count_timeavg',
                  't95count_avg', 't95count_3mmir', 't95count', 't95count_min', 't95count_1yyir', 't95count_max',
                  't96count_min', 't96count_6mmir', 't96count_1yyir', 't96count_3mmir', 't96count_timeavg',
                  't96count_avg', 't96count', 't96count_max', 't100count', 't100count_timeavg', 't101count',
                  't102count_timeavg', 't102count', 't103count', 'feature_16_v_2', 'feature_16_v_3',
                  'feature_16_v_3_timeavg', 'feature_16_v_1_timeavg', 'feature_16_v_1', 'feature_16_v_2',
                  'feature_16_v_3', 'feature_16_v_3_timeavg', 'feature_16_v_1_timeavg', 'feature_16_v_1',
                  'feature_16_v_2', 'feature_16_v_3', 'feature_16_v_3_timeavg', 'feature_16_v_1_timeavg',
                  'feature_16_v_1', 'feature_16_w_2_timeavg', 'feature_16_w_3', 'feature_16_w_3_timeavg',
                  'feature_16_w_1', 'feature_16_w_2', 'feature_16_w_2_timeavg', 'feature_16_w_3',
                  'feature_16_w_3_timeavg', 'feature_16_w_1', 'feature_16_w_2', 'feature_16_w_2_timeavg',
                  'feature_16_w_3', 'feature_16_w_3_timeavg', 'feature_16_w_1', 'feature_16_w_2', 't80count_min',
                  't80count_6mmir', 't80count_3mmir', 't80count_avg', 't80count_timeavg', 't80count', 't80count_1yyir',
                  't80count_max', 't81count_min', 't81count_max', 't81count_1yyir', 't81count_3mmir', 't81count_avg',
                  't81count_timeavg', 't81count_6mmir', 't81count', 't82count_timeavg', 't82count_max',
                  't82count_6mmir', 't82count', 't82count_min', 't82count_3mmir', 't82count_1yyir', 't82count_avg',
                  't83count_1yyir', 't83count_avg', 't83count_3mmir', 't83count_timeavg', 't83count', 't83count_max',
                  't83count_6mmir', 't83count_min', 't84count', 't84count_1yyir', 't84count_avg', 't84count_timeavg',
                  't84count_3mmir', 't84count_6mmir', 't84count_max', 't84count_min', 't85count_min', 't85count',
                  't85count_1yyir', 't85count_3mmir', 't85count_timeavg', 't85count_max', 't85count_6mmir',
                  't85count_avg', 't86count_timeavg', 't86count_avg', 't86count_min', 't86count', 't86count_1yyir',
                  't86count_max', 't86count_6mmir', 't86count_3mmir', 't141count_timeavg', 't141count_min', 't141count',
                  't141count_avg', 't141count_1yyir', 't141count_6mmir', 't141count_3mmir', 't141count_max', 't87count',
                  't87count_min', 't87count_3mmir', 't87count_1yyir', 't87count_6mmir', 't87count_avg',
                  't87count_timeavg', 't87count_max', 't88count_max', 't88count_1yyir', 't88count', 't88count_3mmir',
                  't88count_min', 't88count_6mmir', 't88count_avg', 't88count_timeavg', 't89count_timeavg',
                  't89count_6mmir', 't89count', 't89count_avg', 't89count_1yyir', 't89count_3mmir', 't89count_max',
                  't89count_min', 't90count_3mmir', 't90count_avg', 't90count_1yyir', 't90count_max', 't90count_6mmir',
                  't90count_min', 't90count', 't90count_timeavg', 't142count_min', 't142count_avg', 't142count_6mmir',
                  't142count', 't142count_1yyir', 't142count_3mmir', 't142count_max', 't142count_timeavg',
                  't91count_1yyir', 't91count_max', 't91count_min', 't91count_3mmir', 't91count_avg', 't91count_6mmir',
                  't91count', 't91count_timeavg', 't92count_timeavg', 't92count', 't92count_avg', 't92count_1yyir',
                  't92count_3mmir', 't92count_6mmir', 't92count_max', 't92count_min', 'feature_23_c_2_timeavg',
                  'feature_23_c_1_timeavg', 'feature_23_c_3_timeavg', 'feature_23_c_1', 'feature_23_c_2',
                  'feature_23_c_3', 'feature_23_c_2_timeavg', 'feature_23_c_1_timeavg', 'feature_23_c_3_timeavg',
                  'feature_23_c_1', 'feature_23_c_2', 'feature_23_c_3', 'feature_23_c_2_timeavg',
                  'feature_23_c_1_timeavg', 'feature_23_c_3_timeavg', 'feature_23_c_1', 'feature_23_c_2',
                  'feature_23_c_3', 'feature_23_d_3', 'feature_23_d_3_timeavg', 'feature_23_d_2',
                  'feature_23_d_1_timeavg', 'feature_23_d_2_timeavg', 'feature_23_d_1', 'feature_23_d_3',
                  'feature_23_d_3_timeavg', 'feature_23_d_2', 'feature_23_d_1_timeavg', 'feature_23_d_2_timeavg',
                  'feature_23_d_1', 'feature_23_d_3', 'feature_23_d_3_timeavg', 'feature_23_d_2',
                  'feature_23_d_1_timeavg', 'feature_23_d_2_timeavg', 'feature_23_d_1']
        }
