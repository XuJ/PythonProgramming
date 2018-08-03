# encoding: utf-8

"""
注意：1.修改数据库名 变量：name
     2. 确认输入的公司名称存在的数据库 变量：company_name_table
     3.启动命令：PYSPARK_PYTHON=/opt/cloudera/parcels/Anaconda/bin/python /opt/spark-2.2.0/bin/spark-submit --master yarn --name aladdinMultiVersion --driver-memory 10g --driver-cores 2  --executor-memory 50g --executor-cores 10 --num-executors 20 /data8/qyxypf/aladdin3/program/aladdinMultiVersion.py
"""
import sys
import datetime
import networkx as nx
from pyspark import SparkConf
from pyspark.sql import SparkSession,Row

reload(sys)
sys.setdefaultencoding('utf8')

def main():
    conf = SparkConf()
    conf.set("spark.hadoop.dfs.replication", "2")
    conf.set("spark.shuffle.file.buffer", "128k")
    conf.set("spark.shuffle.memoryFraction", "0.4")
    conf.set("spark.reducer.maxSizeInFlight", "96m")
    conf.set("spark.debug.maxToStringFields", "2000")
    conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse/")

    spark = SparkSession \
        .builder \
        .appName("aladdinMultiVersion") \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()

    off_line_relations_version = ['20150430','20150630','20150930','20151230','20160310',
                                  '20160630','20160920','20161220','20170320','20170620']

    blacklist_p = spark.sql("show partitions dw.black_list").collect()
    blacklist_dt = ''.join(list(blacklist_p)[-1])[3:]

    qyxx_basic_p = spark.sql("show partitions dw.qyxx_basic").collect()
    qyxx_basic_dt = ''.join(list(qyxx_basic_p)[-1])[3:]

    dishonesty_p = spark.sql("show partitions dw.dishonesty").collect()
    dishonesty_dt = ''.join(list(dishonesty_p)[-1])[3:]

    zhixing_p = spark.sql("show partitions dw.zhixing").collect()
    zhixing_dt = ''.join(list(zhixing_p)[-1])[3:]

    qyxg_qyqs_p = spark.sql("show partitions dw.qyxg_qyqs").collect()
    qyxg_qyqs_dt = ''.join(list(qyxg_qyqs_p)[-1])[3:]

    ktgg_p = spark.sql("show partitions dw.ktgg").collect()
    ktgg_dt = ''.join(list(ktgg_p)[-1])[3:]

    qyxg_jyyc_p = spark.sql("show partitions dw.qyxg_jyyc").collect()
    qyxg_jyyc_dt = ''.join(list(qyxg_jyyc_p)[-1])[3:]

    qyxx_fzjg_extend_p = spark.sql("show partitions dw.qyxx_fzjg_extend").collect()
    qyxx_fzjg_extend_dt = ''.join(list(qyxx_fzjg_extend_p)[-1])[3:]

    qyxx_zhuanli_p = spark.sql("show partitions dw.qyxx_zhuanli").collect()
    qyxx_zhuanli_dt = ''.join(list(qyxx_zhuanli_p)[-1])[3:]

    rjzzq_p = spark.sql("show partitions dw.rjzzq").collect()
    rjzzq_dt = ''.join(list(rjzzq_p)[-1])[3:]

    xgxx_shangbiao_p = spark.sql("show partitions dw.xgxx_shangbiao").collect()
    xgxx_shangbiao_dt = ''.join(list(xgxx_shangbiao_p)[-1])[3:]

    qyxx_bgxx_p = spark.sql("show partitions dw.qyxx_bgxx").collect()
    qyxx_bgxx_dt = ''.join(list(qyxx_bgxx_p)[-1])[3:]

    database_part_name = 'tf_'           #数据库前半部分名称
    company_name_table = database_part_name+'20150430.company_list'   #保存有输入公司的变量


    for version in off_line_relations_version:

        database = database_part_name+version

        givenDate = str(version[0:4]) + '-' + str(version[4:6]) + '-' + str(version[6:8])

        givenDateTime = datetime.datetime.strptime(givenDate,'%Y-%m-%d')

        threeMonthAgo = (givenDateTime -  datetime.timedelta(days = 90)).strftime('%Y-%m-%d')

        halfyearAgo = (givenDateTime -  datetime.timedelta(days = 180)).strftime('%Y-%m-%d')

        oneYearAgo = (givenDateTime -  datetime.timedelta(days = 365)).strftime('%Y-%m-%d')

        twoYearAgo = (givenDateTime -  datetime.timedelta(days = 730)).strftime('%Y-%m-%d')

        ####修改关联方版本号

        # #第一阶段白名单,先用这句话将白名单计算出来保存到数据库中，再从数据库中读出来，直接使用的话可能会报空指针的异常

        spark.sql("create database if not exists "+database)

        ###修改公司列表的表
        spark.sql("select * from "+company_name_table).createOrReplaceTempView("companyList1")

        # 涉及到使用bbd_qyxx_id的，可能有重复的公司名，但是bbd_qyxx_id不同，导致数据重复
        spark.sql("select distinct company_name,bbd_qyxx_id from companyList1").createOrReplaceTempView(
            "companylist_id")

        # 只涉及到company_name的，排除重复的名称,替换原有的companyList
        spark.sql("select distinct company_name from companyList1").createOrReplaceTempView("companyList")

        # new_black_list和companylist的计算方式一样，只不过sentence_date取2016-05-30之前的
        spark.sql(
            "select distinct company_name,property,create_time sentence_date from dw.black_list where dt='"+blacklist_dt+"' "
            " and create_time<='" + givenDate + "' and (property='1001' or property='1002' or property='1003' or "
                                                "property='1004')").createOrReplaceTempView("new_black_list")

        # #获取关联方数据
        spark.sql("select * from ald3.off_line_relations_ where dt='"+version+"'").createOrReplaceTempView("flat_relation")

        # #qyxx_basic表
        spark.sql("select company_name,esdate,company_companytype,company_county,company_industry,regcap_amount,"
                  "company_enterprise_status,if(regcap_currency='美元',regcap_amount*6.8525,regcap_amount) unRegcap_amount,"
                  " case when ipo_company='上市公司' then '1'  else '0' end as ipo_company"
                  " from dw.qyxx_basic where dt='"+qyxx_basic_dt+"'").createOrReplaceTempView("qyxx_basic")

        # 指标t1-7
        spark.sql(
            "select l.company_name,ceil(datediff('" + givenDate + "',esdate)/365) esdate,company_companytype,company_county,"
                                                                  "company_industry,regcap_amount,unRegcap_amount,ipo_company from companyList l left join qyxx_basic r on "
                                                                  "l.company_name=r.company_name").createOrReplaceTempView(
            "t1_7")

        spark.sql("create table " + database + ".t1_7 as select * from t1_7")
        # #spark.sql("insert overwrite table " +database+".t1_7  select * from t1_7")

        # 指标t_8 当前目标公司自然人股东数
        # 获取flat_relation表关系中存在自然人股东的数据
        spark.sql("select company_name,source_name,destination_name,pos,isinvest from "
                  "flat_relation where sourceisperson='1' and isinvest='1'  ") \
            .createOrReplaceTempView("person_shareholder")

        spark.sql("select c.company_name,if(t8count is null,0,t8count) t8count from companyList c left join "
                  "(select company_name,count(*) t8count from (select /*+mapjoin(companyList) */  distinct l.company_name,"
                  "r.source_name from companyList l join  person_shareholder r on l.company_name=r.company_name and "
                  "l.company_name=r.destination_name) group by company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView('t_8')
        spark.sql("create table " + database + ".t_8 as select * from t_8")
        # spark.sql("insert overwrite table " +database+".t_8  select * from t_8")

        # #指标9 当前目标公司非自然人股东数
        spark.sql("select company_name,source_name,destination_name from "
                  "flat_relation where sourceisperson='0' and isinvest='1' ") \
            .createOrReplaceTempView("no_person_shareholder")

        spark.sql("select c.company_name,if(t9count is null,0,t9count) t9count from companyList c left join "
                  "(select company_name,count(*) t9count from (select /*+mapjoin(companyList) */  distinct l.company_name,"
                  "r.source_name from companyList l join  no_person_shareholder r on l.company_name=r.company_name and "
                  "l.company_name=r.destination_name) group by company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView('t_9')

        spark.sql("create table " + database + ".t_9 as select * from t_9")
        # spark.sql("insert overwrite table " +database+".t_9  select * from t_9")

        # 指标10 当前目标公司投资其他企业数量
        spark.sql("select c.company_name,if(t10count is null,0,t10count) t10count from companyList c left join "
                  "(select company_name,count(*) t10count from (select distinct l.company_name,r.destination_name from "
                  "companyList l join (select  source_name,"
                  "destination_name from flat_relation where isinvest='1' and sourceisperson='0') r "
                  "on l.company_name=r.source_name) group by company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_10")

        spark.sql("create table " + database + ".t_10 as select * from t_10")
        # spark.sql("insert overwrite table " +database+".t_10  select * from t_10")

        # 指标11 当前目标公司自然人比例
        spark.sql("select c.company_name,if(t11count is null,0,t11count) t11count from companyList c left join "
                  "(select company_name,round((t8count/(t8count+t9count)),2) t11count from "
                  "(select l.company_name,l.t8count,r.t9count from t_8 l join t_9 r on l.company_name=r.company_name)) lr "
                  "on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_11")

        spark.sql("create table " + database + ".t_11 as select * from t_11")
        # spark.sql("insert overwrite table " +database+".t_11  select * from t_11")

        # 指标12 当前一度关联自然人数
        spark.sql("select c.company_name,if(t12count is null,0,t12count) t12count from companyList c left join "
                  "(select l.company_name,count(*) t12count from companyList l join (select distinct company_name,"
                  "source_name from flat_relation where source_degree='1' "
                  "and sourceisperson='1') r on l.company_name=r.company_name group by l.company_name) lr "
                  "on c.company_name=lr.company_name").createOrReplaceTempView("t_12")

        spark.sql("create table " + database + ".t_12 as select * from t_12")
        # spark.sql("insert overwrite table " +database+".t_12  select * from t_12")

        # 指标13 一度关联非自然人数
        spark.sql("select c.company_name,if(t13count is null,0,t13count) t13count from companyList c left join "
                  "(select l.company_name,count(*) t13count from companyList l join (select company_name,tname from "
                  "(select distinct company_name,source_name tname from flat_relation where "
                  "source_degree='1' and sourceisperson='0') union distinct "
                  "(select distinct company_name,destination_name tname from flat_relation where "
                  "destination_degree='1')) r on l.company_name=r.company_name group by l.company_name) lr "
                  "on c.company_name=lr.company_name").createOrReplaceTempView("t_13")

        spark.sql("create table " + database + ".t_13 as select * from t_13")
        # spark.sql("insert overwrite table " +database+".t_13  select * from t_13")

        # 指标14 一度关联方数
        spark.sql(
            "select l.company_name,(t12count+t13count) t14count from t_12 l join t_13 r on l.company_name=r.company_name") \
            .createOrReplaceTempView("t_14")

        spark.sql("create table " + database + ".t_14 as select * from t_14")
        # spark.sql("insert overwrite table " +database+".t_14  select * from t_14")

        # 指标15 一度关联自然人比例
        spark.sql("select c.company_name,if(t15count is null,0,t15count) t15count from companyList c left join "
                  "(select l.company_name,round(t12count/(t12count+t13count),2) t15count from t_12 l join t_13 r on"
                  " l.company_name=r.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_15")

        spark.sql("create table " + database + ".t_15 as select * from t_15")
        # spark.sql("insert overwrite table " +database+".t_15  select * from t_15")

        # 指标16  二度关联自然人数
        spark.sql("select c.company_name,if(t16count is null,0,t16count) t16count from companyList c left join "
                  "(select l.company_name,count(*) t16count from companyList l join (select distinct company_name,"
                  "source_name from flat_relation where source_degree='2' "
                  "and sourceisperson='1') r on l.company_name=r.company_name group by l.company_name) lr "
                  "on c.company_name=lr.company_name").createOrReplaceTempView("t_16")

        spark.sql("create table " + database + ".t_16 as select * from t_16")
        # spark.sql("insert overwrite table " +database+".t_16  select * from t_16")

        # 指标17 二度关联非自然人数
        spark.sql("select c.company_name,if(t17count is null,0,t17count) t17count from companyList c left join "
                  "(select l.company_name,count(*) t17count from companyList l join (select company_name,tname from "
                  "(select distinct company_name,source_name tname from flat_relation where "
                  "source_degree='2' and sourceisperson='0') union distinct "
                  "(select distinct company_name,destination_name tname from flat_relation where "
                  "destination_degree='2')) r on l.company_name=r.company_name group by l.company_name) lr "
                  "on c.company_name=lr.company_name").createOrReplaceTempView("t_17")

        spark.sql("create table " + database + ".t_17 as select * from t_17")
        # spark.sql("insert overwrite table " +database+".t_17  select * from t_17")

        # 指标18 二度关联方数
        spark.sql(
            "select l.company_name,(t16count+t17count) t18count from t_16 l join t_17 r on l.company_name=r.company_name") \
            .createOrReplaceTempView("t_18")

        spark.sql("create table " + database + ".t_18 as select * from t_18")
        # spark.sql("insert overwrite table " +database+".t_18  select * from t_18")

        # 指标19 二度关联自然人比例
        spark.sql("select c.company_name,if(t19count is null,0,t19count) t19count from companyList c left join "
                  "(select l.company_name,round(t16count/(t16count+t17count),2) t19count from t_16 l join t_17 r on "
                  "l.company_name=r.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView("t_19")

        spark.sql("create table " + database + ".t_19 as select * from t_19")
        # spark.sql("insert overwrite table " +database+".t_19  select * from t_19")

        # 指标20 三度关联自然人数
        spark.sql("select c.company_name,if(t20count is null,0,t20count) t20count from companyList c left join "
                  "(select l.company_name,count(*) t20count from companyList l join (select distinct company_name,"
                  "source_name from flat_relation where source_degree='3' "
                  "and sourceisperson='1') r on l.company_name=r.company_name group by l.company_name) lr "
                  "on c.company_name=lr.company_name").createOrReplaceTempView("t_20")

        spark.sql("create table " + database + ".t_20 as select * from t_20")
        # spark.sql("insert overwrite table " +database+".t_20  select * from t_20")

        # 指数21 三度关联方数
        spark.sql("select c.company_name,if(t21count is null,0,t21count) t21count from companyList c left join "
                  "(select l.company_name,count(*) t21count from companyList l join (select company_name,tname from "
                  "(select distinct company_name,source_name tname from flat_relation where "
                  "source_degree='3') union distinct "
                  "(select distinct company_name,destination_name tname from flat_relation where "
                  "destination_degree='3')) r on l.company_name=r.company_name group by l.company_name) lr "
                  "on c.company_name=lr.company_name").createOrReplaceTempView("t_21")

        spark.sql("create table " + database + ".t_21 as select * from t_21")
        # spark.sql("insert overwrite table " +database+".t_21  select * from t_21")


        # 指数22 一度关联非自然人行业跨度
        spark.sql("select c.company_name,if(t22count is null,0,t22count) t22count from companyList c left join "
                  "(select lr.company_name,count(distinct company_industry) t22count from qyxx_basic qb join "
                  "(select l.company_name,tname from companyList l join (select company_name,tname from "
                  "(select distinct company_name,source_name tname from flat_relation where "
                  "source_degree='1' and sourceisperson='0') union distinct "
                  "(select distinct company_name,destination_name tname from flat_relation where "
                  "destination_degree='1')) r on l.company_name=r.company_name ) lr "
                  "on qb.company_name=lr.tname and qb.company_industry is not null and qb.company_industry !='' "
                  "and qb.company_industry !='NULL' group by lr.company_name) lrt "
                  "on c.company_name=lrt.company_name ") \
            .createOrReplaceTempView("t_22")

        spark.sql("create table " + database + ".t_22 as select * from t_22")
        # spark.sql("insert overwrite table " +database+".t_22  select * from t_22")

        # 指数23 二度关联非自然人行业跨度
        spark.sql("select c.company_name,if(t23count is null,0,t23count) t23count from companyList c left join "
                  "(select lr.company_name,count(distinct company_industry) t23count from qyxx_basic qb join "
                  "(select l.company_name,tname from companyList l join (select company_name,tname from "
                  "(select distinct company_name,source_name tname from flat_relation where "
                  "source_degree='2' and sourceisperson='0') union distinct "
                  "(select distinct company_name,destination_name tname from flat_relation where "
                  "destination_degree='2')) r on l.company_name=r.company_name ) lr "
                  "on qb.company_name=lr.tname and qb.company_industry is not null and qb.company_industry !=''"
                  " and qb.company_industry !='NULL' group by lr.company_name) lrt "
                  "on c.company_name=lrt.company_name") \
            .createOrReplaceTempView("t_23")

        spark.sql("create table " + database + ".t_23 as select * from t_23")
        # spark.sql("insert overwrite table " +database+".t_23  select * from t_23")

        # 指数24 目标公司董事数
        spark.sql("select c.company_name,if(t24count is null,0,t24count) t24count from companyList c "
                  "left join (select company_name,count(*) t24count from (select distinct company_name,"
                  "source_name from flat_relation where sourceisperson='1' and "
                  "destination_name=company_name and instr(pos,'董事')>0) group by company_name) r "
                  "on c.company_name=r.company_name").createOrReplaceTempView("t_24")

        spark.sql("create table " + database + ".t_24 as select * from t_24")
        # spark.sql("insert overwrite table " +database+".t_24  select * from t_24")

        # 指数25 目标公司监事数
        spark.sql("select c.company_name,if(t25count is null,0,t25count) t25count from companyList c "
                  "left join (select company_name,count(*) t25count from (select distinct company_name,"
                  "source_name from flat_relation where sourceisperson='1' and "
                  "destination_name=company_name and instr(pos,'监事')>0) group by company_name) r "
                  "on c.company_name=r.company_name").createOrReplaceTempView("t_25")

        spark.sql("create table " + database + ".t_25 as select * from t_25")
        # spark.sql("insert overwrite table " +database+".t_25  select * from t_25")

        # 指数26 目标公司高管数
        spark.sql("select c.company_name,if(t26count is null,0,t26count) t26count from companyList c "
                  "left join (select company_name,count(*) t26count from (select distinct company_name,"
                  "source_name from flat_relation where sourceisperson='1' and "
                  "destination_name=company_name and (instr(pos,'董事')>0 or instr(pos,'监事')>0 or instr(pos,'经理')>0 "
                  "or instr(pos,'总裁')>0 or instr(pos,'总监')>0 or instr(pos,'行长')>0 or instr(pos,'财务负责人')>0 or "
                  "instr(pos,'财务总监')>0 or instr(pos,'厂长')>0) ) group by company_name) r "
                  "on c.company_name=r.company_name").createOrReplaceTempView("t_26")

        spark.sql("create table " + database + ".t_26 as select * from t_26")
        # spark.sql("insert overwrite table " +database+".t_26  select * from t_26")

        # 指数27 一度关联公司董事数
        spark.sql("select l.company_name,r.tname from companyList l join (select company_name,tname from "
                  "(select company_name,source_name tname from flat_relation where sourceisperson='0' "
                  "and source_degree='1') union distinct (select company_name,destination_name tname from "
                  "flat_relation where destination_degree='1')) r on l.company_name=r.company_name") \
            .createOrReplaceTempView("oneDCL")

        spark.sql("select c.company_name,if(t27count is null,0,t27count) t27count from companyList c left join "
                  "(select company_name,count(*) t27count from (select distinct l.company_name,r.source_name from "
                  "oneDCL l join (select source_name,destination_name from "
                  "flat_relation where sourceisperson='1' and instr(pos,'董事')>0) r on "
                  "l.tname=r.destination_name) group by company_name) r on c.company_name=r.company_name ") \
            .createOrReplaceTempView("t_27")

        spark.sql("create table " + database + ".t_27 as select * from t_27")
        # spark.sql("insert overwrite table " +database+".t_27  select * from t_27")

        # 指数28 一度关联公司监事数
        spark.sql("select c.company_name,if(t28count is null,0,t28count) t28count from companyList c left join "
                  "(select company_name,count(*) t28count from (select distinct l.company_name,r.source_name from "
                  "oneDCL l join (select source_name,destination_name from "
                  "flat_relation where sourceisperson='1' and instr(pos,'监事')>0) r on "
                  "l.tname=r.destination_name) group by company_name) r on c.company_name=r.company_name ") \
            .createOrReplaceTempView("t_28")

        spark.sql("create table " + database + ".t_28 as select * from t_28")
        # spark.sql("insert overwrite table " +database+".t_28  select * from t_28")

        # 指数29 一度关联公司高管数  ,改了只统计company_name和source_name，不统计destination_name，添加了destination_degree='0'
        spark.sql("select c.company_name,if(t29count is null,0,t29count) t29count from companyList c left join "
                  "(select company_name,count(*) t29count from (select distinct l.company_name,r.source_name from "
                  "oneDCL l join (select company_name,source_name,destination_name from "
                  "flat_relation where sourceisperson='1'  and (instr(pos,'董事')>0 or "
                  "instr(pos,'监事')>0 or instr(pos,'经理')>0 "
                  "or instr(pos,'总裁')>0 or instr(pos,'总监')>0 or instr(pos,'行长')>0 or instr(pos,'财务负责人')>0 or "
                  "instr(pos,'财务总监')>0 or instr(pos,'厂长')>0)) r on "
                  "l.tname=r.destination_name and l.company_name=r.company_name) group by company_name) r on c.company_name=r.company_name ") \
            .createOrReplaceTempView("t_29")

        spark.sql("create table " + database + ".t_29 as select * from t_29")
        # spark.sql("insert overwrite table " +database+".t_29  select * from t_29")

        # 指数30 目标公司自然人股东对外投资数量
        spark.sql("select c.company_name,if(t30count is null,0,t30count) t30count from companyList c left join "
                  "(select m.company_name,count(*) t30count from (select distinct r.company_name,r.source_name "
                  "from companyList l join  person_shareholder  r on l.company_name=r.company_name and l.company_name = "
                  "r.destination_name) lr join person_shareholder m on lr.company_name=m.company_name and lr.source_name=m.source_name "
                  "and m.company_name != m.destination_name group by m.company_name) lrm on c.company_name=lrm.company_name") \
            .createOrReplaceTempView("t_30")

        spark.sql("create table " + database + ".t_30 as select * from t_30")
        # spark.sql("insert overwrite table " +database+".t_30  select * from t_30")

        # 指数31 目标公司自然人股东在外任职数量,添加了lr.company_name != m.destination_name
        spark.sql("select c.company_name,if(t31count is null,0,t31count) t31count from companyList c left join "
                  "(select lr.company_name,count(*) t31count from (select distinct r.company_name,r.source_name,r.destination_name "
                  "from companyList l join person_shareholder r on l.company_name=r.company_name and l.company_name = "
                  "r.destination_name ) lr join flat_relation m on lr.company_name=m.company_name and lr.source_name=m.source_name "
                  "and lr.company_name != m.destination_name "
                  "and (instr(pos,'董事')>0 or instr(pos,'监事')>0 or instr(pos,'经理')>0 "
                  "or instr(pos,'总裁')>0 or instr(pos,'总监')>0 or instr(pos,'行长')>0 or instr(pos,'财务负责人')>0 or "
                  "instr(pos,'财务总监')>0 or instr(pos,'厂长')>0) "
                  "group by lr.company_name) lrm on c.company_name=lrm.company_name") \
            .createOrReplaceTempView("t_31")

        spark.sql("create table " + database + ".t_31 as select * from t_31")
        # spark.sql("insert overwrite table " +database+".t_31  select * from t_31")

        # 指数32 目标公司高管对外投资数量
        spark.sql("select distinct company_name,source_name from flat_relation where sourceisperson='1' and "
                  "destination_name=company_name and (instr(pos,'董事')>0 or instr(pos,'监事')>0 or instr(pos,'经理')>0 "
                  "or instr(pos,'总裁')>0 or instr(pos,'总监')>0 or instr(pos,'行长')>0 or instr(pos,'财务负责人')>0 or "
                  "instr(pos,'财务总监')>0 or instr(pos,'厂长')>0)") \
            .createOrReplaceTempView("tExecutive")

        spark.sql("select c.company_name,if(t32count is null,0,t32count) t32count from companyList c left join "
                  "(select lr.company_name,count(*) t32count from (select l.company_name,r.source_name from companyList l "
                  "join tExecutive r on l.company_name=r.company_name) lr join flat_relation m on lr.company_name=m.company_name "
                  "and lr.source_name=m.source_name and lr.company_name != m.destination_name and m.isinvest='1' "
                  "group by lr.company_name) lrm on c.company_name=lrm.company_name").createOrReplaceTempView("t_32")

        spark.sql("create table " + database + ".t_32 as select * from t_32")
        # spark.sql("insert overwrite table " +database+".t_32  select * from t_32")

        # 指数33 目标公司高管在外任职数量
        spark.sql("select c.company_name,if(t33count is null,0,t33count) t33count from companyList c left join"
                  " (select company_name,count(*) t33count from "
                  "(select distinct lr.company_name,lr.source_name,m.destination_name from "
                  "(select l.company_name,r.source_name from companyList l "
                  "join tExecutive r on l.company_name=r.company_name) lr join (select * from flat_relation where "
                  "instr(pos,'董事')>0 or instr(pos,'监事')>0 or instr(pos,'经理')>0 "
                  "or instr(pos,'总裁')>0 or instr(pos,'总监')>0 or instr(pos,'行长')>0 or instr(pos,'财务负责人')>0 or "
                  "instr(pos,'财务总监')>0 or instr(pos,'厂长')>0 ) m on lr.company_name=m.company_name "
                  "and lr.source_name=m.source_name and lr.company_name != m.destination_name )"
                  "group by company_name) lrm on c.company_name=lrm.company_name").createOrReplaceTempView("t_33")

        spark.sql("create table " + database + ".t_33 as select * from t_33")
        # spark.sql("insert overwrite table " +database+".t_33  select * from t_33")

        # 指数34 法定代表人对外投资数量

        spark.sql("select distinct company_name,source_name from flat_relation where sourceisperson='1' and "
                  "destination_name=company_name and instr(pos,'法定代表人')>0 ") \
            .createOrReplaceTempView("tfr")

        spark.sql("select c.company_name,if(t34count is null,0,t34count) t34count from companyList c left join "
                  "(select lr.company_name,count(*) t34count from (select l.company_name,r.source_name from companyList l "
                  "join tfr r on l.company_name=r.company_name) lr join flat_relation m on lr.company_name=m.company_name "
                  "and lr.source_name=m.source_name and lr.company_name != m.destination_name and m.isinvest='1' "
                  "group by lr.company_name) lrm on c.company_name=lrm.company_name").createOrReplaceTempView("t_34")

        spark.sql("create table " + database + ".t_34 as select * from t_34")
        # spark.sql("insert overwrite table " +database+".t_34  select * from t_34")

        # 指数35 法定代表人在外任职数量

        spark.sql("select c.company_name,if(t35count is null,0,t35count) t35count from companyList c left join "
                  "(select company_name,count(*) t35count from "
                  "(select distinct lr.company_name,lr.source_name,m.destination_name from "
                  "(select l.company_name,r.source_name from companyList l "
                  "join tfr r on l.company_name=r.company_name) lr join (select * from flat_relation where "
                  "instr(pos,'董事')>0 or instr(pos,'监事')>0 or instr(pos,'经理')>0 "
                  "or instr(pos,'总裁')>0 or instr(pos,'总监')>0 or instr(pos,'行长')>0 or instr(pos,'财务负责人')>0 or "
                  "instr(pos,'财务总监')>0 or instr(pos,'厂长')>0 ) m on lr.company_name=m.company_name "
                  "and lr.source_name=m.source_name and lr.company_name != m.destination_name )"
                  "group by company_name) lrm on c.company_name=lrm.company_name").createOrReplaceTempView("t_35")

        spark.sql("create table " + database + ".t_35 as select * from t_35")
        # spark.sql("insert overwrite table " +database+".t_35  select * from t_35")

        # 指数36 2度以内关联方中上市公司数量  :先计算2度以内的公司，再和qyxx_basic关联，去重统计，改and为or
        spark.sql("select c.company_name,if(t36count is null,0,t36count) t36count from companyList c left join "
                  "(select lr.company_name,count(distinct tname) t36count  from (select distinct l.company_name,r.tname from companyList "
                  "l join (select * from (select company_name,source_name tname from flat_relation where  sourceisperson='0' and"
                  " (source_degree ='2' or source_degree='1' )) union distinct (select company_name,destination_name tname from "
                  "flat_relation where destination_degree ='2' or destination_degree='1')) r on l.company_name=r.company_name) lr "
                  "join (select company_name from dw.qyxx_basic where dt='"+qyxx_basic_dt+"' and ipo_company='上市公司') m "
                  "on lr.tname=m.company_name  group by lr.company_name) lrm on c.company_name=lrm.company_name") \
            .createOrReplaceTempView("t_36")

        spark.sql("create table " + database + ".t_36 as select * from t_36")
        # spark.sql("insert overwrite table " +database+".t_36  select * from t_36")

        # 指数37 1度关联方中PE、VC数量（入度）  (修改了去重)
        spark.sql("select c.company_name,if(t37count is null,0,t37count) t37count from companyList c left join "
                  "(select l.company_name,count(*) t37count from companyList l join (select distinct company_name,source_name "
                  "from flat_relation where "
                  "sourceisperson='0' and source_degree='1' and destination_degree='0' and isinvest='1' and "
                  "instr(source_name,'投资')>0 and instr(source_name,'有限合伙')>0 ) r on l.company_name=r.company_name "
                  "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView("t_37")

        spark.sql("create table " + database + ".t_37 as select * from t_37")
        # spark.sql("insert overwrite table " +database+".t_37  select * from t_37")

        # 指数38 2度以内关联方中优质PE,VC数量
        spark.sql("select * from ald3.yzvc").createOrReplaceTempView("yzvc")
        # spark.read.csv("/user/wangjinyang/data/yzvc.csv",header=True,sep=',').createOrReplaceTempView("yzvc")

        spark.sql("select c.company_name,if(t38count is null,0,t38count) t38count from companyList c left join "
                  "(select lr.company_name,count(*) t38count from (select l.company_name,tname from companyList l join "
                  "(select * from (select company_name,source_name tname from flat_relation where "
                  "cast(source_degree as int) <=2) union distinct (select company_name,destination_name tname from "
                  "flat_relation where cast(destination_degree as int) <=2)) r on l.company_name=r.company_name) lr "
                  "left semi join yzvc m on lr.tname=m.company_name group by lr.company_name) lrm on "
                  "c.company_name=lrm.company_name").createOrReplaceTempView("t_38")

        spark.sql("create table " + database + ".t_38 as select * from t_38")
        # spark.sql("insert overwrite table " +database+".t_38  select * from t_38")


        spark.sql(
            "select case_type,title,ju_proc,sentence_date,action_cause,def_litigant,caseout_come from ald3.zgcpwsw") \
            .createOrReplaceTempView("zgcpwsw")

        # 指数39 目标公司近两年裁判文书数量
        spark.sql("select c.company_name,if(t39count is null,0,t39count) t39count from companyList c left join "
                  "(select l.company_name,count(*) t39count from companyList l join (select def_litigant,action_cause from "
                  "zgcpwsw where sentence_date>='" + twoYearAgo + "' and sentence_date <='" + givenDate + "') r on "
                                                                                                          "l.company_name=r.def_litigant  group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_39")

        spark.sql("create table " + database + ".t_39 as select * from t_39")
        # spark.sql("insert overwrite table " +database+".t_39  select * from t_39")

        # 指数40 目标公司近一年裁判文书数量
        spark.sql("select c.company_name,if(t40count is null,0,t40count) t40count from companyList c left join "
                  "(select l.company_name,count(*) t40count from companyList l join (select def_litigant,action_cause from "
                  "zgcpwsw where sentence_date>='" + oneYearAgo + "' and sentence_date <='" + givenDate + "') r on "
                                                                                                          "l.company_name=r.def_litigant  group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_40")

        spark.sql("create table " + database + ".t_40 as select * from t_40")
        # spark.sql("insert overwrite table " +database+".t_40  select * from t_40")

        # 指数41 目标公司近半年裁判文书数量
        spark.sql("select c.company_name,if(t41count is null,0,t41count) t41count from companyList c left join "
                  "(select l.company_name,count(*) t41count from companyList l join (select def_litigant,action_cause from "
                  "zgcpwsw where sentence_date>='" + halfyearAgo + "' and sentence_date <='" + givenDate + "') r on "
                                                                                                           "l.company_name=r.def_litigant  group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_41")

        spark.sql("create table " + database + ".t_41 as select * from t_41")
        # spark.sql("insert overwrite table " +database+".t_41  select * from t_41")

        # 指数42 目标公司近三个月裁判文书数量
        spark.sql("select c.company_name,if(t42count is null,0,t42count) t42count from companyList c left join "
                  "(select l.company_name,count(*) t42count from companyList l join (select def_litigant,action_cause from "
                  "zgcpwsw where sentence_date>='" + threeMonthAgo + "' and sentence_date <='" + givenDate + "') r on "
                                                                                                             "l.company_name=r.def_litigant  group by l.company_name) lr on c.company_name=lr.company_name ") \
            .createOrReplaceTempView("t_42")

        spark.sql("create table " + database + ".t_42 as select * from t_42")
        # spark.sql("insert overwrite table " +database+".t_42  select * from t_42")

        # 指数43 近两年一级风险裁判文书数量
        spark.sql(
            "select * from zgcpwsw where sentence_date>= '" + twoYearAgo + "' and sentence_date<='" + givenDate + "'"
                                                                                                                  " and instr(ju_proc,'一审')>0 "
                                                                                                                  "and (caseout_come='原告胜诉' or caseout_come='部分胜诉' or "
                                                                                                                  "caseout_come='起诉方 胜诉' or caseout_come='起诉方 部分胜诉' or "
                                                                                                                  "caseout_come='原告 胜诉' or caseout_come='原告 部分胜诉' or "
                                                                                                                  "caseout_come='起诉方 部分胜诉' or caseout_come='上诉人胜诉' "
                                                                                                                  "or caseout_come='上诉人 部分胜诉' or caseout_come='上诉人 胜诉'"
                                                                                                                  " or caseout_come='申诉人 胜诉' or caseout_come='原告人 部分胜诉') and "
                                                                                                                  "(action_cause='执行' or action_cause='民间借贷纠纷' or action_cause='买卖合同纠纷' "
                                                                                                                  "or action_cause='金融借款合同纠纷' or action_cause='借款合同纠纷' "
                                                                                                                  "or action_cause='小额借款合同纠纷' or action_cause='企业借贷纠纷' "
                                                                                                                  "or action_cause='借款纠纷' or action_cause='债务纠纷' "
                                                                                                                  "or action_cause='买卖合同货款纠纷' or action_cause='买卖纠纷' ) ") \
            .createOrReplaceTempView("temp_43")

        spark.sql("select c.company_name,if(t43count is null,0,t43count) t43count from companyList c left join "
                  "(select l.company_name,count(*) t43count from companyList l join temp_43 r on "
                  "l.company_name=r.def_litigant group by l.company_name) lr on c.company_name=lr.company_name ") \
            .createOrReplaceTempView("t_43")

        spark.sql("create table " + database + ".t_43 as select * from t_43")
        # spark.sql("insert overwrite table " +database+".t_43  select * from t_43")

        # 指数44 近一年一级风险裁判文书数量

        spark.sql(
            "select * from zgcpwsw where sentence_date>= '" + oneYearAgo + "' and sentence_date<='" + givenDate + "'"
                                                                                                                  " and instr(ju_proc,'一审')>0 "
                                                                                                                  "and (caseout_come='原告胜诉' or caseout_come='部分胜诉' or "
                                                                                                                  "caseout_come='起诉方 胜诉' or caseout_come='起诉方 部分胜诉' or "
                                                                                                                  "caseout_come='原告 胜诉' or caseout_come='原告 部分胜诉' or "
                                                                                                                  "caseout_come='起诉方 部分胜诉' or caseout_come='上诉人胜诉' "
                                                                                                                  "or caseout_come='上诉人 部分胜诉' or caseout_come='上诉人 胜诉'"
                                                                                                                  " or caseout_come='申诉人 胜诉' or caseout_come='原告人 部分胜诉') and "
                                                                                                                  "(action_cause='执行' or action_cause='民间借贷纠纷' or action_cause='买卖合同纠纷' "
                                                                                                                  "or action_cause='金融借款合同纠纷' or action_cause='借款合同纠纷' "
                                                                                                                  "or action_cause='小额借款合同纠纷' or action_cause='企业借贷纠纷' "
                                                                                                                  "or action_cause='借款纠纷' or action_cause='债务纠纷' "
                                                                                                                  "or action_cause='买卖合同货款纠纷' or action_cause='买卖纠纷' ) ") \
            .createOrReplaceTempView("temp_44")

        spark.sql("select c.company_name,if(t44count is null,0,t44count) t44count from companyList c left join "
                  "(select l.company_name,count(*) t44count from companyList l join temp_44 r on "
                  "l.company_name=r.def_litigant group by l.company_name) lr on c.company_name=lr.company_name ") \
            .createOrReplaceTempView("t_44")

        spark.sql("create table " + database + ".t_44 as select * from t_44")
        # spark.sql("insert overwrite table " +database+".t_44  select * from t_44")

        # 指数45 近半年一级风险裁判文书数量

        spark.sql(
            "select * from zgcpwsw where sentence_date>= '" + halfyearAgo + "' and sentence_date<='" + givenDate + "'"
                                                                                                                   " and instr(ju_proc,'一审')>0 "
                                                                                                                   "and (caseout_come='原告胜诉' or caseout_come='部分胜诉' or "
                                                                                                                   "caseout_come='起诉方 胜诉' or caseout_come='起诉方 部分胜诉' or "
                                                                                                                   "caseout_come='原告 胜诉' or caseout_come='原告 部分胜诉' or "
                                                                                                                   "caseout_come='起诉方 部分胜诉' or caseout_come='上诉人胜诉' "
                                                                                                                   "or caseout_come='上诉人 部分胜诉' or caseout_come='上诉人 胜诉'"
                                                                                                                   " or caseout_come='申诉人 胜诉' or caseout_come='原告人 部分胜诉') and "
                                                                                                                   "(action_cause='执行' or action_cause='民间借贷纠纷' or action_cause='买卖合同纠纷' "
                                                                                                                   "or action_cause='金融借款合同纠纷' or action_cause='借款合同纠纷' "
                                                                                                                   "or action_cause='小额借款合同纠纷' or action_cause='企业借贷纠纷' "
                                                                                                                   "or action_cause='借款纠纷' or action_cause='债务纠纷' "
                                                                                                                   "or action_cause='买卖合同货款纠纷' or action_cause='买卖纠纷' ) ") \
            .createOrReplaceTempView("temp_45")

        spark.sql("select c.company_name,if(t45count is null,0,t45count) t45count from companyList c left join "
                  "(select l.company_name,count(*) t45count from companyList l join temp_45 r on "
                  "l.company_name=r.def_litigant group by l.company_name) lr on c.company_name=lr.company_name ") \
            .createOrReplaceTempView("t_45")

        spark.sql("create table " + database + ".t_45 as select * from t_45")
        # spark.sql("insert overwrite table " +database+".t_45  select * from t_45")

        # 指数46 近三个月一级风险裁判文书数量

        spark.sql(
            "select * from zgcpwsw where sentence_date>= '" + threeMonthAgo + "' and sentence_date<='" + givenDate + "'"
                                                                                                                     " and instr(ju_proc,'一审')>0 "
                                                                                                                     "and (caseout_come='原告胜诉' or caseout_come='部分胜诉' or "
                                                                                                                     "caseout_come='起诉方 胜诉' or caseout_come='起诉方 部分胜诉' or "
                                                                                                                     "caseout_come='原告 胜诉' or caseout_come='原告 部分胜诉' or "
                                                                                                                     "caseout_come='起诉方 部分胜诉' or caseout_come='上诉人胜诉' "
                                                                                                                     "or caseout_come='上诉人 部分胜诉' or caseout_come='上诉人 胜诉'"
                                                                                                                     " or caseout_come='申诉人 胜诉' or caseout_come='原告人 部分胜诉') and "
                                                                                                                     "(action_cause='执行' or action_cause='民间借贷纠纷' or action_cause='买卖合同纠纷' "
                                                                                                                     "or action_cause='金融借款合同纠纷' or action_cause='借款合同纠纷' "
                                                                                                                     "or action_cause='小额借款合同纠纷' or action_cause='企业借贷纠纷' "
                                                                                                                     "or action_cause='借款纠纷' or action_cause='债务纠纷' "
                                                                                                                     "or action_cause='买卖合同货款纠纷' or action_cause='买卖纠纷' ) ") \
            .createOrReplaceTempView("temp_46")

        spark.sql("select c.company_name,if(t46count is null,0,t46count) t46count from companyList c left join "
                  "(select l.company_name,count(*) t46count from companyList l join temp_46 r on "
                  "l.company_name=r.def_litigant group by l.company_name) lr on c.company_name=lr.company_name ") \
            .createOrReplaceTempView("t_46")

        spark.sql("create table " + database + ".t_46 as select * from t_46")
        # spark.sql("insert overwrite table " +database+".t_46  select * from t_46")

        # 指数47 近两年二级风险裁判文书数量
        spark.sql(
            "select * from zgcpwsw where sentence_date>= '" + twoYearAgo + "' and sentence_date<='" + givenDate + "'  "
                                                                                                                  "and instr(ju_proc,'一审')>0 "
                                                                                                                  "and (caseout_come='原告胜诉' or caseout_come='部分胜诉' or "
                                                                                                                  "caseout_come='起诉方 胜诉' or caseout_come='起诉方 部分胜诉' or "
                                                                                                                  "caseout_come='原告 胜诉' or caseout_come='原告 部分胜诉' or "
                                                                                                                  "caseout_come='起诉方 部分胜诉' or caseout_come='上诉人胜诉' "
                                                                                                                  "or caseout_come='上诉人 部分胜诉' or caseout_come='上诉人 胜诉'"
                                                                                                                  " or caseout_come='申诉人 胜诉' or caseout_come='原告人 部分胜诉') and "
                                                                                                                  "(action_cause='劳动争议' or action_cause='建设工程施工合同纠纷' or action_cause='追索劳动报酬纠纷' "
                                                                                                                  "or action_cause='劳动合同纠纷' or action_cause='融资租赁合同纠纷' "
                                                                                                                  "or action_cause='担保物权纠纷' or action_cause='抵押合同纠纷' "
                                                                                                                  "or action_cause='金融不良债权追偿纠纷' or action_cause='欠款纠纷' "
                                                                                                                  "or action_cause='担保合同纠纷' or action_cause='追索劳动报酬及经济补偿金纠纷' ) ") \
            .createOrReplaceTempView("temp_47")

        spark.sql("select c.company_name,if(t47count is null,0,t47count) t47count from companyList c left join "
                  "(select l.company_name,count(*) t47count from companyList l join temp_47 r on "
                  "l.company_name=r.def_litigant group by l.company_name) lr on c.company_name=lr.company_name ") \
            .createOrReplaceTempView("t_47")

        spark.sql("create table " + database + ".t_47 as select * from t_47")
        # spark.sql("insert overwrite table " +database+".t_47  select * from t_47")

        # 指数48 近一年二级风险裁判文书数量

        spark.sql(
            "select * from zgcpwsw where sentence_date>= '" + oneYearAgo + "' and sentence_date<='" + givenDate + "'  "
                                                                                                                  "and instr(ju_proc,'一审')>0 "
                                                                                                                  "and (caseout_come='原告胜诉' or caseout_come='部分胜诉' or "
                                                                                                                  "caseout_come='起诉方 胜诉' or caseout_come='起诉方 部分胜诉' or "
                                                                                                                  "caseout_come='原告 胜诉' or caseout_come='原告 部分胜诉' or "
                                                                                                                  "caseout_come='起诉方 部分胜诉' or caseout_come='上诉人胜诉' "
                                                                                                                  "or caseout_come='上诉人 部分胜诉' or caseout_come='上诉人 胜诉'"
                                                                                                                  " or caseout_come='申诉人 胜诉' or caseout_come='原告人 部分胜诉') and "
                                                                                                                  "(action_cause='劳动争议' or action_cause='建设工程施工合同纠纷' or action_cause='追索劳动报酬纠纷' "
                                                                                                                  "or action_cause='劳动合同纠纷' or action_cause='融资租赁合同纠纷' "
                                                                                                                  "or action_cause='担保物权纠纷' or action_cause='抵押合同纠纷' "
                                                                                                                  "or action_cause='金融不良债权追偿纠纷' or action_cause='欠款纠纷' "
                                                                                                                  "or action_cause='担保合同纠纷' or action_cause='追索劳动报酬及经济补偿金纠纷' ) ") \
            .createOrReplaceTempView("temp_48")

        spark.sql("select c.company_name,if(t48count is null,0,t48count) t48count from companyList c left join "
                  "(select l.company_name,count(*) t48count from companyList l join temp_48 r on "
                  "l.company_name=r.def_litigant group by l.company_name) lr on c.company_name=lr.company_name ") \
            .createOrReplaceTempView("t_48")

        spark.sql("create table " + database + ".t_48 as select * from t_48")
        # spark.sql("insert overwrite table " +database+".t_48  select * from t_48")

        # 指数49 近半年二级风险裁判文书数量

        spark.sql(
            "select * from zgcpwsw where sentence_date>= '" + halfyearAgo + "' and sentence_date<='" + givenDate + "'  "
                                                                                                                   "and instr(ju_proc,'一审')>0 "
                                                                                                                   "and (caseout_come='原告胜诉' or caseout_come='部分胜诉' or "
                                                                                                                   "caseout_come='起诉方 胜诉' or caseout_come='起诉方 部分胜诉' or "
                                                                                                                   "caseout_come='原告 胜诉' or caseout_come='原告 部分胜诉' or "
                                                                                                                   "caseout_come='起诉方 部分胜诉' or caseout_come='上诉人胜诉' "
                                                                                                                   "or caseout_come='上诉人 部分胜诉' or caseout_come='上诉人 胜诉'"
                                                                                                                   " or caseout_come='申诉人 胜诉' or caseout_come='原告人 部分胜诉') and "
                                                                                                                   "(action_cause='劳动争议' or action_cause='建设工程施工合同纠纷' or action_cause='追索劳动报酬纠纷' "
                                                                                                                   "or action_cause='劳动合同纠纷' or action_cause='融资租赁合同纠纷' "
                                                                                                                   "or action_cause='担保物权纠纷' or action_cause='抵押合同纠纷' "
                                                                                                                   "or action_cause='金融不良债权追偿纠纷' or action_cause='欠款纠纷' "
                                                                                                                   "or action_cause='担保合同纠纷' or action_cause='追索劳动报酬及经济补偿金纠纷' ) ") \
            .createOrReplaceTempView("temp_49")

        spark.sql("select c.company_name,if(t49count is null,0,t49count) t49count from companyList c left join "
                  "(select l.company_name,count(*) t49count from companyList l join temp_49 r on "
                  "l.company_name=r.def_litigant group by l.company_name) lr on c.company_name=lr.company_name ") \
            .createOrReplaceTempView("t_49")

        spark.sql("create table " + database + ".t_49 as select * from t_49")
        # spark.sql("insert overwrite table " +database+".t_49  select * from t_49")

        # 指数50 近三个月二级风险裁判文书数量

        spark.sql(
            "select * from zgcpwsw where sentence_date>= '" + threeMonthAgo + "' and sentence_date<='" + givenDate + "'  "
                                                                                                                     "and instr(ju_proc,'一审')>0 "
                                                                                                                     "and (caseout_come='原告胜诉' or caseout_come='部分胜诉' or "
                                                                                                                     "caseout_come='起诉方 胜诉' or caseout_come='起诉方 部分胜诉' or "
                                                                                                                     "caseout_come='原告 胜诉' or caseout_come='原告 部分胜诉' or "
                                                                                                                     "caseout_come='起诉方 部分胜诉' or caseout_come='上诉人胜诉' "
                                                                                                                     "or caseout_come='上诉人 部分胜诉' or caseout_come='上诉人 胜诉'"
                                                                                                                     " or caseout_come='申诉人 胜诉' or caseout_come='原告人 部分胜诉') and "
                                                                                                                     "(action_cause='劳动争议' or action_cause='建设工程施工合同纠纷' or action_cause='追索劳动报酬纠纷' "
                                                                                                                     "or action_cause='劳动合同纠纷' or action_cause='融资租赁合同纠纷' "
                                                                                                                     "or action_cause='担保物权纠纷' or action_cause='抵押合同纠纷' "
                                                                                                                     "or action_cause='金融不良债权追偿纠纷' or action_cause='欠款纠纷' "
                                                                                                                     "or action_cause='担保合同纠纷' or action_cause='追索劳动报酬及经济补偿金纠纷' ) ") \
            .createOrReplaceTempView("temp_50")

        spark.sql("select c.company_name,if(t50count is null,0,t50count) t50count from companyList c left join "
                  "(select l.company_name,count(*) t50count from companyList l join temp_50 r on "
                  "l.company_name=r.def_litigant group by l.company_name) lr on c.company_name=lr.company_name ") \
            .createOrReplaceTempView("t_50")

        spark.sql("create table " + database + ".t_50 as select * from t_50")
        # spark.sql("insert overwrite table " +database+".t_50  select * from t_50")

        # 指数51 近两年三级风险裁判文书数量

        spark.sql(
            "select * from zgcpwsw where sentence_date>= '" + twoYearAgo + "' and sentence_date<='" + givenDate + "'  "
                                                                                                                  "and instr(ju_proc,'一审')>0 "
                                                                                                                  "and (caseout_come='原告胜诉' or caseout_come='部分胜诉' or "
                                                                                                                  "caseout_come='起诉方 胜诉' or caseout_come='起诉方 部分胜诉' or "
                                                                                                                  "caseout_come='原告 胜诉' or caseout_come='原告 部分胜诉' or "
                                                                                                                  "caseout_come='起诉方 部分胜诉' or caseout_come='上诉人胜诉' "
                                                                                                                  "or caseout_come='上诉人 部分胜诉' or caseout_come='上诉人 胜诉'"
                                                                                                                  " or caseout_come='申诉人 胜诉' or caseout_come='原告人 部分胜诉') and "
                                                                                                                  "(action_cause='股权转让纠纷' or action_cause='股东资格确认纠纷' "
                                                                                                                  "or action_cause='股东出资纠纷' or action_cause='劳动争议纠纷' "
                                                                                                                  "or action_cause='公司解散纠纷' or action_cause='申请破产清算' ) ") \
            .createOrReplaceTempView("temp_51")

        spark.sql("select c.company_name,if(t51count is null,0,t51count) t51count from companyList c left join "
                  "(select l.company_name,count(*) t51count from companyList l join temp_51 r on "
                  "l.company_name=r.def_litigant group by l.company_name) lr on c.company_name=lr.company_name ") \
            .createOrReplaceTempView("t_51")

        spark.sql("create table " + database + ".t_51 as select * from t_51")
        # spark.sql("insert overwrite table " +database+".t_51  select * from t_51")

        # 指数52 近一年三级风险裁判文书数量

        spark.sql(
            "select * from zgcpwsw where sentence_date>= '" + oneYearAgo + "' and sentence_date<='" + givenDate + "'  "
                                                                                                                  "and instr(ju_proc,'一审')>0 "
                                                                                                                  "and (caseout_come='原告胜诉' or caseout_come='部分胜诉' or "
                                                                                                                  "caseout_come='起诉方 胜诉' or caseout_come='起诉方 部分胜诉' or "
                                                                                                                  "caseout_come='原告 胜诉' or caseout_come='原告 部分胜诉' or "
                                                                                                                  "caseout_come='起诉方 部分胜诉' or caseout_come='上诉人胜诉' "
                                                                                                                  "or caseout_come='上诉人 部分胜诉' or caseout_come='上诉人 胜诉'"
                                                                                                                  " or caseout_come='申诉人 胜诉' or caseout_come='原告人 部分胜诉') and "
                                                                                                                  "(action_cause='股权转让纠纷' or action_cause='股东资格确认纠纷' "
                                                                                                                  "or action_cause='股东出资纠纷' or action_cause='劳动争议纠纷' "
                                                                                                                  "or action_cause='公司解散纠纷' or action_cause='申请破产清算' ) ") \
            .createOrReplaceTempView("temp_52")

        spark.sql("select c.company_name,if(t52count is null,0,t52count) t52count from companyList c left join "
                  "(select l.company_name,count(*) t52count from companyList l join temp_52 r on "
                  "l.company_name=r.def_litigant group by l.company_name) lr on c.company_name=lr.company_name ") \
            .createOrReplaceTempView("t_52")

        spark.sql("create table " + database + ".t_52 as select * from t_52")
        # spark.sql("insert overwrite table " +database+".t_52  select * from t_52")

        # 指数53 近半年三级风险裁判文书数量

        spark.sql(
            "select * from zgcpwsw where sentence_date>= '" + halfyearAgo + "' and sentence_date<='" + givenDate + "'  "
                                                                                                                   "and instr(ju_proc,'一审')>0 "
                                                                                                                   "and (caseout_come='原告胜诉' or caseout_come='部分胜诉' or "
                                                                                                                   "caseout_come='起诉方 胜诉' or caseout_come='起诉方 部分胜诉' or "
                                                                                                                   "caseout_come='原告 胜诉' or caseout_come='原告 部分胜诉' or "
                                                                                                                   "caseout_come='起诉方 部分胜诉' or caseout_come='上诉人胜诉' "
                                                                                                                   "or caseout_come='上诉人 部分胜诉' or caseout_come='上诉人 胜诉'"
                                                                                                                   " or caseout_come='申诉人 胜诉' or caseout_come='原告人 部分胜诉') and "
                                                                                                                   "(action_cause='股权转让纠纷' or action_cause='股东资格确认纠纷' "
                                                                                                                   "or action_cause='股东出资纠纷' or action_cause='劳动争议纠纷' "
                                                                                                                   "or action_cause='公司解散纠纷' or action_cause='申请破产清算' ) ") \
            .createOrReplaceTempView("temp_53")

        spark.sql("select c.company_name,if(t53count is null,0,t53count) t53count from companyList c left join "
                  "(select l.company_name,count(*) t53count from companyList l join temp_53 r on "
                  "l.company_name=r.def_litigant group by l.company_name) lr on c.company_name=lr.company_name ") \
            .createOrReplaceTempView("t_53")

        spark.sql("create table " + database + ".t_53 as select * from t_53")
        # spark.sql("insert overwrite table " +database+".t_53  select * from t_53")

        # 指数54 近三个月三级风险裁判文书数量

        spark.sql(
            "select * from zgcpwsw where sentence_date>= '" + threeMonthAgo + "' and sentence_date<='" + givenDate + "'  "
                                                                                                                     "and instr(ju_proc,'一审')>0 "
                                                                                                                     "and (caseout_come='原告胜诉' or caseout_come='部分胜诉' or "
                                                                                                                     "caseout_come='起诉方 胜诉' or caseout_come='起诉方 部分胜诉' or "
                                                                                                                     "caseout_come='原告 胜诉' or caseout_come='原告 部分胜诉' or "
                                                                                                                     "caseout_come='起诉方 部分胜诉' or caseout_come='上诉人胜诉' "
                                                                                                                     "or caseout_come='上诉人 部分胜诉' or caseout_come='上诉人 胜诉'"
                                                                                                                     " or caseout_come='申诉人 胜诉' or caseout_come='原告人 部分胜诉') and "
                                                                                                                     "(action_cause='股权转让纠纷' or action_cause='股东资格确认纠纷' "
                                                                                                                     "or action_cause='股东出资纠纷' or action_cause='劳动争议纠纷' "
                                                                                                                     "or action_cause='公司解散纠纷' or action_cause='申请破产清算' ) ") \
            .createOrReplaceTempView("temp_54")

        spark.sql("select c.company_name,if(t54count is null,0,t54count) t54count from companyList c left join "
                  "(select l.company_name,count(*) t54count from companyList l join temp_54 r on "
                  "l.company_name=r.def_litigant group by l.company_name) lr on c.company_name=lr.company_name ") \
            .createOrReplaceTempView("t_54")

        spark.sql("create table " + database + ".t_54 as select * from t_54")
        # spark.sql("insert overwrite table " +database+".t_54  select * from t_54")

        # 指数55 近两年其他风险裁判文书数量,修改or为and

        spark.sql(
            "select * from zgcpwsw where sentence_date>= '" + twoYearAgo + "' and sentence_date<='" + givenDate + "'  "
                                                                                                                  "and instr(ju_proc,'一审')>0 "
                                                                                                                  "and (caseout_come='原告胜诉' or caseout_come='部分胜诉' or "
                                                                                                                  "caseout_come='起诉方 胜诉' or caseout_come='起诉方 部分胜诉' or "
                                                                                                                  "caseout_come='原告 胜诉' or caseout_come='原告 部分胜诉' "
                                                                                                                  "or caseout_come='上诉人胜诉' "
                                                                                                                  "or caseout_come='上诉人 部分胜诉' or caseout_come='上诉人 胜诉'"
                                                                                                                  " or caseout_come='申诉人 胜诉' or caseout_come='原告人 部分胜诉') and "
                                                                                                                  "( action_cause !='执行' and action_cause !='民间借贷纠纷' and action_cause !='买卖合同纠纷' "
                                                                                                                  "and action_cause !='金融借款合同纠纷' and action_cause != '借款合同纠纷' "
                                                                                                                  "and action_cause !='小额借款合同纠纷' and action_cause != '企业借贷纠纷' "
                                                                                                                  "and action_cause  !='借款纠纷' and action_cause !='借贷纠纷' and action_cause !='债务纠纷' "
                                                                                                                  "and action_cause !='买卖合同货款纠纷' and action_cause !='买卖纠纷' and "
                                                                                                                  "action_cause !='劳动争议' and action_cause !='建设工程施工合同纠纷' and "
                                                                                                                  "action_cause !='追索劳动报酬纠纷' "
                                                                                                                  "and action_cause !='劳动合同纠纷' and action_cause !='融资租赁合同纠纷' "
                                                                                                                  "and action_cause !='担保物权纠纷' and action_cause !='抵押合同纠纷' "
                                                                                                                  "and action_cause !='金融不良债权追偿纠纷' and action_cause !='欠款纠纷' "
                                                                                                                  "and action_cause !='担保合同纠纷' and action_cause !='追索劳动报酬及经济补偿金纠纷' and "
                                                                                                                  "action_cause !='股权转让纠纷' and action_cause !='股东资格确认纠纷' "
                                                                                                                  "and action_cause !='股东出资纠纷' and action_cause !='劳动争议纠纷' "
                                                                                                                  "and action_cause !='公司解散纠纷' and action_cause !='申请破产清算' ) ") \
            .createOrReplaceTempView("temp_55")

        spark.sql("select c.company_name,if(t55count is null,0,t55count) t55count from companyList c left join "
                  "(select l.company_name,count(*) t55count from companyList l join temp_55 r on "
                  "l.company_name=r.def_litigant group by l.company_name) lr on c.company_name=lr.company_name ") \
            .createOrReplaceTempView("t_55")

        spark.sql("create table " + database + ".t_55 as select * from t_55")
        # spark.sql("insert overwrite table " +database+".t_55  select * from t_55")

        # 指数56 近一年其他风险裁判文书数量

        spark.sql(
            "select * from zgcpwsw where sentence_date>= '" + oneYearAgo + "' and sentence_date<='" + givenDate + "'  "
                                                                                                                  "and instr(ju_proc,'一审')>0 "
                                                                                                                  "and (caseout_come='原告胜诉' or caseout_come='部分胜诉' or "
                                                                                                                  "caseout_come='起诉方 胜诉' or caseout_come='起诉方 部分胜诉' or "
                                                                                                                  "caseout_come='原告 胜诉' or caseout_come='原告 部分胜诉' "
                                                                                                                  "or caseout_come='上诉人胜诉' "
                                                                                                                  "or caseout_come='上诉人 部分胜诉' or caseout_come='上诉人 胜诉'"
                                                                                                                  " or caseout_come='申诉人 胜诉' or caseout_come='原告人 部分胜诉') and "
                                                                                                                  "( action_cause !='执行' and action_cause !='民间借贷纠纷' and action_cause !='买卖合同纠纷' "
                                                                                                                  "and action_cause !='金融借款合同纠纷' and action_cause != '借款合同纠纷' "
                                                                                                                  "and action_cause !='小额借款合同纠纷' and action_cause != '企业借贷纠纷' "
                                                                                                                  "and action_cause  !='借款纠纷' and action_cause !='借贷纠纷' and action_cause !='债务纠纷' "
                                                                                                                  "and action_cause !='买卖合同货款纠纷' and action_cause !='买卖纠纷' and "
                                                                                                                  "action_cause !='劳动争议' and action_cause !='建设工程施工合同纠纷' and "
                                                                                                                  "action_cause !='追索劳动报酬纠纷' "
                                                                                                                  "and action_cause !='劳动合同纠纷' and action_cause !='融资租赁合同纠纷' "
                                                                                                                  "and action_cause !='担保物权纠纷' and action_cause !='抵押合同纠纷' "
                                                                                                                  "and action_cause !='金融不良债权追偿纠纷' and action_cause !='欠款纠纷' "
                                                                                                                  "and action_cause !='担保合同纠纷' and action_cause !='追索劳动报酬及经济补偿金纠纷' and "
                                                                                                                  "action_cause !='股权转让纠纷' and action_cause !='股东资格确认纠纷' "
                                                                                                                  "and action_cause !='股东出资纠纷' and action_cause !='劳动争议纠纷' "
                                                                                                                  "and action_cause !='公司解散纠纷' and action_cause !='申请破产清算' ) ") \
            .createOrReplaceTempView("temp_56")

        spark.sql("select c.company_name,if(t56count is null,0,t56count) t56count from companyList c left join "
                  "(select l.company_name,count(*) t56count from companyList l join temp_56 r on "
                  "l.company_name=r.def_litigant group by l.company_name) lr on c.company_name=lr.company_name ") \
            .createOrReplaceTempView("t_56")

        spark.sql("create table " + database + ".t_56 as select * from t_56")
        # spark.sql("insert overwrite table " +database+".t_56  select * from t_56")

        # 指数57 近半年其他风险裁判文书数量

        spark.sql(
            "select * from zgcpwsw where sentence_date>= '" + halfyearAgo + "' and sentence_date<='" + givenDate + "'  "
                                                                                                                   "and instr(ju_proc,'一审')>0 "
                                                                                                                   "and (caseout_come='原告胜诉' or caseout_come='部分胜诉' or "
                                                                                                                   "caseout_come='起诉方 胜诉' or caseout_come='起诉方 部分胜诉' or "
                                                                                                                   "caseout_come='原告 胜诉' or caseout_come='原告 部分胜诉' "
                                                                                                                   "or caseout_come='上诉人胜诉' "
                                                                                                                   "or caseout_come='上诉人 部分胜诉' or caseout_come='上诉人 胜诉'"
                                                                                                                   " or caseout_come='申诉人 胜诉' or caseout_come='原告人 部分胜诉') and "
                                                                                                                   "( action_cause !='执行' and action_cause !='民间借贷纠纷' and action_cause !='买卖合同纠纷' "
                                                                                                                   "and action_cause !='金融借款合同纠纷' and action_cause != '借款合同纠纷' "
                                                                                                                   "and action_cause !='小额借款合同纠纷' and action_cause != '企业借贷纠纷' "
                                                                                                                   "and action_cause  !='借款纠纷' and action_cause !='借贷纠纷' and action_cause !='债务纠纷' "
                                                                                                                   "and action_cause !='买卖合同货款纠纷' and action_cause !='买卖纠纷' and "
                                                                                                                   "action_cause !='劳动争议' and action_cause !='建设工程施工合同纠纷' and "
                                                                                                                   "action_cause !='追索劳动报酬纠纷' "
                                                                                                                   "and action_cause !='劳动合同纠纷' and action_cause !='融资租赁合同纠纷' "
                                                                                                                   "and action_cause !='担保物权纠纷' and action_cause !='抵押合同纠纷' "
                                                                                                                   "and action_cause !='金融不良债权追偿纠纷' and action_cause !='欠款纠纷' "
                                                                                                                   "and action_cause !='担保合同纠纷' and action_cause !='追索劳动报酬及经济补偿金纠纷' and "
                                                                                                                   "action_cause !='股权转让纠纷' and action_cause !='股东资格确认纠纷' "
                                                                                                                   "and action_cause !='股东出资纠纷' and action_cause !='劳动争议纠纷' "
                                                                                                                   "and action_cause !='公司解散纠纷' and action_cause !='申请破产清算' ) ") \
            .createOrReplaceTempView("temp_57")

        spark.sql("select c.company_name,if(t57count is null,0,t57count) t57count from companyList c left join "
                  "(select l.company_name,count(*) t57count from companyList l join temp_57 r on "
                  "l.company_name=r.def_litigant group by l.company_name) lr on c.company_name=lr.company_name ") \
            .createOrReplaceTempView("t_57")

        spark.sql("create table " + database + ".t_57 as select * from t_57")
        # spark.sql("insert overwrite table " +database+".t_57  select * from t_57")

        # 指数58 近三个月其他风险裁判文书数量

        spark.sql(
            "select * from zgcpwsw where sentence_date>= '" + threeMonthAgo + "' and sentence_date<='" + givenDate + "'  "
                                                                                                                     "and instr(ju_proc,'一审')>0 "
                                                                                                                     "and (caseout_come='原告胜诉' or caseout_come='部分胜诉' or "
                                                                                                                     "caseout_come='起诉方 胜诉' or caseout_come='起诉方 部分胜诉' or "
                                                                                                                     "caseout_come='原告 胜诉' or caseout_come='原告 部分胜诉' "
                                                                                                                     "or caseout_come='上诉人胜诉' "
                                                                                                                     "or caseout_come='上诉人 部分胜诉' or caseout_come='上诉人 胜诉'"
                                                                                                                     " or caseout_come='申诉人 胜诉' or caseout_come='原告人 部分胜诉') and "
                                                                                                                     "( action_cause !='执行' and action_cause !='民间借贷纠纷' and action_cause !='买卖合同纠纷' "
                                                                                                                     "and action_cause !='金融借款合同纠纷' and action_cause != '借款合同纠纷' "
                                                                                                                     "and action_cause !='小额借款合同纠纷' and action_cause != '企业借贷纠纷' "
                                                                                                                     "and action_cause  !='借款纠纷' and action_cause !='借贷纠纷' and action_cause !='债务纠纷' "
                                                                                                                     "and action_cause !='买卖合同货款纠纷' and action_cause !='买卖纠纷' and "
                                                                                                                     "action_cause !='劳动争议' and action_cause !='建设工程施工合同纠纷' and "
                                                                                                                     "action_cause !='追索劳动报酬纠纷' "
                                                                                                                     "and action_cause !='劳动合同纠纷' and action_cause !='融资租赁合同纠纷' "
                                                                                                                     "and action_cause !='担保物权纠纷' and action_cause !='抵押合同纠纷' "
                                                                                                                     "and action_cause !='金融不良债权追偿纠纷' and action_cause !='欠款纠纷' "
                                                                                                                     "and action_cause !='担保合同纠纷' and action_cause !='追索劳动报酬及经济补偿金纠纷' and "
                                                                                                                     "action_cause !='股权转让纠纷' and action_cause !='股东资格确认纠纷' "
                                                                                                                     "and action_cause !='股东出资纠纷' and action_cause !='劳动争议纠纷' "
                                                                                                                     "and action_cause !='公司解散纠纷' and action_cause !='申请破产清算' ) ") \
            .createOrReplaceTempView("temp_58")

        spark.sql("select c.company_name,if(t58count is null,0,t58count) t58count from companyList c left join "
                  "(select l.company_name,count(*) t58count from companyList l join temp_58 r on "
                  "l.company_name=r.def_litigant group by l.company_name) lr on c.company_name=lr.company_name ") \
            .createOrReplaceTempView("t_58")

        spark.sql("create table " + database + ".t_58 as select * from t_58")
        # spark.sql("insert overwrite table " +database+".t_58  select * from t_58")


        spark.sql("select distinct bbd_qyxx_id,bbd_xgxx_id,case_create_time from dw.dishonesty where dt='"+dishonesty_dt+"' and "
                  "bbd_qyxx_id is not null").createOrReplaceTempView("dishonesty")
        # 指数59 目标公司近两年失信次数
        spark.sql("select c.company_name,if(t59count is null,0,t59count) t59count from companyList c left join "
                  "(select l.company_name,count(*) t59count from companylist_id l join (select * from dishonesty where "
                  "case_create_time>='" + twoYearAgo + "' and case_create_time<='" + givenDate + "') r on l.bbd_qyxx_id=r.bbd_qyxx_id "
                                                                                                 "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_59")
        spark.sql("create table " + database + ".t_59 as select * from t_59")
        # spark.sql("insert overwrite table " +database+".t_59  select * from t_59")

        # 指数60 目标公司近一年失信次数
        spark.sql("select c.company_name,if(t60count is null,0,t60count) t60count from companyList c left join "
                  "(select l.company_name,count(*) t60count from companylist_id l join (select * from dishonesty where "
                  "case_create_time>='" + oneYearAgo + "' and case_create_time<='" + givenDate + "') r on l.bbd_qyxx_id=r.bbd_qyxx_id "
                                                                                                 "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_60")

        spark.sql("create table " + database + ".t_60 as select * from t_60")
        # spark.sql("insert overwrite table " +database+".t_60  select * from t_60")

        # 指数61 目标公司近半年失信次数
        spark.sql("select c.company_name,if(t61count is null,0,t61count) t61count from companyList c left join "
                  "(select l.company_name,count(*) t61count from companylist_id l join (select * from dishonesty where "
                  "case_create_time>='" + halfyearAgo + "' and case_create_time<='" + givenDate + "') r on l.bbd_qyxx_id=r.bbd_qyxx_id "
                                                                                                  "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_61")

        spark.sql("create table " + database + ".t_61 as select * from t_61")
        # spark.sql("insert overwrite table " +database+".t_61  select * from t_61")

        # 指数62 目标公司近三个月失信次数
        spark.sql("select c.company_name,if(t62count is null,0,t62count) t62count from companyList c left join "
                  "(select l.company_name,count(*) t62count from companylist_id l join (select * from dishonesty where "
                  "case_create_time>='" + threeMonthAgo + "' and case_create_time<='" + givenDate + "') r on l.bbd_qyxx_id=r.bbd_qyxx_id "
                                                                                                    "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_62")

        spark.sql("create table " + database + ".t_62 as select * from t_62")
        # spark.sql("insert overwrite table " +database+".t_62  select * from t_62")

        spark.sql("select company_name,case_create_time,exec_subject from dw.zhixing where dt='"+zhixing_dt+"' "
                  "and company_name is not null").createOrReplaceTempView("zhixing")

        # 指数63 目标公司近两年被执行次数
        spark.sql("select c.company_name,if(t63count is null,0,t63count) t63count from companyList c left join "
                  "(select l.company_name,count(*) t63count from companyList l join (select * from zhixing where "
                  "case_create_time >='" + twoYearAgo + "' and case_create_time<='" + givenDate + "') r on l.company_name = r.company_name "
                                                                                                  "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_63")
        spark.sql("create table " + database + ".t_63 as select * from t_63")
        # spark.sql("insert overwrite table " +database+".t_63  select * from t_63")

        # 指标64 目标公司近一年被执行次数
        spark.sql("select c.company_name,if(t64count is null,0,t64count) t64count from companyList c left join "
                  "(select l.company_name,count(*) t64count from companyList l join (select * from zhixing where "
                  "case_create_time >='" + oneYearAgo + "' and case_create_time<='" + givenDate + "') r on l.company_name = r.company_name "
                                                                                                  "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_64")
        spark.sql("create table " + database + ".t_64 as select * from t_64")
        # spark.sql("insert overwrite table " +database+".t_64  select * from t_64")
        # 指标65 目标公司近半年被执行次数
        spark.sql("select c.company_name,if(t65count is null,0,t65count) t65count from companyList c left join "
                  "(select l.company_name,count(*) t65count from companyList l join (select * from zhixing where "
                  "case_create_time >='" + halfyearAgo + "' and case_create_time<='" + givenDate + "') r on l.company_name = r.company_name "
                                                                                                   "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_65")
        spark.sql("create table " + database + ".t_65 as select * from t_65")
        # spark.sql("insert overwrite table " +database+".t_65  select * from t_65")

        # 指标66 目标公司近三个月被执行次数
        spark.sql("select c.company_name,if(t66count is null,0,t66count) t66count from companyList c left join "
                  "(select l.company_name,count(*) t66count from companyList l join (select * from zhixing where "
                  "case_create_time >='" + threeMonthAgo + "' and case_create_time<='" + givenDate + "') r on l.company_name = r.company_name "
                                                                                                     "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_66")

        spark.sql("create table " + database + ".t_66 as select * from t_66")
        # spark.sql("insert overwrite table " +database+".t_66  select * from t_66")

        # 指标67 目标公司近两年最大被执行标的金额
        spark.sql("select c.company_name,if(t67count is null,0,t67count) t67count from companyList c left join "
                  "(select l.company_name,max(exec_subject) t67count from companyList l join (select * from zhixing where "
                  "case_create_time >='" + twoYearAgo + "' and case_create_time<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                                  "group by l.company_name) lr on c.company_name=lr.company_name ").createOrReplaceTempView(
            "t_67")
        spark.sql("create table " + database + ".t_67 as select * from t_67")
        # spark.sql("insert overwrite table " +database+".t_67  select * from t_67")

        # 指标68 目标公司近两年最小被执行标的金额

        spark.sql("select c.company_name,if(t68count is null,0,t68count) t68count from companyList c left join "
                  "(select l.company_name,min(exec_subject) t68count from companyList l join (select * from zhixing where "
                  "case_create_time >='" + twoYearAgo + "' and case_create_time<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                                  "group by l.company_name) lr on c.company_name=lr.company_name ").createOrReplaceTempView(
            "t_68")
        spark.sql("create table " + database + ".t_68 as select * from t_68")
        # spark.sql("insert overwrite table " +database+".t_68  select * from t_68")

        # 指标69 目标公司近两年被执行标的总金额
        spark.sql("select c.company_name,if(t69count is null,0,t69count) t69count from companyList c left join "
                  "(select l.company_name,sum(exec_subject) t69count from companyList l join (select * from zhixing where "
                  "case_create_time >='" + twoYearAgo + "' and case_create_time<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                                  "group by l.company_name) lr on c.company_name=lr.company_name ").createOrReplaceTempView(
            "t_69")
        spark.sql("create table " + database + ".t_69 as select * from t_69")
        # spark.sql("insert overwrite table " +database+".t_69  select * from t_69")

        # 指标70 目标公司近一年被执行标的总金额
        spark.sql("select c.company_name,if(t70count is null,0,t70count) t70count from companyList c left join "
                  "(select l.company_name,sum(exec_subject) t70count from companyList l join (select * from zhixing where "
                  "case_create_time >='" + oneYearAgo + "' and case_create_time<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                                  "group by l.company_name) lr on c.company_name=lr.company_name ").createOrReplaceTempView(
            "t_70")
        spark.sql("create table " + database + ".t_70 as select * from t_70")
        # spark.sql("insert overwrite table " +database+".t_70  select * from t_70")

        # 指标71 目标公司近半年被执行标的总金额
        spark.sql("select c.company_name,if(t71count is null,0,t71count) t71count from companyList c left join "
                  "(select l.company_name,sum(exec_subject) t71count from companyList l join (select * from zhixing where "
                  "case_create_time >='" + halfyearAgo + "' and case_create_time<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                                   "group by l.company_name) lr on c.company_name=lr.company_name ").createOrReplaceTempView(
            "t_71")
        spark.sql("create table " + database + ".t_71 as select * from t_71")
        # spark.sql("insert overwrite table " +database+".t_71  select * from t_71")
        #
        # 指标72 目标公司近三个月被执行标的总金额
        spark.sql("select c.company_name,if(t72count is null,0,t72count) t72count from companyList c left join "
                  "(select l.company_name,sum(exec_subject) t72count from companyList l join (select * from zhixing where "
                  "case_create_time >='" + threeMonthAgo + "' and case_create_time<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                                     "group by l.company_name) lr on c.company_name=lr.company_name ").createOrReplaceTempView(
            "t_72")
        spark.sql("create table " + database + ".t_72 as select * from t_72")
        # spark.sql("insert overwrite table " +database+".t_72  select * from t_72")

        # 指标73 目标公司历史上是否有欠税记录
        spark.sql(
            "select distinct bbd_qyxx_id,bbd_xgxx_id from dw.qyxg_qyqs where dt='"+qyxg_qyqs_dt+"' and bbd_qyxx_id is not null") \
            .createOrReplaceTempView("qyqs")
        spark.sql("select c.company_name,if(t73count is null,0,t73count) t73count from companyList c left join "
                  "(select company_name,count(*) t73count from companylist_id l join qyqs r on l.bbd_qyxx_id=r.bbd_qyxx_id "
                  "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView("t_73")
        spark.sql("create table " + database + ".t_73 as select * from t_73")
        # spark.sql("insert overwrite table " +database+".t_73  select * from t_73")


        spark.sql(
            "select distinct bbd_qyxx_id,bbd_xgxx_id,trial_date from dw.ktgg where dt='"+ktgg_dt+"' and bbd_qyxx_id is not null") \
            .createOrReplaceTempView("ktgg")
        # 指标74 目标公司近两年开庭公告数
        spark.sql("select c.company_name,if(t74count is null,0,t74count) t74count from companyList c left join "
                  "(select l.company_name,count(*) t74count from companylist_id l join (select * from ktgg where trial_date>='" + twoYearAgo + "' "
                                                                                                                                               "and trial_date<='" + givenDate + "') r on l.bbd_qyxx_id=r.bbd_qyxx_id group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_74")
        spark.sql("create table " + database + ".t_74 as select * from t_74")
        # spark.sql("insert overwrite table " +database+".t_74  select * from t_74")

        # 指标75 目标公司近一年开庭公告数
        spark.sql("select c.company_name,if(t75count is null,0,t75count) t75count from companyList c left join "
                  "(select l.company_name,count(*) t75count from companylist_id l join (select * from ktgg where trial_date>='" + oneYearAgo + "' "
                                                                                                                                               "and trial_date<='" + givenDate + "') r on l.bbd_qyxx_id=r.bbd_qyxx_id group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_75")
        spark.sql("create table " + database + ".t_75 as select * from t_75")
        # spark.sql("insert overwrite table " +database+".t_75  select * from t_75")

        # 指标76 目标公司近半年开庭公告数
        spark.sql("select c.company_name,if(t76count is null,0,t76count) t76count from companyList c left join "
                  "(select l.company_name,count(*) t76count from companylist_id l join (select * from ktgg where trial_date>='" + halfyearAgo + "' "
                                                                                                                                                "and trial_date<='" + givenDate + "') r on l.bbd_qyxx_id=r.bbd_qyxx_id group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_76")
        spark.sql("create table " + database + ".t_76 as select * from t_76")
        # spark.sql("insert overwrite table " +database+".t_76  select * from t_76")

        # 指标77 目标公司近三个月年开庭公告数
        spark.sql("select c.company_name,if(t77count is null,0,t77count) t77count from companyList c left join "
                  "(select l.company_name,count(*) t77count from companylist_id l join (select * from ktgg where trial_date>='" + threeMonthAgo + "' "
                                                                                                                                                  "and trial_date<='" + givenDate + "') r on l.bbd_qyxx_id=r.bbd_qyxx_id group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_77")
        spark.sql("create table " + database + ".t_77 as select * from t_77")
        # spark.sql("insert overwrite table " +database+".t_77  select * from t_77")


        # 指标78 一度关联非自然人诉讼数   ，裁判文书的def_litigant写成company_name了
        spark.sql("select c.company_name,if(t78count is null,0,t78count) t78count from companyList c left join "
                  "(select lr.company_name,count(*) t78count from (select * from zgcpwsw where sentence_date<='" + givenDate + "') "
                                                                                                                               "z join (select l.company_name,tname from companyList l join (select company_name,tname from "
                                                                                                                               "(select distinct company_name,source_name tname from flat_relation where "
                                                                                                                               "source_degree='1' and sourceisperson='0') union distinct "
                                                                                                                               "(select distinct company_name,destination_name tname from flat_relation where "
                                                                                                                               "destination_degree='1')) r on l.company_name=r.company_name) lr on z.def_litigant=lr.tname group by "
                                                                                                                               "lr.company_name) lrz on c.company_name=lrz.company_name") \
            .createOrReplaceTempView("t_78")

        spark.sql("create table " + database + ".t_78 as select * from t_78")
        # spark.sql("insert overwrite table " +database+".t_78  select * from t_78")

        # 指标79 二度关联非自然人诉讼数
        spark.sql("select c.company_name,if(t79count is null,0,t79count) t79count from companyList c left join "
                  "(select lr.company_name,count(*) t79count from (select * from zgcpwsw where sentence_date<='" + givenDate + "') "
                                                                                                                               "z join (select l.company_name,tname from companyList l join (select company_name,tname from "
                                                                                                                               "(select distinct company_name,source_name tname from flat_relation where "
                                                                                                                               "source_degree='2' and sourceisperson='0') union distinct "
                                                                                                                               "(select distinct company_name,destination_name tname from flat_relation where "
                                                                                                                               "destination_degree='2')) r on l.company_name=r.company_name) lr on z.def_litigant=lr.tname group by "
                                                                                                                               "lr.company_name) lrz on c.company_name=lrz.company_name") \
            .createOrReplaceTempView("t_79")

        spark.sql("create table " + database + ".t_79 as select * from t_79")
        # spark.sql("insert overwrite table " +database+".t_79  select * from t_79")

        # 指标80 1度关联方中黑名单企业数量（出度）
        spark.sql("select company_name from new_black_list where sentence_date>='" + twoYearAgo + "' and "
                                                                                                  "sentence_date<='" + givenDate + "' ").createOrReplaceTempView(
            "temp_80")

        spark.sql("select c.company_name,if(t80count is null,0,t80count) t80count from companyList c left join "
                  "(select lr.company_name,count(*) t80count from (select distinct l.company_name,r.destination_name from "
                  "companyList l join (select  source_name,destination_name from flat_relation where isinvest='1' and "
                  "sourceisperson='0') r on l.company_name=r.source_name) lr left semi join temp_80 t on lr.destination_name "
                  "= t.company_name group by lr.company_name ) lrt on c.company_name=lrt.company_name ") \
            .createOrReplaceTempView("t_80")

        spark.sql("create table " + database + ".t_80 as select * from t_80")
        # spark.sql("insert overwrite table " +database+".t_80  select * from t_80")

        # 指标81 （过去累积）1度关联方中黑名单企业数量（出度）
        spark.sql("select company_name from new_black_list where sentence_date<='" + givenDate + "'") \
            .createOrReplaceTempView("temp_81")

        spark.sql("select c.company_name,if(t81count is null,0,t81count) t81count from companyList c left join "
                  "(select lr.company_name,count(*) t81count from (select distinct l.company_name,r.destination_name from "
                  "companyList l join (select  source_name,destination_name from flat_relation where isinvest='1' and "
                  "sourceisperson='0') r on l.company_name=r.source_name) lr left semi join temp_81 t on lr.destination_name "
                  "= t.company_name group by lr.company_name ) lrt on c.company_name=lrt.company_name ") \
            .createOrReplaceTempView("t_81")

        spark.sql("create table " + database + ".t_81 as select * from t_81")
        # spark.sql("insert overwrite table " +database+".t_81  select * from t_81")

        # 指标82 1度关联方中黑名单企业数量（入度）  改了：source_name和destination_name写反了
        spark.sql("select company_name from new_black_list where sentence_date>='" + twoYearAgo + "' and "
                                                                                                  "sentence_date<='" + givenDate + "'").createOrReplaceTempView(
            "temp_82")

        spark.sql("select c.company_name,if(t82count is null,0,t82count) t82count from companyList c left join "
                  "(select lr.company_name,count(*) t82count from (select distinct l.company_name,r.source_name from "
                  "companyList l join (select  source_name,destination_name from flat_relation where isinvest='1' and "
                  "sourceisperson='0') r on l.company_name=r.destination_name) lr left semi join temp_82 t on lr.source_name "
                  "= t.company_name group by lr.company_name ) lrt on c.company_name=lrt.company_name ") \
            .createOrReplaceTempView("t_82")

        spark.sql("create table " + database + ".t_82 as select * from t_82")
        # spark.sql("insert overwrite table " +database+".t_82  select * from t_82")

        # 指标83 （过去累积）度关联方中黑名单企业数量（入度） 改了：source_name和destination_name写反了,时间原来写的2015-05-30
        spark.sql("select company_name from new_black_list where sentence_date<='" + givenDate + "' ") \
            .createOrReplaceTempView("temp_83")

        spark.sql("select c.company_name,if(t83count is null,0,t83count) t83count from companyList c left join "
                  "(select lr.company_name,count(*) t83count from (select distinct l.company_name,r.source_name from "
                  "companyList l join (select  source_name,destination_name from flat_relation where isinvest='1' and "
                  "sourceisperson='0') r on l.company_name=r.destination_name) lr left semi join temp_83 t on lr.source_name "
                  "= t.company_name group by lr.company_name ) lrt on c.company_name=lrt.company_name ") \
            .createOrReplaceTempView("t_83")

        spark.sql("create table " + database + ".t_83 as select * from t_83")
        # spark.sql("insert overwrite table " +database+".t_83  select * from t_83")

        # 指标84 2度关联方中黑名单企业数量

        spark.sql("select c.company_name,if(t84count is null,0,t84count) t84count from companyList c left join "
                  "(select lr.company_name,count(*) t84count from (select l.company_name,r.tname from companyList l "
                  "join (select company_name,tname from "
                  "(select distinct company_name,source_name tname from flat_relation where "
                  "source_degree='2' and sourceisperson='0') union distinct "
                  "(select distinct company_name,destination_name tname from flat_relation where "
                  "destination_degree='2')) r on l.company_name=r.company_name ) lr left semi join temp_82 t "
                  "on lr.tname=t.company_name group by lr.company_name) lrt on c.company_name=lrt.company_name") \
            .createOrReplaceTempView("t_84")

        spark.sql("create table " + database + ".t_84 as select * from t_84")
        # spark.sql("insert overwrite table " +database+".t_84  select * from t_84")

        # 指标85 一度关联黑名单与目标公司共同自然人数（MAX） 用到了temp82

        # 筛选目标公司的一度关联方黑名单
        spark.sql("select lr.company_name,lr.tname from (select l.company_name,tname from companyList l join "
                  "(select company_name,tname from "
                  "(select distinct company_name,source_name tname from flat_relation where "
                  "source_degree='1' and sourceisperson='0') union distinct "
                  "(select distinct company_name,destination_name tname from flat_relation where "
                  "destination_degree='1')) r on l.company_name=r.company_name) lr left semi join temp_82 t on lr.tname "
                  "= t.company_name").createOrReplaceTempView("temp85_1")

        # 计算所有的目标公司和一度黑名单的自然人(自然人包括投资自然人和任职自然人)
        spark.sql(
            "select distinct r.company_name,r.source_name from (select * from (select company_name from temp85_1) "
            "union distinct (select tname company_name from temp85_1)) l join "
            "( select company_name,source_name from "
            "(select company_name,source_name from flat_relation where sourceisperson='1' and source_degree='1' "
            "and isinvest='1' and company_name=destination_name) "
            "union distinct "
            "(select destination_name company_name,source_name from flat_relation where instr(pos,'董事')>0 or "
            "instr(pos,'监事')>0 or instr(pos,'经理')>0 or instr(pos,'总裁')>0 or instr(pos,'总监')>0 "
            "or instr(pos,'行长')>0 or instr(pos,'财务负责人')>0 or instr(pos,'财务总监')>0 or instr(pos,'厂长')>0 ) "
            ") r on l.company_name=r.company_name").createOrReplaceTempView("temp85_2")

        # 目标公司和一度关联黑名单之间的有几个共同自然人,temp85_3算的是有共同自然人的，没有的没包含在里面
        spark.sql("select lr.company_name,lr.tname,count(*) t85_temp from (select l.company_name,r.source_name sn1,"
                  "l.tname from temp85_1 l join temp85_2 r on l.company_name=r.company_name) lr join temp85_2 m on lr.tname"
                  "=m.company_name and lr.sn1=m.source_name group by lr.company_name,lr.tname") \
            .createOrReplaceTempView("temp85_3")

        spark.sql("select c.company_name,if(t85count is null,0,t85count) t85count from companyList c left join "
                  "(select company_name,max(t85_temp) t85count from temp85_3 group by company_name) l "
                  "on c.company_name=l.company_name").createOrReplaceTempView("t_85")

        spark.sql("create table " + database + ".t_85 as select * from t_85")
        # spark.sql("insert overwrite table " +database+".t_85  select * from t_85")

        # 指标86 一度关联黑名单与目标公司共同自然人数（MIN）
        spark.sql("select l.company_name,l.tname,if(t85_temp is null,0,t85_temp) t86_temp  from temp85_1 l left join "
                  "temp85_3 r on l.company_name=r.company_name and l.tname=r.tname").createOrReplaceTempView("temp86")

        spark.sql("select c.company_name,if(t86count is null,0,t86count) t86count from companyList c left join "
                  "(select company_name,min(t86_temp) t86count from temp86 group by company_name) l "
                  "on c.company_name=l.company_name").createOrReplaceTempView("t_86")
        spark.sql("create table " + database + ".t_86 as select * from t_86")
        # spark.sql("insert overwrite table " +database+".t_86  select * from t_86")

        # 指标141 一度关联黑名单与目标公司共同自然人数之和（SUM）
        spark.sql("select c.company_name,if(t141count is null,0,t141count) t141count from companyList c left join "
                  "(select company_name,sum(t85_temp) t141count from temp85_3 group by company_name) l "
                  "on c.company_name=l.company_name").createOrReplaceTempView("t_141")

        spark.sql("create table " + database + ".t_141 as select * from t_141")
        # spark.sql("insert overwrite table " +database+".t_141  select * from t_141")

        # 指标87 一度关联黑名单与目标公司共同自然人数（MEAN）
        spark.sql("select c.company_name,if(t87count is null,0,t87count) t87count from companyList c left join "
                  "(select l.company_name,round((t87_temp_2/t87_temp_1),2) t87count from (select company_name,count(*) t87_temp_1 "
                  "from temp86 group by company_name) l join (select company_name,sum(t86_temp) t87_temp_2 from temp86 "
                  "group by company_name) r on l.company_name=r.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_87")

        spark.sql("create table " + database + ".t_87 as select * from t_87")
        # spark.sql("insert overwrite table " +database+".t_87  select * from t_87")

        # 指标88 与目标公司有共同自然人数的一度黑名单数
        spark.sql("select c.company_name,if(t88count is null,0,t88count) t88count from companyList c left join "
                  "(select company_name,count(*) t88count from temp85_3 group by company_name) l on "
                  "c.company_name=l.company_name").createOrReplaceTempView("t_88")
        spark.sql("create table " + database + ".t_88 as select * from t_88")
        # spark.sql("insert overwrite table " +database+".t_88  select * from t_88")

        # 指标89 二度关联黑名单与目标公司共同自然人数（MAX）,用到了temp_82
        spark.sql("select lr.company_name,lr.tname from (select l.company_name,tname from companyList l join "
                  "(select company_name,tname from "
                  "(select distinct company_name,source_name tname from flat_relation where "
                  "source_degree='2' and sourceisperson='0') union distinct "
                  "(select distinct company_name,destination_name tname from flat_relation where "
                  "destination_degree='2')) r on l.company_name=r.company_name) lr left semi join temp_82 t on lr.tname "
                  "= t.company_name").createOrReplaceTempView("temp89_1")

        # 计算所有的目标公司和二度黑名单的自然人
        spark.sql(
            "select distinct r.company_name,r.source_name from (select * from (select company_name from temp89_1) "
            "union distinct (select tname company_name from temp89_1)) l join "
            "( select company_name,source_name from "
            "(select company_name,source_name from flat_relation where sourceisperson='1' and source_degree='1' "
            "and isinvest='1' and company_name=destination_name) "
            " union distinct "
            "(select destination_name company_name,source_name from flat_relation where instr(pos,'董事')>0 or "
            "instr(pos,'监事')>0 or instr(pos,'经理')>0 or instr(pos,'总裁')>0 or instr(pos,'总监')>0 "
            "or instr(pos,'行长')>0 or instr(pos,'财务负责人')>0 or instr(pos,'财务总监')>0 or instr(pos,'厂长')>0 )) "
            " r on l.company_name=r.company_name") \
            .createOrReplaceTempView("temp89_2")

        spark.sql("select lr.company_name,lr.tname,count(*) t89_temp from (select l.company_name,r.source_name sn1,"
                  "l.tname from temp89_1 l join temp89_2 r on l.company_name=r.company_name) lr join temp89_2 m on lr.tname"
                  "=m.company_name and lr.sn1=m.source_name group by lr.company_name,lr.tname") \
            .createOrReplaceTempView("temp89_3")

        spark.sql("select c.company_name,if(t89count is null,0,t89count) t89count from companyList c left join "
                  "(select company_name,max(t89_temp) t89count from temp89_3 group by company_name) l "
                  "on c.company_name=l.company_name").createOrReplaceTempView("t_89")

        spark.sql("create table " + database + ".t_89 as select * from t_89")
        # spark.sql("insert overwrite table " +database+".t_89  select * from t_89")

        # 指标90 二度关联黑名单与目标公司共同自然人数（MIN）,要计算到所有的目标公司和一度关联方
        spark.sql("select l.company_name,l.tname,if(t89_temp is null,0,t89_temp) t90_temp  from temp89_1 l left join "
                  "temp89_3 r on l.company_name=r.company_name and l.tname=r.tname").createOrReplaceTempView("temp90")

        spark.sql("select c.company_name,if(t90count is null,0,t90count) t90count from companyList c left join "
                  "(select company_name,min(t90_temp) t90count from temp90 group by company_name) l "
                  "on c.company_name=l.company_name").createOrReplaceTempView("t_90")
        spark.sql("create table " + database + ".t_90 as select * from t_90")
        # spark.sql("insert overwrite table " +database+".t_90  select * from t_90")

        # 指标142 二度关联黑名单与目标公司共同自然人数之和（SUM）
        spark.sql("select c.company_name,if(t142count is null,0,t142count) t142count from companyList c left join "
                  "(select company_name,sum(t89_temp) t142count from temp89_3 group by company_name) l "
                  "on c.company_name=l.company_name").createOrReplaceTempView("t_142")
        spark.sql("create table " + database + ".t_142 as select * from t_142")
        # spark.sql("insert overwrite table " +database+".t_142  select * from t_142")

        # 指标91 二度关联黑名单与目标公司共同自然人数（MEAN）
        spark.sql("select c.company_name,if(t91count is null,0,t91count) t91count from companyList c left join "
                  "(select l.company_name,round((t91_temp_2/t91_temp_1),2) t91count from (select company_name,count(*) t91_temp_1 "
                  "from temp90 group by company_name) l join (select company_name,sum(t90_temp) t91_temp_2 from temp90 "
                  "group by company_name) r on l.company_name=r.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_91")
        spark.sql("create table " + database + ".t_91 as select * from t_91")
        # spark.sql("insert overwrite table " +database+".t_91  select * from t_91")

        # 指标92 与目标公司有共同自然人数的二度黑名单数
        spark.sql("select c.company_name,if(t92count is null,0,t92count) t92count from companyList c left join "
                  "(select company_name,count(*) t92count from temp89_3 group by company_name) l on "
                  "c.company_name=l.company_name").createOrReplaceTempView("t_92")
        spark.sql("create table " + database + ".t_92 as select * from t_92")
        # spark.sql("insert overwrite table " +database+".t_92  select * from t_92")

        # 指标93 一度关联方中吊销企业数量
        spark.sql("select c.company_name,if(t93count is null,0,t93count) t93count from companyList c left join "
                  "(select lr.company_name,count(*) t93count from (select l.company_name,tname from companyList l join "
                  "(select company_name,tname from "
                  "(select distinct company_name,source_name tname from flat_relation where "
                  "source_degree='1' and sourceisperson='0') union distinct "
                  "(select distinct company_name,destination_name tname from flat_relation where "
                  "destination_degree='1')) r on l.company_name=r.company_name ) lr join "
                  "(select company_name,company_enterprise_status from qyxx_basic where instr(company_enterprise_status,'吊销')>0) "
                  "qb on lr.tname=qb.company_name group by lr.company_name) lrt on c.company_name= lrt.company_name") \
            .createOrReplaceTempView("t_93")
        spark.sql("create table " + database + ".t_93 as select * from t_93")
        # spark.sql("insert overwrite table " +database+".t_93  select * from t_93")

        # 指标94 二度关联方中吊销企业数量
        spark.sql("select c.company_name,if(t94count is null,0,t94count) t94count from companyList c left join "
                  "(select lr.company_name,count(*) t94count from (select l.company_name,tname from companyList l join "
                  "(select company_name,tname from "
                  "(select distinct company_name,source_name tname from flat_relation where "
                  "source_degree='2' and sourceisperson='0') union distinct "
                  "(select distinct company_name,destination_name tname from flat_relation where "
                  "destination_degree='2')) r on l.company_name=r.company_name ) lr join "
                  "(select company_name,company_enterprise_status from qyxx_basic where instr(company_enterprise_status,'吊销')>0) "
                  "qb on lr.tname=qb.company_name group by lr.company_name) lrt on c.company_name= lrt.company_name") \
            .createOrReplaceTempView("t_94")
        spark.sql("create table " + database + ".t_94 as select * from t_94")
        # spark.sql("insert overwrite table " +database+".t_94  select * from t_94")

        # 指标95 一度关联方中注销企业数量
        spark.sql("select c.company_name,if(t95count is null,0,t95count) t95count from companyList c left join "
                  "(select lr.company_name,count(*) t95count from (select l.company_name,tname from companyList l join "
                  "(select company_name,tname from "
                  "(select distinct company_name,source_name tname from flat_relation where "
                  "source_degree='1' and sourceisperson='0') union distinct "
                  "(select distinct company_name,destination_name tname from flat_relation where "
                  "destination_degree='1')) r on l.company_name=r.company_name ) lr join "
                  "(select company_name,company_enterprise_status from qyxx_basic where instr(company_enterprise_status,'注销')>0) "
                  "qb on lr.tname=qb.company_name group by lr.company_name) lrt on c.company_name= lrt.company_name") \
            .createOrReplaceTempView("t_95")
        spark.sql("create table " + database + ".t_95 as select * from t_95")
        # spark.sql("insert overwrite table " +database+".t_95  select * from t_95")

        # 指标96 二度关联方中注销企业数量
        spark.sql("select c.company_name,if(t96count is null,0,t96count) t96count from companyList c left join "
                  "(select lr.company_name,count(*) t96count from (select l.company_name,tname from companyList l join "
                  "(select company_name,tname from "
                  "(select distinct company_name,source_name tname from flat_relation where "
                  "source_degree='2' and sourceisperson='0') union distinct "
                  "(select distinct company_name,destination_name tname from flat_relation where "
                  "destination_degree='2')) r on l.company_name=r.company_name ) lr join "
                  "(select company_name,company_enterprise_status from qyxx_basic where instr(company_enterprise_status,'注销')>0) "
                  "qb on lr.tname=qb.company_name group by lr.company_name) lrt on c.company_name= lrt.company_name") \
            .createOrReplaceTempView("t_96")
        spark.sql("create table " + database + ".t_96 as select * from t_96")
        # spark.sql("insert overwrite table " +database+".t_96  select * from t_96")

        # 指标97 目标公司最近一次被列入经营异常距现在天数
        spark.sql(
            "select distinct company_name,bbd_xgxx_id,rank_date from dw.qyxg_jyyc where dt='"+qyxg_jyyc_dt+"' and notice_type='列入' "
            "and rank_date<='" + givenDate + "'").createOrReplaceTempView("jyyc")

        spark.sql("select c.company_name,if(t97count is null,0,t97count) t97count from companyList c left join "
                  "(select l.company_name,min(jyyc_day) t97count from companyList l join (select company_name,"
                  "datediff('" + givenDate + "',rank_date) jyyc_day from jyyc) r on l.company_name=r.company_name "
                                             "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_97")
        spark.sql("create table " + database + ".t_97 as select * from t_97")
        # spark.sql("insert overwrite table " +database+".t_97  select * from t_97")

        # 指标98 目标公司历史被列入经营异常名录的次数

        spark.sql("select c.company_name,if(t98count is null,0,t98count) t98count from companyList c left join "
                  "(select l.company_name,count(*) t98count from companyList l join jyyc r on l.company_name=r.company_name "
                  "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView("t_98")
        spark.sql("create table " + database + ".t_98 as select * from t_98")
        # spark.sql("insert overwrite table " +database+".t_98  select * from t_98")

        # 指标99 目标公司近两年被列入经营异常名录的次数
        spark.sql("select c.company_name,if(t99count is null,0,t99count) t99count from companyList c left join "
                  "(select l.company_name,count(*) t99count from companyList l join (select * from jyyc where "
                  "rank_date>='" + twoYearAgo + "' and rank_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                   "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_99")
        spark.sql("create table " + database + ".t_99 as select * from t_99")
        # spark.sql("insert overwrite table " +database+".t_99  select * from t_99")

        # 指标100 一度关联方历史被列入经营异常名录的次数
        spark.sql("select c.company_name,if(t100count is null,0,t100count) t100count from companyList c left join "
                  "(select lr.company_name,count(*) t100count from (select l.company_name,tname from companyList l join "
                  "(select company_name,tname from "
                  "(select distinct company_name,source_name tname from flat_relation where "
                  "source_degree='1' and sourceisperson='0') union distinct "
                  "(select distinct company_name,destination_name tname from flat_relation where "
                  "destination_degree='1')) r on l.company_name=r.company_name ) lr join jyyc j on "
                  "lr.tname=j.company_name group by lr.company_name) lrj on c.company_name=lrj.company_name") \
            .createOrReplaceTempView("t_100")

        spark.sql("create table " + database + ".t_100 as select * from t_100")
        # spark.sql("insert overwrite table " +database+".t_100  select * from t_100")

        # 指标101 一度关联方近两年被列入经营异常名录的次数
        spark.sql("select c.company_name,if(t101count is null,0,t101count) t101count from companyList c left join "
                  "(select lr.company_name,count(*) t101count from (select l.company_name,tname from companyList l join "
                  "(select company_name,tname from "
                  "(select distinct company_name,source_name tname from flat_relation where "
                  "source_degree='1' and sourceisperson='0') union distinct "
                  "(select distinct company_name,destination_name tname from flat_relation where "
                  "destination_degree='1')) r on l.company_name=r.company_name ) lr join (select * from jyyc where "
                  "rank_date>='" + twoYearAgo + "' and rank_date<='" + givenDate + "') j on "
                                                                                   "lr.tname=j.company_name group by lr.company_name) lrj on c.company_name=lrj.company_name") \
            .createOrReplaceTempView("t_101")

        spark.sql("create table " + database + ".t_101 as select * from t_101")
        # spark.sql("insert overwrite table " +database+".t_101  select * from t_101")

        # 指标102 二度关联方历史被列入经营异常名录的次数
        spark.sql("select c.company_name,if(t102count is null,0,t102count) t102count from companyList c left join "
                  "(select lr.company_name,count(*) t102count from (select l.company_name,tname from companyList l join "
                  "(select company_name,tname from "
                  "(select distinct company_name,source_name tname from flat_relation where "
                  "source_degree='2' and sourceisperson='0') union distinct "
                  "(select distinct company_name,destination_name tname from flat_relation where "
                  "destination_degree='2')) r on l.company_name=r.company_name ) lr join jyyc j on "
                  "lr.tname=j.company_name group by lr.company_name) lrj on c.company_name=lrj.company_name") \
            .createOrReplaceTempView("t_102")
        spark.sql("create table " + database + ".t_102 as select * from t_102")
        # spark.sql("insert overwrite table " +database+".t_102  select * from t_102")

        # 指标103 二度关联方近两年被列入经营异常名录的次数
        spark.sql("select c.company_name,if(t103count is null,0,t103count) t103count from companyList c left join "
                  "(select lr.company_name,count(*) t103count from (select l.company_name,tname from companyList l join "
                  "(select company_name,tname from "
                  "(select distinct company_name,source_name tname from flat_relation where "
                  "source_degree='2' and sourceisperson='0') union distinct "
                  "(select distinct company_name,destination_name tname from flat_relation where "
                  "destination_degree='2')) r on l.company_name=r.company_name ) lr join (select * from jyyc where "
                  "rank_date>='" + twoYearAgo + "' and rank_date<='" + givenDate + "') j on "
                                                                                   "lr.tname=j.company_name group by lr.company_name) lrj on c.company_name=lrj.company_name") \
            .createOrReplaceTempView("t_103")
        spark.sql("create table " + database + ".t_103 as select * from t_103")
        # spark.sql("insert overwrite table " +database+".t_103  select * from t_103")

        # 指标104 1度关联方中小额贷款公司数量
        spark.sql("select c.company_name,if(t104count is null,0,t104count) t104count from companyList c left join "
                  "(select r.company_name,count(*) t104count from companyList l join (select * from (select distinct company_name,"
                  "source_name tname from flat_relation where source_degree='1' and sourceisperson='0' and "
                  "instr(source_name,'小额贷款')>0 ) union distinct "
                  "(select distinct company_name,destination_name tname from flat_relation where "
                  "destination_degree='1' and instr(destination_name,'小额贷款')>0 )) r on l.company_name=r.company_name  "
                  "group by r.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_104")

        spark.sql("create table " + database + ".t_104 as select * from t_104")
        # spark.sql("insert overwrite table " +database+".t_104  select * from t_104")

        # 指标105 2度关联方中小额贷款公司数量

        spark.sql("select c.company_name,if(t105count is null,0,t105count) t105count from companyList c left join "
                  "(select r.company_name,count(*) t105count from companyList l join (select * from (select distinct company_name,"
                  "source_name tname from flat_relation where source_degree='2' and sourceisperson='0' and "
                  "instr(source_name,'小额贷款')>0 ) union distinct (select distinct company_name,destination_name tname "
                  "from flat_relation where destination_degree='2' and instr(destination_name,'小额贷款')>0 )) "
                  " r on l.company_name=r.company_name group by r.company_name) lr on "
                  "c.company_name=lr.company_name").createOrReplaceTempView("t_105")

        spark.sql("create table " + database + ".t_105 as select * from t_105")
        # spark.sql("insert overwrite table " +database+".t_105  select * from t_105")

        # 指标106 目标公司分支机构数量

        spark.sql(
            "select distinct name,company_name from dw.qyxx_fzjg_extend where dt='"+qyxx_fzjg_extend_dt+"' and company_name is not null") \
            .createOrReplaceTempView("fzjg")

        spark.sql("select c.company_name,if(t106count is null,0,t106count) t106count from companyList c left join "
                  "(select l.company_name,count(*) t106count from companyList l join fzjg r on l.company_name=r.company_name "
                  "group by l.company_name) lr on c.company_name=lr.company_name ").createOrReplaceTempView("t_106")
        spark.sql("create table " + database + ".t_106 as select * from t_106")
        # spark.sql("insert overwrite table " +database+".t_106  select * from t_106")

        # 指标107 目标公司注吊销分支机构数量
        spark.sql("select c.company_name,if(t107count is null,0,t107count) t107count from companyList c left join "
                  "(select lr.company_name,count(*) t107count from (select l.company_name,name from companyList l join "
                  "fzjg r on l.company_name=r.company_name ) lr join "
                  "(select company_name,company_enterprise_status from qyxx_basic where instr(company_enterprise_status,'吊销')>0 or "
                  "instr(company_enterprise_status,'注销')>0 ) qb on lr.name=qb.company_name group by lr.company_name) lrt"
                  " on c.company_name=lrt.company_name ").createOrReplaceTempView("t_107")
        spark.sql("create table " + database + ".t_107 as select * from t_107")
        # spark.sql("insert overwrite table " +database+".t_107  select * from t_107")

        # 指标108 目标公司专利数量
        spark.sql("select distinct company_name,bbd_xgxx_id from dw.qyxx_zhuanli where dt='"+qyxx_zhuanli_dt+"' and "
                  "publidate<='" + givenDate + "'and company_name is not null").createOrReplaceTempView("zhuanli")

        spark.sql("select c.company_name,if(t108count is null,0,t108count) t108count from companyList c left join "
                  "(select l.company_name,count(*) t108count from companyList l join zhuanli r on "
                  "l.company_name=r.company_name group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_108")

        spark.sql("create table " + database + ".t_108 as select * from t_108")
        # spark.sql("insert overwrite table " +database+".t_108  select * from t_108")

        # 指标109 目标公司软著数量
        spark.sql("select distinct bbd_qyxx_id,bbd_xgxx_id from dw.rjzzq where dt='"+rjzzq_dt+"' and "
                  "regdate<='" + givenDate + "' and bbd_qyxx_id is not null").createOrReplaceTempView("rjzzq")

        spark.sql("select c.company_name,if(t109count is null,0,t109count) t109count from companyList c left join "
                  "(select l.company_name,count(*) t109count from companylist_id l join rjzzq r on "
                  "l.bbd_qyxx_id=r.bbd_qyxx_id group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_109")
        spark.sql("create table " + database + ".t_109 as select * from t_109")
        # spark.sql("insert overwrite table " +database+".t_109  select * from t_109")

        # 指标110 目标公司商标数量
        spark.sql("select distinct company_name,bbd_xgxx_id from dw.xgxx_shangbiao where dt='"+xgxx_shangbiao_dt+"' and "
                  " company_name is not null").createOrReplaceTempView("shangbiao")

        spark.sql("select c.company_name,if(t110count is null,0,t110count) t110count from companyList c left join "
                  "(select l.company_name,count(*) t110count from companyList l join shangbiao r on "
                  "l.company_name=r.company_name group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_110")
        spark.sql("create table " + database + ".t_110 as select * from t_110")
        # spark.sql("insert overwrite table " +database+".t_110  select * from t_110")

        # #指标111 网络聚集系数
        #
        #
        #
        spark.sql("select distinct company_name,change_date,change_items,content_before_change,content_after_change "
                  "from dw.qyxx_bgxx where dt='"+qyxx_bgxx_dt+"' and change_date<='" + givenDate + "' and company_name is not null") \
            .createOrReplaceTempView("bgxx")
        # 指标112 公司最近一次变更信息距今的天数
        spark.sql("select c.company_name,if(t112count is null,0,t112count) t112count from companyList c left join "
                  "(select l.company_name,min(bg_day) t112count from companyList l join (select company_name,"
                  "datediff('" + givenDate + "',change_date) bg_day from bgxx) r on l.company_name=r.company_name "
                                             "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_112")
        spark.sql("create table " + database + ".t_112 as select * from t_112")
        # spark.sql("insert overwrite table " +database+".t_112  select * from t_112")

        # 指标113 公司最近两年内变更信息数
        spark.sql("select c.company_name,if(t113count is null,0,t113count) t113count from companyList c left join "
                  "(select l.company_name,count(*) t113count from companyList l join (select * from bgxx where "
                  "change_date>='" + twoYearAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                       "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_113")
        spark.sql("create table " + database + ".t_113 as select * from t_113")
        # spark.sql("insert overwrite table " +database+".t_113  select * from t_113")
        # 指标114 公司最近一年内变更信息数
        spark.sql("select c.company_name,if(t114count is null,0,t114count) t114count from companyList c left join "
                  "(select l.company_name,count(*) t114count from companyList l join (select * from bgxx where "
                  "change_date>='" + oneYearAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                       "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_114")
        spark.sql("create table " + database + ".t_114 as select * from t_114")
        # spark.sql("insert overwrite table " +database+".t_114  select * from t_114")

        # 指标115 公司最近半年内变更信息数
        spark.sql("select c.company_name,if(t115count is null,0,t115count) t115count from companyList c left join "
                  "(select l.company_name,count(*) t115count from companyList l join (select * from bgxx where "
                  "change_date>='" + halfyearAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                        "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_115")
        spark.sql("create table " + database + ".t_115 as select * from t_115")
        # spark.sql("insert overwrite table " +database+".t_115  select * from t_115")

        # 指标116 公司最近三个月内变更信息数
        spark.sql("select c.company_name,if(t116count is null,0,t116count) t116count from companyList c left join "
                  "(select l.company_name,count(*) t116count from companyList l join (select * from bgxx where "
                  "change_date>='" + threeMonthAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                          "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_116")
        spark.sql("create table " + database + ".t_116 as select * from t_116")
        # spark.sql("insert overwrite table " +database+".t_116  select * from t_116")


        spark.sql("select company_name,change_date from bgxx where instr(change_items,'法定代表人')>0 and "
                  "instr(change_items,'变更')>0").createOrReplaceTempView("frbg")

        # 指标117 目标公司近两年法定代表人变更次数
        spark.sql("select c.company_name,if(t117count is null,0,t117count) t117count from companyList c left join "
                  "(select l.company_name,count(*) t117count from companyList l join (select * from frbg where "
                  "change_date>='" + twoYearAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                       "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_117")
        spark.sql("create table " + database + ".t_117 as select * from t_117")
        # spark.sql("insert overwrite table " +database+".t_117  select * from t_117")

        # 指标118 目标公司近一年法定代表人变更次数
        spark.sql("select c.company_name,if(t118count is null,0,t118count) t118count from companyList c left join "
                  "(select l.company_name,count(*) t118count from companyList l join (select * from frbg where "
                  "change_date>='" + oneYearAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                       "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_118")
        spark.sql("create table " + database + ".t_118 as select * from t_118")
        # spark.sql("insert overwrite table " +database+".t_118  select * from t_118")

        # 指标119 目标公司近半年法定代表人变更次数
        spark.sql("select c.company_name,if(t119count is null,0,t119count) t119count from companyList c left join "
                  "(select l.company_name,count(*) t119count from companyList l join (select * from frbg where "
                  "change_date>='" + halfyearAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                        "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_119")
        spark.sql("create table " + database + ".t_119 as select * from t_119")
        # spark.sql("insert overwrite table " +database+".t_119  select * from t_119")

        # 指标120 目标公司近三个月法定代表人变更次数
        spark.sql("select c.company_name,if(t120count is null,0,t120count) t120count from companyList c left join "
                  "(select l.company_name,count(*) t120count from companyList l join (select * from frbg where "
                  "change_date>='" + threeMonthAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                          "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_120")
        spark.sql("create table " + database + ".t_120 as select * from t_120")
        # spark.sql("insert overwrite table " +database+".t_120  select * from t_120")


        spark.sql("select company_name,change_date from bgxx where change_items='投资人（投资人）股权变更' or "
                  "change_items='投资人变更（包括出资额、出资方式、出资日期、投资人名称等）变更' "
                  "or change_items='投资人变更（包括出资额、出资方式、出资日期、投资人名称等）' or "
                  "change_items='投资人变更' or change_items='自然人股东' or change_items='股东变更' "
                  "or change_items='股东变更(股权转让)'").createOrReplaceTempView("gqbg")

        # 指标121 目标公司近两年股权变更次数
        spark.sql("select c.company_name,if(t121count is null,0,t121count) t121count from companyList c left join "
                  "(select l.company_name,count(*) t121count from companyList l join (select * from gqbg where "
                  "change_date>='" + twoYearAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                       "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_121")
        spark.sql("create table " + database + ".t_121 as select * from t_121")
        # spark.sql("insert overwrite table " +database+".t_121  select * from t_121")

        # 指标122 目标公司近一年股权变更次数
        spark.sql("select c.company_name,if(t122count is null,0,t122count) t122count from companyList c left join "
                  "(select l.company_name,count(*) t122count from companyList l join (select * from gqbg where "
                  "change_date>='" + oneYearAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                       "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_122")
        spark.sql("create table " + database + ".t_122 as select * from t_122")
        # spark.sql("insert overwrite table " +database+".t_122  select * from t_122")

        # 指标123 目标公司近半年股权变更次数
        spark.sql("select c.company_name,if(t123count is null,0,t123count) t123count from companyList c left join "
                  "(select l.company_name,count(*) t123count from companyList l join (select * from gqbg where "
                  "change_date>='" + halfyearAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                        "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_123")
        spark.sql("create table " + database + ".t_123 as select * from t_123")
        # spark.sql("insert overwrite table " +database+".t_123  select * from t_123")

        # 指标124 目标公司近三个月股权变更次数
        spark.sql("select c.company_name,if(t124count is null,0,t124count) t124count from companyList c left join "
                  "(select l.company_name,count(*) t124count from companyList l join (select * from gqbg where "
                  "change_date>='" + threeMonthAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                          "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_124")
        spark.sql("create table " + database + ".t_124 as select * from t_124")
        # spark.sql("insert overwrite table " +database+".t_124  select * from t_124")


        spark.sql(
            "select company_name,change_date,change_items,content_before_change,content_after_change from bgxx where "
            "instr(change_items,'注册资本')>0 ").createOrReplaceTempView("zczbbg")
        # 指标125 目标公司近两年注册资本变更次数
        spark.sql("select c.company_name,if(t125count is null,0,t125count) t125count from companyList c left join "
                  "(select l.company_name,count(*) t125count from companyList l join (select * from zczbbg where "
                  "change_date>='" + twoYearAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                       "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_125")
        spark.sql("create table " + database + ".t_125 as select * from t_125")
        # spark.sql("insert overwrite table " +database+".t_125  select * from t_125")

        # 指标126 目标公司近一年注册资本变更次数
        spark.sql("select c.company_name,if(t126count is null,0,t126count) t126count from companyList c left join "
                  "(select l.company_name,count(*) t126count from companyList l join (select * from zczbbg where "
                  "change_date>='" + oneYearAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                       "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_126")
        spark.sql("create table " + database + ".t_126 as select * from t_126")
        # spark.sql("insert overwrite table " +database+".t_126  select * from t_126")

        # 指标127 目标公司近半年注册资本变更次数
        spark.sql("select c.company_name,if(t127count is null,0,t127count) t127count from companyList c left join "
                  "(select l.company_name,count(*) t127count from companyList l join (select * from zczbbg where "
                  "change_date>='" + halfyearAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                        "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_127")
        spark.sql("create table " + database + ".t_127 as select * from t_127")
        # spark.sql("insert overwrite table " +database+".t_127  select * from t_127")

        # 指标128 目标公司近三个月注册资本变更次数
        spark.sql("select c.company_name,if(t128count is null,0,t128count) t128count from companyList c left join "
                  "(select l.company_name,count(*) t128count from companyList l join (select * from zczbbg where "
                  "change_date>='" + threeMonthAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                          "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_128")
        spark.sql("create table " + database + ".t_128 as select * from t_128")
        # spark.sql("insert overwrite table " +database+".t_128  select * from t_128")


        # 指标129 目标公司近两年注册资本变更次数（增加）
        spark.sql("select c.company_name,if(t129count is null,0,t129count) t129count from companyList c left join "
                  "(select l.company_name,count(*) t129count from companyList l join (select * from zczbbg where "
                  "change_date>='" + twoYearAgo + "' and change_date<='" + givenDate + "' and "
                                                                                       "(regexp_extract(content_after_change,'[0-9.]+',0)-regexp_extract(content_before_change,'[0-9.]+',0))>0) r "
                                                                                       "on l.company_name=r.company_name group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_129")
        spark.sql("create table " + database + ".t_129 as select * from t_129")
        # spark.sql("insert overwrite table " +database+".t_129  select * from t_129")

        # 指标130 目标公司近一年注册资本变更次数（增加）
        spark.sql("select c.company_name,if(t130count is null,0,t130count) t130count from companyList c left join "
                  "(select l.company_name,count(*) t130count from companyList l join (select * from zczbbg where "
                  "change_date>='" + oneYearAgo + "' and change_date<='" + givenDate + "' and "
                                                                                       "(regexp_extract(content_after_change,'[0-9.]+',0)-regexp_extract(content_before_change,'[0-9.]+',0))>0) r "
                                                                                       "on l.company_name=r.company_name group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_130")
        spark.sql("create table " + database + ".t_130 as select * from t_130")
        # spark.sql("insert overwrite table " +database+".t_130  select * from t_130")

        # 指标131 目标公司近半年注册资本变更次数（增加）
        spark.sql("select c.company_name,if(t131count is null,0,t131count) t131count from companyList c left join "
                  "(select l.company_name,count(*) t131count from companyList l join (select * from zczbbg where "
                  "change_date>='" + halfyearAgo + "' and change_date<='" + givenDate + "' and "
                                                                                        "(regexp_extract(content_after_change,'[0-9.]+',0)-regexp_extract(content_before_change,'[0-9.]+',0))>0) r "
                                                                                        "on l.company_name=r.company_name group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_131")
        spark.sql("create table " + database + ".t_131 as select * from t_131")
        # spark.sql("insert overwrite table " +database+".t_131  select * from t_131")

        # 指标132 目标公司近三个月注册资本变更次数（增加）
        spark.sql("select c.company_name,if(t132count is null,0,t132count) t132count from companyList c left join "
                  "(select l.company_name,count(*) t132count from companyList l join (select * from zczbbg where "
                  "change_date>='" + threeMonthAgo + "' and change_date<='" + givenDate + "' and "
                                                                                          "(regexp_extract(content_after_change,'[0-9.]+',0)-regexp_extract(content_before_change,'[0-9.]+',0))>0) r "
                                                                                          "on l.company_name=r.company_name group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_132")
        spark.sql("create table " + database + ".t_132 as select * from t_132")
        # spark.sql("insert overwrite table " +database+".t_132  select * from t_132")

        # 指标133 目标公司近两年注册资本变更次数（减少）
        spark.sql("select c.company_name,if(t133count is null,0,t133count) t133count from companyList c left join "
                  "(select l.company_name,count(*) t133count from companyList l join (select * from zczbbg where "
                  "change_date>='" + twoYearAgo + "' and change_date<='" + givenDate + "' and "
                                                                                       "(regexp_extract(content_after_change,'[0-9.]+',0)-regexp_extract(content_before_change,'[0-9.]+',0))<0) r "
                                                                                       "on l.company_name=r.company_name group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_133")
        spark.sql("create table " + database + ".t_133 as select * from t_133")
        # spark.sql("insert overwrite table " +database+".t_133  select * from t_133")

        # 指标134 目标公司近一年注册资本变更次数（减少）
        spark.sql("select c.company_name,if(t134count is null,0,t134count) t134count from companyList c left join "
                  "(select l.company_name,count(*) t134count from companyList l join (select * from zczbbg where "
                  "change_date>='2015-08-31' and change_date<='2016-08-30' and "
                  "(regexp_extract(content_after_change,'[0-9.]+',0)-regexp_extract(content_before_change,'[0-9.]+',0))<0) r "
                  "on l.company_name=r.company_name group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_134")
        spark.sql("create table " + database + ".t_134 as select * from t_134")
        # spark.sql("insert overwrite table " +database+".t_134  select * from t_134")

        # 指标135 目标公司近半年注册资本变更次数（减少）
        spark.sql("select c.company_name,if(t135count is null,0,t135count) t135count from companyList c left join "
                  "(select l.company_name,count(*) t135count from companyList l join (select * from zczbbg where "
                  "change_date>='" + halfyearAgo + "' and change_date<='" + givenDate + "' and "
                                                                                        "(regexp_extract(content_after_change,'[0-9.]+',0)-regexp_extract(content_before_change,'[0-9.]+',0))<0) r "
                                                                                        "on l.company_name=r.company_name group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_135")
        spark.sql("create table " + database + ".t_135 as select * from t_135")
        # spark.sql("insert overwrite table " +database+".t_135  select * from t_135")

        # 指标136 目标公司近三个月注册资本变更次数（减少）
        spark.sql("select c.company_name,if(t136count is null,0,t136count) t136count from companyList c left join "
                  "(select l.company_name,count(*) t136count from companyList l join (select * from zczbbg where "
                  "change_date>='" + threeMonthAgo + "' and change_date<='" + givenDate + "' and "
                                                                                          "(regexp_extract(content_after_change,'[0-9.]+',0)-regexp_extract(content_before_change,'[0-9.]+',0))<0) r "
                                                                                          "on l.company_name=r.company_name group by l.company_name) lr on c.company_name=lr.company_name") \
            .createOrReplaceTempView("t_136")
        spark.sql("create table " + database + ".t_136 as select * from t_136")
        # spark.sql("insert overwrite table " +database+".t_136  select * from t_136")


        spark.sql("select company_name,change_date from bgxx where instr(change_items,'住所')>0 or "
                  "instr(change_items,'经营场所')>0 or instr(change_items,'地址')>0 ").createOrReplaceTempView("jydzbg")

        # 指标137 目标公司近两年经营地址变更次数
        spark.sql("select c.company_name,if(t137count is null,0,t137count) t137count from companyList c left join "
                  "(select l.company_name,count(*) t137count from companyList l join (select * from jydzbg where "
                  "change_date>='" + twoYearAgo + "' and change_date<='" + "+givenDate+" + "') r on l.company_name=r.company_name "
                                                                                           "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_137")
        spark.sql("create table " + database + ".t_137 as select * from t_137")
        # spark.sql("insert overwrite table " +database+".t_137  select * from t_137")

        # 指标138 目标公司近一年经营地址变更次数
        spark.sql("select c.company_name,if(t138count is null,0,t138count) t138count from companyList c left join "
                  "(select l.company_name,count(*) t138count from companyList l join (select * from jydzbg where "
                  "change_date>='" + oneYearAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                       "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_138")
        spark.sql("create table " + database + ".t_138 as select * from t_138")
        # spark.sql("insert overwrite table " +database+".t_138  select * from t_138")

        # 指标139 目标公司近半年经营地址变更次数
        spark.sql("select c.company_name,if(t139count is null,0,t139count) t139count from companyList c left join "
                  "(select l.company_name,count(*) t139count from companyList l join (select * from jydzbg where "
                  "change_date>='" + halfyearAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                        "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_139")
        spark.sql("create table " + database + ".t_139 as select * from t_139")
        # spark.sql("insert overwrite table " +database+".t_139  select * from t_139")

        # 指标140 目标公司近三个月经营地址变更次数
        spark.sql("select c.company_name,if(t140count is null,0,t140count) t140count from companyList c left join "
                  "(select l.company_name,count(*) t140count from companyList l join (select * from jydzbg where "
                  "change_date>='" + threeMonthAgo + "' and change_date<='" + givenDate + "') r on l.company_name=r.company_name "
                                                                                          "group by l.company_name) lr on c.company_name=lr.company_name").createOrReplaceTempView(
            "t_140")
        spark.sql("create table " + database + ".t_140 as select * from t_140")
        # spark.sql("insert overwrite table " +database+".t_140  select * from t_140")


        def process(data):
            node = data.company_name
            edgeList = data.sou_des.split(',')
            relation = []
            for edge in edgeList:
                sour_dest = edge.split('|')
                if len(sour_dest) != 2:
                    continue
                relation.append((sour_dest[0], sour_dest[1]))
            G = nx.Graph()
            G.add_node(node)
            G.add_edges_from(relation)
            aver_clustering = nx.average_clustering(G)
            return Row(company_name=node, t111count=aver_clustering)

        spark.sql(
            "select /*+mapjoin(companyList) */ DISTINCT r.company_name,concat_ws('|',r.source_name,r.destination_name) sou_des "
            "from companyList l join flat_relation r on l.company_name=r.company_name") \
            .createOrReplaceTempView("target_data")

        relationRdd = spark.sql(
            "select company_name,concat_ws(',', collect_set(sou_des)) as sou_des from target_data group by company_name") \
            .rdd \
            .map(process)
        spark.createDataFrame(relationRdd).createOrReplaceTempView("t_111")
        spark.sql(
            "select l.company_name,if(t111count is null,0.0,t111count) t111count from companyList l left join t_111 r "
            "on l.company_name=r.company_name").createOrReplaceTempView("t_111")

        spark.sql("create table " + database + ".t_111 as select company_name,round(t111count,2) t111count from t_111")


        ###################################部分合并#################################################
        spark.sql("select distinct company_name from "+company_name_table) \
            .createOrReplaceTempView("companylist_property")

        spark.sql("select * from " + database + ".t1_7").createOrReplaceTempView("t1_7")
        spark.sql("select * from " + database + ".t_8").createOrReplaceTempView("t_8")
        spark.sql("select * from " + database + ".t_9").createOrReplaceTempView("t_9")
        spark.sql("select * from " + database + ".t_10").createOrReplaceTempView("t_10")
        spark.sql("select * from " + database + ".t_11").createOrReplaceTempView("t_11")
        spark.sql("select * from " + database + ".t_12").createOrReplaceTempView("t_12")
        spark.sql("select * from " + database + ".t_13").createOrReplaceTempView("t_13")
        spark.sql("select * from " + database + ".t_14").createOrReplaceTempView("t_14")
        spark.sql("select * from " + database + ".t_15").createOrReplaceTempView("t_15")
        spark.sql("select * from " + database + ".t_16").createOrReplaceTempView("t_16")
        spark.sql("select * from " + database + ".t_17").createOrReplaceTempView("t_17")
        spark.sql("select * from " + database + ".t_18").createOrReplaceTempView("t_18")
        spark.sql("select * from " + database + ".t_19").createOrReplaceTempView("t_19")
        spark.sql("select * from " + database + ".t_20").createOrReplaceTempView("t_20")
        spark.sql("select * from " + database + ".t_21").createOrReplaceTempView("t_21")
        spark.sql("select * from " + database + ".t_22").createOrReplaceTempView("t_22")
        spark.sql("select * from " + database + ".t_23").createOrReplaceTempView("t_23")
        spark.sql("select * from " + database + ".t_24").createOrReplaceTempView("t_24")
        spark.sql("select * from " + database + ".t_25").createOrReplaceTempView("t_25")
        spark.sql("select * from " + database + ".t_26").createOrReplaceTempView("t_26")
        spark.sql("select * from " + database + ".t_27").createOrReplaceTempView("t_27")
        spark.sql("select * from " + database + ".t_28").createOrReplaceTempView("t_28")
        spark.sql("select * from " + database + ".t_29").createOrReplaceTempView("t_29")
        spark.sql("select * from " + database + ".t_30").createOrReplaceTempView("t_30")
        spark.sql("select * from " + database + ".t_31").createOrReplaceTempView("t_31")
        spark.sql("select * from " + database + ".t_32").createOrReplaceTempView("t_32")
        spark.sql("select * from " + database + ".t_33").createOrReplaceTempView("t_33")
        spark.sql("select * from " + database + ".t_34").createOrReplaceTempView("t_34")
        spark.sql("select * from " + database + ".t_35").createOrReplaceTempView("t_35")
        spark.sql("select * from " + database + ".t_36").createOrReplaceTempView("t_36")
        spark.sql("select * from " + database + ".t_37").createOrReplaceTempView("t_37")
        spark.sql("select * from " + database + ".t_38").createOrReplaceTempView("t_38")
        spark.sql("select * from " + database + ".t_39").createOrReplaceTempView("t_39")
        spark.sql("select * from " + database + ".t_40").createOrReplaceTempView("t_40")
        spark.sql("select * from " + database + ".t_41").createOrReplaceTempView("t_41")
        spark.sql("select * from " + database + ".t_42").createOrReplaceTempView("t_42")
        spark.sql("select * from " + database + ".t_43").createOrReplaceTempView("t_43")
        spark.sql("select * from " + database + ".t_44").createOrReplaceTempView("t_44")
        spark.sql("select * from " + database + ".t_45").createOrReplaceTempView("t_45")
        spark.sql("select * from " + database + ".t_46").createOrReplaceTempView("t_46")
        spark.sql("select * from " + database + ".t_47").createOrReplaceTempView("t_47")
        spark.sql("select * from " + database + ".t_48").createOrReplaceTempView("t_48")
        spark.sql("select * from " + database + ".t_49").createOrReplaceTempView("t_49")
        spark.sql("select * from " + database + ".t_50").createOrReplaceTempView("t_50")
        spark.sql("select * from " + database + ".t_51").createOrReplaceTempView("t_51")
        spark.sql("select * from " + database + ".t_52").createOrReplaceTempView("t_52")
        spark.sql("select * from " + database + ".t_53").createOrReplaceTempView("t_53")
        spark.sql("select * from " + database + ".t_54").createOrReplaceTempView("t_54")
        spark.sql("select * from " + database + ".t_55").createOrReplaceTempView("t_55")
        spark.sql("select * from " + database + ".t_56").createOrReplaceTempView("t_56")
        spark.sql("select * from " + database + ".t_57").createOrReplaceTempView("t_57")
        spark.sql("select * from " + database + ".t_58").createOrReplaceTempView("t_58")
        spark.sql("select * from " + database + ".t_59").createOrReplaceTempView("t_59")
        spark.sql("select * from " + database + ".t_60").createOrReplaceTempView("t_60")
        spark.sql("select * from " + database + ".t_61").createOrReplaceTempView("t_61")
        spark.sql("select * from " + database + ".t_62").createOrReplaceTempView("t_62")
        spark.sql("select * from " + database + ".t_63").createOrReplaceTempView("t_63")
        spark.sql("select * from " + database + ".t_64").createOrReplaceTempView("t_64")
        spark.sql("select * from " + database + ".t_65").createOrReplaceTempView("t_65")
        spark.sql("select * from " + database + ".t_66").createOrReplaceTempView("t_66")
        spark.sql("select * from " + database + ".t_67").createOrReplaceTempView("t_67")
        spark.sql("select * from " + database + ".t_68").createOrReplaceTempView("t_68")
        spark.sql("select * from " + database + ".t_69").createOrReplaceTempView("t_69")
        spark.sql("select * from " + database + ".t_70").createOrReplaceTempView("t_70")
        spark.sql("select * from " + database + ".t_71").createOrReplaceTempView("t_71")
        spark.sql("select * from " + database + ".t_72").createOrReplaceTempView("t_72")
        spark.sql("select * from " + database + ".t_73").createOrReplaceTempView("t_73")
        spark.sql("select * from " + database + ".t_74").createOrReplaceTempView("t_74")
        spark.sql("select * from " + database + ".t_75").createOrReplaceTempView("t_75")
        spark.sql("select * from " + database + ".t_76").createOrReplaceTempView("t_76")
        spark.sql("select * from " + database + ".t_77").createOrReplaceTempView("t_77")
        spark.sql("select * from " + database + ".t_78").createOrReplaceTempView("t_78")
        spark.sql("select * from " + database + ".t_79").createOrReplaceTempView("t_79")
        spark.sql("select * from " + database + ".t_80").createOrReplaceTempView("t_80")
        spark.sql("select * from " + database + ".t_81").createOrReplaceTempView("t_81")
        spark.sql("select * from " + database + ".t_82").createOrReplaceTempView("t_82")
        spark.sql("select * from " + database + ".t_83").createOrReplaceTempView("t_83")
        spark.sql("select * from " + database + ".t_84").createOrReplaceTempView("t_84")
        spark.sql("select * from " + database + ".t_85").createOrReplaceTempView("t_85")
        spark.sql("select * from " + database + ".t_86").createOrReplaceTempView("t_86")
        spark.sql("select * from " + database + ".t_87").createOrReplaceTempView("t_87")
        spark.sql("select * from " + database + ".t_88").createOrReplaceTempView("t_88")
        spark.sql("select * from " + database + ".t_89").createOrReplaceTempView("t_89")
        spark.sql("select * from " + database + ".t_90").createOrReplaceTempView("t_90")
        spark.sql("select * from " + database + ".t_91").createOrReplaceTempView("t_91")
        spark.sql("select * from " + database + ".t_92").createOrReplaceTempView("t_92")
        spark.sql("select * from " + database + ".t_93").createOrReplaceTempView("t_93")
        spark.sql("select * from " + database + ".t_94").createOrReplaceTempView("t_94")
        spark.sql("select * from " + database + ".t_95").createOrReplaceTempView("t_95")
        spark.sql("select * from " + database + ".t_96").createOrReplaceTempView("t_96")
        spark.sql("select * from " + database + ".t_97").createOrReplaceTempView("t_97")
        spark.sql("select * from " + database + ".t_98").createOrReplaceTempView("t_98")
        spark.sql("select * from " + database + ".t_99").createOrReplaceTempView("t_99")
        spark.sql("select * from " + database + ".t_100").createOrReplaceTempView("t_100")
        spark.sql("select * from " + database + ".t_101").createOrReplaceTempView("t_101")
        spark.sql("select * from " + database + ".t_102").createOrReplaceTempView("t_102")
        spark.sql("select * from " + database + ".t_103").createOrReplaceTempView("t_103")
        spark.sql("select * from " + database + ".t_104").createOrReplaceTempView("t_104")
        spark.sql("select * from " + database + ".t_105").createOrReplaceTempView("t_105")
        spark.sql("select * from " + database + ".t_106").createOrReplaceTempView("t_106")
        spark.sql("select * from " + database + ".t_107").createOrReplaceTempView("t_107")
        spark.sql("select * from " + database + ".t_108").createOrReplaceTempView("t_108")
        spark.sql("select * from " + database + ".t_109").createOrReplaceTempView("t_109")
        spark.sql("select * from " + database + ".t_110").createOrReplaceTempView("t_110")
        spark.sql("select * from " + database + ".t_111").createOrReplaceTempView("t_111")
        spark.sql("select * from " + database + ".t_112").createOrReplaceTempView("t_112")
        spark.sql("select * from " + database + ".t_113").createOrReplaceTempView("t_113")
        spark.sql("select * from " + database + ".t_114").createOrReplaceTempView("t_114")
        spark.sql("select * from " + database + ".t_115").createOrReplaceTempView("t_115")
        spark.sql("select * from " + database + ".t_116").createOrReplaceTempView("t_116")
        spark.sql("select * from " + database + ".t_117").createOrReplaceTempView("t_117")
        spark.sql("select * from " + database + ".t_118").createOrReplaceTempView("t_118")
        spark.sql("select * from " + database + ".t_119").createOrReplaceTempView("t_119")
        spark.sql("select * from " + database + ".t_120").createOrReplaceTempView("t_120")
        spark.sql("select * from " + database + ".t_121").createOrReplaceTempView("t_121")
        spark.sql("select * from " + database + ".t_122").createOrReplaceTempView("t_122")
        spark.sql("select * from " + database + ".t_123").createOrReplaceTempView("t_123")
        spark.sql("select * from " + database + ".t_124").createOrReplaceTempView("t_124")
        spark.sql("select * from " + database + ".t_125").createOrReplaceTempView("t_125")
        spark.sql("select * from " + database + ".t_126").createOrReplaceTempView("t_126")
        spark.sql("select * from " + database + ".t_127").createOrReplaceTempView("t_127")
        spark.sql("select * from " + database + ".t_128").createOrReplaceTempView("t_128")
        spark.sql("select * from " + database + ".t_129").createOrReplaceTempView("t_129")
        spark.sql("select * from " + database + ".t_130").createOrReplaceTempView("t_130")
        spark.sql("select * from " + database + ".t_131").createOrReplaceTempView("t_131")
        spark.sql("select * from " + database + ".t_132").createOrReplaceTempView("t_132")
        spark.sql("select * from " + database + ".t_133").createOrReplaceTempView("t_133")
        spark.sql("select * from " + database + ".t_134").createOrReplaceTempView("t_134")
        spark.sql("select * from " + database + ".t_135").createOrReplaceTempView("t_135")
        spark.sql("select * from " + database + ".t_136").createOrReplaceTempView("t_136")
        spark.sql("select * from " + database + ".t_137").createOrReplaceTempView("t_137")
        spark.sql("select * from " + database + ".t_138").createOrReplaceTempView("t_138")
        spark.sql("select * from " + database + ".t_139").createOrReplaceTempView("t_139")
        spark.sql("select * from " + database + ".t_140").createOrReplaceTempView("t_140")
        spark.sql("select * from " + database + ".t_141").createOrReplaceTempView("t_141")
        spark.sql("select * from " + database + ".t_142").createOrReplaceTempView("t_142")

        spark.sql("select l.company_name,t1.esdate,t1.company_companytype,t1.company_county,"
                  "t1.company_industry,t1.regcap_amount,t1.unregcap_amount,t1.ipo_company,t8.t8count,t9.t9count,"
                  "t10.t10count,t11.t11count,t12.t12count,t13.t13count,t14.t14count,t15.t15count,t16.t16count,"
                  "t17.t17count,t18.t18count "
                  "from companylist_property l "
                  "left join t1_7 t1 on l.company_name=t1.company_name "
                  "left join t_8  t8 on l.company_name=t8.company_name "
                  "left join t_9  t9 on l.company_name=t9.company_name "
                  "left join t_10  t10 on l.company_name=t10.company_name "
                  "left join t_11  t11 on l.company_name=t11.company_name "
                  "left join t_12  t12 on l.company_name=t12.company_name "
                  "left join t_13  t13 on l.company_name=t13.company_name "
                  "left join t_14  t14 on l.company_name=t14.company_name "
                  "left join t_15  t15 on l.company_name=t15.company_name "
                  "left join t_16  t16 on l.company_name=t16.company_name "
                  "left join t_17  t17 on l.company_name=t17.company_name "
                  "left join t_18  t18 on l.company_name=t18.company_name") \
            .createOrReplaceTempView("ald3_first_white_18")

        spark.sql("create table " + database + ".ald3_first_white_18 as select * from ald3_first_white_18")
        # #spark.sql("insert overwrite table " +database+".ald3_first_black_total  select * from ald3_first_black_total")

        spark.sql("select l.company_name,"
                  "t19.t19count,t20.t20count,t21.t21count,t22.t22count,t23.t23count,"
                  "t24.t24count,t25.t25count,t26.t26count,t27.t27count,t28.t28count,t29.t29count,t30.t30count "
                  "from companylist_property l "
                  "left join t_19  t19 on l.company_name=t19.company_name "
                  "left join t_20  t20 on l.company_name=t20.company_name "
                  "left join t_21  t21 on l.company_name=t21.company_name "
                  "left join t_22  t22 on l.company_name=t22.company_name "
                  "left join t_23  t23 on l.company_name=t23.company_name "
                  "left join t_24  t24 on l.company_name=t24.company_name "
                  "left join t_25  t25 on l.company_name=t25.company_name "
                  "left join t_26  t26 on l.company_name=t26.company_name "
                  "left join t_27  t27 on l.company_name=t27.company_name "
                  "left join t_28  t28 on l.company_name=t28.company_name "
                  "left join t_29  t29 on l.company_name=t29.company_name "
                  "left join t_30  t30 on l.company_name=t30.company_name ") \
            .createOrReplaceTempView("ald3_first_white_30")
        spark.sql("create table " + database + ".ald3_first_white_30 as select * from ald3_first_white_30")
        # #spark.sql("insert overwrite table " +database+".ald3_first_white_30  select * from ald3_first_white_30")

        spark.sql("select l.company_name,"
                  "t31.t31count,t32.t32count,t33.t33count,t34.t34count,t35.t35count,t36.t36count,t37.t37count,"
                  "t38.t38count,t39.t39count,t40.t40count,t41.t41count,t42.t42count "
                  "from companylist_property l "
                  "left join t_31  t31 on l.company_name=t31.company_name "
                  "left join t_32  t32 on l.company_name=t32.company_name "
                  "left join t_33  t33 on l.company_name=t33.company_name "
                  "left join t_34  t34 on l.company_name=t34.company_name "
                  "left join t_35  t35 on l.company_name=t35.company_name "
                  "left join t_36  t36 on l.company_name=t36.company_name "
                  "left join t_37  t37 on l.company_name=t37.company_name "
                  "left join t_38  t38 on l.company_name=t38.company_name "
                  "left join t_39  t39 on l.company_name=t39.company_name "
                  "left join t_40  t40 on l.company_name=t40.company_name "
                  "left join t_41  t41 on l.company_name=t41.company_name "
                  "left join t_42  t42 on l.company_name=t42.company_name ") \
            .createOrReplaceTempView("ald3_first_white_42")

        spark.sql("create table " + database + ".ald3_first_white_42 as select * from ald3_first_white_42")
        # #spark.sql("insert overwrite table " +database+".ald3_first_white_43  select * from ald3_first_white_43")

        spark.sql("select l.company_name,"
                  "t43.t43count,t44.t44count,t45.t45count,t46.t46count,t47.t47count,t48.t48count,t49.t49count,t50.t50count,"
                  "t51.t51count,t52.t52count,t53.t53count,t54.t54count "
                  "from companylist_property l "
                  "left join t_43  t43 on l.company_name=t43.company_name "
                  "left join t_44  t44 on l.company_name=t44.company_name "
                  "left join t_45  t45 on l.company_name=t45.company_name "
                  "left join t_46  t46 on l.company_name=t46.company_name "
                  "left join t_47  t47 on l.company_name=t47.company_name "
                  "left join t_48  t48 on l.company_name=t48.company_name "
                  "left join t_49  t49 on l.company_name=t49.company_name "
                  "left join t_50  t50 on l.company_name=t50.company_name "
                  "left join t_51  t51 on l.company_name=t51.company_name "
                  "left join t_52  t52 on l.company_name=t52.company_name "
                  "left join t_53  t53 on l.company_name=t53.company_name "
                  "left join t_54  t54 on l.company_name=t54.company_name ") \
            .createOrReplaceTempView("ald3_first_white_54")

        spark.sql("create table " + database + ".ald3_first_white_54 as select * from ald3_first_white_54")
        # #spark.sql("insert overwrite table " +database+".ald3_first_white_57  select * from ald3_first_white_57")

        spark.sql("select l.company_name,"
                  "t55.t55count,t56.t56count,t57.t57count,t58.t58count,t59.t59count,t60.t60count,t61.t61count,"
                  "t62.t62count,t63.t63count,t64.t64count,t65.t65count,t66.t66count "
                  "from companylist_property l "
                  "left join t_55  t55 on l.company_name=t55.company_name "
                  "left join t_56  t56 on l.company_name=t56.company_name "
                  "left join t_57  t57 on l.company_name=t57.company_name "
                  "left join t_58  t58 on l.company_name=t58.company_name "
                  "left join t_59  t59 on l.company_name=t59.company_name "
                  "left join t_60  t60 on l.company_name=t60.company_name "
                  "left join t_61  t61 on l.company_name=t61.company_name "
                  "left join t_62  t62 on l.company_name=t62.company_name "
                  "left join t_63  t63 on l.company_name=t63.company_name "
                  "left join t_64  t64 on l.company_name=t64.company_name "
                  "left join t_65  t65 on l.company_name=t65.company_name "
                  "left join t_66  t66 on l.company_name=t66.company_name ") \
            .createOrReplaceTempView("ald3_first_white_66")

        spark.sql("create table " + database + ".ald3_first_white_66 as select * from ald3_first_white_66")
        # #spark.sql("insert overwrite table " +database+".ald3_first_white_71  select * from ald3_first_white_71")

        spark.sql("select l.company_name,"
                  "t67.t67count,t68.t68count,t69.t69count,t70.t70count,t71.t71count,t72.t72count,t73.t73count,"
                  "t74.t74count,t75.t75count,t76.t76count,t77.t77count,t78.t78count "
                  "from companylist_property l "
                  "left join t_67  t67 on l.company_name=t67.company_name "
                  "left join t_68  t68 on l.company_name=t68.company_name "
                  "left join t_69  t69 on l.company_name=t69.company_name "
                  "left join t_70  t70 on l.company_name=t70.company_name "
                  "left join t_71  t71 on l.company_name=t71.company_name "
                  "left join t_72  t72 on l.company_name=t72.company_name "
                  "left join t_73  t73 on l.company_name=t73.company_name "
                  "left join t_74  t74 on l.company_name=t74.company_name "
                  "left join t_75  t75 on l.company_name=t75.company_name "
                  "left join t_76  t76 on l.company_name=t76.company_name "
                  "left join t_77  t77 on l.company_name=t77.company_name "
                  "left join t_78  t78 on l.company_name=t78.company_name ") \
            .createOrReplaceTempView("ald3_first_white_78")

        spark.sql("create table " + database + ".ald3_first_white_78 as select * from ald3_first_white_78")
        # #spark.sql("insert overwrite table " +database+".ald3_first_white_78  select * from ald3_first_white_78")

        spark.sql("select l.company_name,"
                  "t79.t79count,t80.t80count,t81.t81count,t82.t82count,t83.t83count,t84.t84count,t85.t85count,"
                  "t86.t86count,t87.t87count,t88.t88count,t89.t89count,t90.t90count "
                  "from companylist_property l "
                  "left join t_79  t79 on l.company_name=t79.company_name "
                  "left join t_80  t80 on l.company_name=t80.company_name "
                  "left join t_81  t81 on l.company_name=t81.company_name "
                  "left join t_82  t82 on l.company_name=t82.company_name "
                  "left join t_83  t83 on l.company_name=t83.company_name "
                  "left join t_84  t84 on l.company_name=t84.company_name "
                  "left join t_85  t85 on l.company_name=t85.company_name "
                  "left join t_86  t86 on l.company_name=t86.company_name "
                  "left join t_87  t87 on l.company_name=t87.company_name "
                  "left join t_88  t88 on l.company_name=t88.company_name "
                  "left join t_89  t89 on l.company_name=t89.company_name "
                  "left join t_90  t90 on l.company_name=t90.company_name ") \
            .createOrReplaceTempView("ald3_first_white_90")

        spark.sql("create table " + database + ".ald3_first_white_90 as select * from ald3_first_white_90")
        # #spark.sql("insert overwrite table " +database+".ald_first_white_99  select * from ald_first_white_99")
        spark.sql("select l.company_name, "
                  "t91.t91count,t92.t92count,t93.t93count,t94.t94count,t95.t95count,t96.t96count,"
                  "t97.t97count,t98.t98count,t99.t99count,t100.t100count,t101.t101count,t102.t102count "
                  "from companylist_property l "
                  "left join t_91  t91 on l.company_name=t91.company_name "
                  "left join t_92  t92 on l.company_name=t92.company_name "
                  "left join t_93  t93 on l.company_name=t93.company_name "
                  "left join t_94  t94 on l.company_name=t94.company_name "
                  "left join t_95  t95 on l.company_name=t95.company_name "
                  "left join t_96  t96 on l.company_name=t96.company_name "
                  "left join t_97  t97 on l.company_name=t97.company_name "
                  "left join t_98  t98 on l.company_name=t98.company_name "
                  "left join t_99  t99 on l.company_name=t99.company_name "
                  "left join t_100  t100 on l.company_name=t100.company_name "
                  "left join t_101  t101 on l.company_name=t101.company_name "
                  "left join t_102  t102 on l.company_name=t102.company_name ") \
            .createOrReplaceTempView("ald3_first_white_102")
        spark.sql("create table " + database + ".ald3_first_white_102 as select * from ald3_first_white_102")
        # #spark.sql("insert overwrite table " +database+".ald_first_white_112  select * from ald_first_white_112")

        spark.sql("select l.company_name, "
                  "t103.t103count,t104.t104count,t105.t105count,t106.t106count,t107.t107count,t108.t108count,"
                  "t109.t109count,t110.t110count,t111.t111count,t112.t112count,t113.t113count,t114.t114count "
                  "from companylist_property l "
                  "left join t_103  t103 on l.company_name=t103.company_name "
                  "left join t_104  t104 on l.company_name=t104.company_name "
                  "left join t_105  t105 on l.company_name=t105.company_name "
                  "left join t_106  t106 on l.company_name=t106.company_name "
                  "left join t_107  t107 on l.company_name=t107.company_name "
                  "left join t_108  t108 on l.company_name=t108.company_name "
                  "left join t_109  t109 on l.company_name=t109.company_name "
                  "left join t_110  t110 on l.company_name=t110.company_name "
                  "left join t_111  t111 on l.company_name=t111.company_name "
                  "left join t_112  t112 on l.company_name=t112.company_name "
                  "left join t_113  t113 on l.company_name=t113.company_name "
                  "left join t_114  t114 on l.company_name=t114.company_name ") \
            .createOrReplaceTempView("ald3_first_white_114")

        spark.sql("create table " + database + ".ald3_first_white_114 as select * from ald3_first_white_114")
        # #spark.sql("insert overwrite table " +database+".ald_first_white_124  select * from ald_first_white_124")


        spark.sql("select l.company_name, "
                  "t115.t115count,t116.t116count,t117.t117count,t118.t118count,t119.t119count,t120.t120count,"
                  "t121.t121count,t122.t122count,t123.t123count,t124.t124count,t125.t125count,t126.t126count "
                  "from companylist_property l "
                  "left join t_115  t115 on l.company_name=t115.company_name "
                  "left join t_116  t116 on l.company_name=t116.company_name "
                  "left join t_117  t117 on l.company_name=t117.company_name "
                  "left join t_118  t118 on l.company_name=t118.company_name "
                  "left join t_119  t119 on l.company_name=t119.company_name "
                  "left join t_120  t120 on l.company_name=t120.company_name "
                  "left join t_121  t121 on l.company_name=t121.company_name "
                  "left join t_122  t122 on l.company_name=t122.company_name "
                  "left join t_123  t123 on l.company_name=t123.company_name "
                  "left join t_124  t124 on l.company_name=t124.company_name "
                  "left join t_125  t125 on l.company_name=t125.company_name "
                  "left join t_126  t126 on l.company_name=t126.company_name ") \
            .createOrReplaceTempView("ald3_first_white_126")

        spark.sql("create table " + database + ".ald3_first_white_126 as select * from ald3_first_white_126")
        # #spark.sql("insert overwrite table " +database+".ald_first_white_137  select * from ald_first_white_137")

        spark.sql("select l.company_name, "
                  "t127.t127count,t128.t128count,t129.t129count,t130.t130count,t131.t131count,t132.t132count,"
                  "t133.t133count,t134.t134count,t135.t135count,t136.t136count,t137.t137count,t138.t138count "
                  "from companylist_property l "
                  "left join t_127  t127 on l.company_name=t127.company_name "
                  "left join t_128  t128 on l.company_name=t128.company_name "
                  "left join t_129  t129 on l.company_name=t129.company_name "
                  "left join t_130  t130 on l.company_name=t130.company_name "
                  "left join t_131  t131 on l.company_name=t131.company_name "
                  "left join t_132  t132 on l.company_name=t132.company_name "
                  "left join t_133  t133 on l.company_name=t133.company_name "
                  "left join t_134  t134 on l.company_name=t134.company_name "
                  "left join t_135  t135 on l.company_name=t135.company_name "
                  "left join t_136  t136 on l.company_name=t136.company_name "
                  "left join t_137  t137 on l.company_name=t137.company_name "
                  "left join t_138  t138 on l.company_name=t138.company_name ") \
            .createOrReplaceTempView("ald3_first_white_138")
        spark.sql("create table " + database + ".ald3_first_white_138 as select * from ald3_first_white_138")
        # #spark.sql("insert overwrite table " +database+".ald_first_white_140  select * from ald_first_white_140")

        spark.sql("select l.company_name, "
                  "t139.t139count,t140.t140count,t141.t141count,t142.t142count "
                  "from companylist_property l "
                  "left join t_139  t139 on l.company_name=t139.company_name "
                  "left join t_140  t140 on l.company_name=t140.company_name "
                  "left join t_141  t141 on l.company_name=t141.company_name "
                  "left join t_142  t142 on l.company_name=t142.company_name ") \
            .createOrReplaceTempView("ald3_first_white_142")

        spark.sql("create table " + database + ".ald3_first_white_142 as select * from ald3_first_white_142")

        ###################################部分合并（向上）#################################################

        ###################################全部合并#####################################################
        spark.sql("select * from " + database + ".ald3_first_white_18").createOrReplaceTempView("ald3_first_white_18")
        spark.sql("select * from " + database + ".ald3_first_white_30").createOrReplaceTempView("ald3_first_white_30")
        spark.sql("select * from " + database + ".ald3_first_white_42").createOrReplaceTempView("ald3_first_white_42")
        spark.sql("select * from " + database + ".ald3_first_white_54").createOrReplaceTempView("ald3_first_white_54")
        spark.sql("select * from " + database + ".ald3_first_white_66").createOrReplaceTempView("ald3_first_white_66")
        spark.sql("select * from " + database + ".ald3_first_white_78").createOrReplaceTempView("ald3_first_white_78")
        spark.sql("select * from " + database + ".ald3_first_white_90").createOrReplaceTempView("ald3_first_white_90")
        spark.sql("select * from " + database + ".ald3_first_white_102").createOrReplaceTempView("ald3_first_white_102")
        spark.sql("select * from " + database + ".ald3_first_white_114").createOrReplaceTempView("ald3_first_white_114")
        spark.sql("select * from " + database + ".ald3_first_white_126").createOrReplaceTempView("ald3_first_white_126")
        spark.sql("select * from " + database + ".ald3_first_white_138").createOrReplaceTempView("ald3_first_white_138")
        spark.sql("select * from " + database + ".ald3_first_white_142").createOrReplaceTempView("ald3_first_white_142")

        spark.sql("select l.company_name,l.esdate,l.company_companytype,l.company_county,"
                  "l.company_industry,l.regcap_amount,l.unregcap_amount,l.ipo_company,l.t8count,l.t9count,"
                  "l.t10count,l.t11count,l.t12count,l.t13count,l.t14count,l.t15count,l.t16count,"
                  "l.t17count,l.t18count,r1.t19count,r1.t20count,r1.t21count,r1.t22count,r1.t23count,"
                  "r1.t24count,r1.t25count,r1.t26count,r1.t27count,r1.t28count,r1.t29count,r1.t30count,"
                  "r2.t31count,r2.t32count,r2.t33count,r2.t34count,r2.t35count,r2.t36count,r2.t37count,"
                  "r2.t38count,r2.t39count,r2.t40count,r2.t41count,r2.t42count,r3.t43count,"
                  "r3.t44count,r3.t45count,r3.t46count,r3.t47count,r3.t48count,r3.t49count,r3.t50count,"
                  "r3.t51count,r3.t52count,r3.t53count,r3.t54count,r4.t55count,r4.t56count,r4.t57count,"
                  "r4.t58count,r4.t59count,r4.t60count,r4.t61count,r4.t62count,r4.t63count,r4.t64count,"
                  "r4.t65count,r4.t66count,r5.t67count,r5.t68count,r5.t69count,r5.t70count,r5.t71count,"
                  "r5.t72count,r5.t73count,r5.t74count,r5.t75count,r5.t76count,r5.t77count,r5.t78count,"
                  "r6.t79count,r6.t80count,r6.t81count,r6.t82count,r6.t83count,r6.t84count,r6.t85count,"
                  "r6.t86count,r6.t87count,r6.t88count,r6.t89count,r6.t90count,r7.t91count,r7.t92count,"
                  "r7.t93count,r7.t94count,r7.t95count,r7.t96count,r7.t97count,r7.t98count,r7.t99count,"
                  "r7.t100count,r7.t101count,r7.t102count,r8.t103count,r8.t104count,r8.t105count,"
                  "r8.t106count,r8.t107count,r8.t108count,r8.t109count,r8.t110count,r8.t111count,r8.t112count,"
                  "r8.t113count,r8.t114count,r9.t115count,r9.t116count,r9.t117count,r9.t118count,"
                  "r9.t119count,r9.t120count,r9.t121count,r9.t122count,r9.t123count,r9.t124count,"
                  "r9.t125count,r9.t126count,r10.t127count,r10.t128count,r10.t129count,r10.t130count, "
                  "r10.t131count,r10.t132count,r10.t133count,r10.t134count,r10.t135count,r10.t136count,"
                  "r10.t137count,r10.t138count,r11.t139count,r11.t140count,r11.t141count,r11.t142count "
                  " from "
                  "ald3_first_white_18 l "
                  "join ald3_first_white_30 r1 on l.company_name=r1.company_name "
                  "join ald3_first_white_42 r2 on l.company_name=r2.company_name  "
                  "join ald3_first_white_54 r3 on l.company_name=r3.company_name "
                  "join ald3_first_white_66 r4 on l.company_name=r4.company_name  "
                  "join ald3_first_white_78 r5 on l.company_name=r5.company_name  "
                  "join ald3_first_white_90 r6 on l.company_name=r6.company_name  "
                  "join ald3_first_white_102 r7 on l.company_name=r7.company_name  "
                  "join ald3_first_white_114 r8 on l.company_name=r8.company_name  "
                  "join ald3_first_white_126 r9 on l.company_name=r9.company_name "
                  "join ald3_first_white_138 r10 on l.company_name=r10.company_name "
                  "join ald3_first_white_142 r11 on l.company_name=r11.company_name") \
            .createOrReplaceTempView("ald3_total")

        spark.sql("create table " +database+".ald3_total as select * from ald3_total")
        spark.sql("select * from ald3_total ").repartition(1) \
            .write \
            .csv("/user/qyxypf/ald3_result/" + database, header=True, sep=',')

        # os.system(
        #     "hadoop fs -getmerge /user/wangjinyang/result/wd_new_" + partition + " /data1/wangjinyang/data/wd_ald/wd_new_" + partition + ".csv")

    spark.stop()

if __name__ == '__main__':
    main()