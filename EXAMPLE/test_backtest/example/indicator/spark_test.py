# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
#from QUANTAXIS.ML import RegUtil
from pyspark.sql.types import *
import pyspark.sql.functions as fn
#from pyspark.sql.functions import *
import copy
#import talib
import time

spark = SparkSession.builder.appName("test").getOrCreate()

# fin = pro.QA_fetch_get_finindicator(start='20150101', end='20181231')
# stock = pro.QA_SU_stock_info()
# spark = SparkSession.builder.appName("shedule delay").getOrCreate()
# fin_spark = spark.createDataFrame(fin)
# stock_spark = spark.createDataFrame(stock)

p_struct = ['equity2_pb7', 'equity3_pb7', 'roe_year_pb7', 'roe_half_year_pb7', 'netasset', 'cash', 'q_ocf',
            'q4_opincome', 'q_dtprofit', 'gross_margin_poly', 'q_gsprofit_margin_poly', 'inv_turn_poly', 'fa_turn_poly']
p_struct = ['equity2_pb7', 'equity3_pb7', 'netasset', 'cash', 'q_ocf', 'q4_ocf', 'q4_opincome', 'q4_dtprofit']
# p = copy.deepcopy(fin_spark.schema)
p = StructType()
list(map(lambda x: p.add(StructField(x, DoubleType())), p_struct))


# @pandas_udf(p, PandasUDFType.GROUPED_MAP)
# def _ver_indicator0(data):
#     if (data['q_dt_roe'].isnull().all()):
#         roe_half_year = roe_year = np.array([np.nan] * data.shape[0])
#     else:
#         t = data['q_dt_roe'].fillna(method='bfill')  # 后面的数据向前填充
#         t = t.fillna(method='pad')
#         roe_year = (talib.EMA(t, 4) * 4 / 100 + 1).fillna(method='bfill')
#         roe_half_year = (talib.EMA(t, 2) * 4 / 100 + 1).fillna(method='bfill')
#         # roe_year = (QA.EMA(data['q_dt_roe'], 4)*4/100+1).fillna(method='bfill')
#         # roe_half_year = (QA.EMA(data['q_dt_roe'], 2)*4/100+1).fillna(method='bfill')
#     # 近2年净资产收益率
#     asset_rise_2year = (data['equity_yoy'].shift(4) / 100 + 1) * (data['equity_yoy'] / 100 + 1)
#     asset_rise_3year = (data['equity_yoy'].shift(8) / 100 + 1) * asset_rise_2year
#     data.loc[:, 'equity3_pb7'] = np.round(np.power(np.power(asset_rise_3year, 1 / 3), 7), 2)
#     # print(len(asset_rise_3year[asset_rise_3year.isnull()]))
#     data.loc[:, 'equity2_pb7'] = np.round(np.power(np.power(asset_rise_2year, 1 / 2), 7), 2)
#     # data.loc[:, 'roe_year_pb6'] = np.round(np.power(roe_year, 6),2)
#     # data.loc[:, 'roe_year_pb7'] = np.round(np.power(roe_year, 7),2)
#     # data.loc[:, 'roe_half_year_pb7'] = np.round(np.power(roe_half_year, 7), 2)
#     data.loc[:, 'netasset'] = data.ebit / data.ebit_ps * data.bps  # 净资产
#     data.loc[:, 'q_ocf'] = data.q_opincome * data.q_ocf_to_or  # 单季度经营活动产生的现金流量
#     data.loc[:, 'cash'] = data.ebit / data.ebit_ps * data.cfps  # 现金流
#     if data['q_ocf'].isnull().all():
#         data.loc[:, 'q4_ocf'] = np.nan
#     else:
#         t = data['q_ocf'].fillna(method='bfill')  # 后面的数据向前填充
#         t = t.fillna(method='pad')
#         data.loc[:, 'q4_ocf'] = (talib.EMA(t, 4)).fillna(method='bfill')  # QA.EMA(data['q_ocf'], 4)
#     if data['q_opincome'].isnull().all():
#         data.loc[:, 'q4_opincome'] = np.nan
#     else:
#         t = data['q_opincome'].fillna(method='bfill')  # 后面的数据向前填充
#         t = t.fillna(method='pad')
#         data.loc[:, 'q4_opincome'] = (talib.EMA(t, 4)).fillna(method='bfill')
#     if data['q_dtprofit'].isnull().all():
#         data.loc[:, 'q4_dtprofit'] = np.nan
#     else:
#         t = data['q_dtprofit'].fillna(method='bfill')  # 后面的数据向前填充
#         t = t.fillna(method='pad')
#         data.loc[:, 'q4_dtprofit'] = (talib.EMA(t, 4)).fillna(method='bfill')
#     data.q_gsprofit_margin.fillna(method='pad', inplace=True)
#     data.q_gsprofit_margin.fillna(method='bfill', inplace=True)
#     gs = data.q_gsprofit_margin[0:3].append(data.q_gsprofit_margin)  # 凑够长度，反正前几条数据也没有计算意义
#     gs = gs.fillna(method='bfill')
#     gs = gs.fillna(method='pad')
#     for i in range(data.q_gsprofit_margin.shape[0]):
#         RegUtil.calc_regress_deg(gs[i:i + 4], show=False)
#     return data

#fin_spark.groupby('ts_code').apply(_ver_indicator0).toPandas()


# @pandas_udf(stock_spark.schema, PandasUDFType.GROUPED_MAP)
# def _ver_indicator1(key,data):
#     t = data[data.ts_code=='000066.SZ']
#     #print(type(t))
#     if len(t) >0 :
#         print(type(t))
#         print(t.name.values[0])
#     #from statsmodels import api as sm, regression
#     #gs = data.q_gsprofit_margin[0:3].append(data.q_gsprofit_margin)  # 凑够长度，反正前几条数据也没有计算意义
#     #gs = gs.fillna(method='bfill')
#     #gs = gs.fillna(method='pad')
#     # RegUtil.calc_regress_deg(gs[0:4], show=False)
#     #x = np.arange(0, 4)
#     #x = sm.add_constant(x)
#     #model = regression.linear_model.OLS(gs[0:4], x).fit()
#     # for i in range(data.q_gsprofit_margin.shape[0]):
#     return data


#stock_spark.groupby('industry').apply(_ver_indicator1).toPandas()



# def _ver_indicator2(data):
#     for key in fin_spark.rdd.map(lambda x: x["ts_code"]).distinct().collect():
#         cur = fin_spark.filter(fin_spark.ts_code == key)
#         s = pd.Series(cur.select('q_gsprofit_margin').collect()).fillna(method='bfill')
#         s = s.fillna(method='pad')
#         gs = s[0:3].append(s)  # 凑够长度，反正前几条数据也没有计算意义
#         new_col = [RegUtil.calc_regress_deg(gs[i:i + 4], show=False) for i in range(s.shape[0])]
#         cur.withColumn('q_gsprofit_margin_poly', new_col)
        # fin_spark.filter(fin_spark.ts_code==key).withColumn(colName: String, col: Column)

        # def set_poly()
        # poly = udf(set_poly, DoubleType())
        # cur.select(col('q_gsprofit_margin'),)


#_ver_indicator2(fin_spark)

def calc_timing(df,cond) :
    t1 = time.time()
    print('#############################datafram created#################' + str(t1))
    df3 = df.filter(cond)
    # df3.cache()
    req = df3.groupBy("ts_code").agg(fn.sum('close').alias('sum_req'), fn.count('circ_mv').alias('n_req'))
    # print('##########'+str(df3.count())+'#################')
    req.collect()
    t2 = time.time() - t1
    print('###############action finished#################' + str(t2))

# def _ver_indicator3(data):
#     if (data['q_dt_roe'].isnull().all()):
#         roe_half_year = roe_year = np.array([np.nan] * data.shape[0])
#     else:
#         t = data['q_dt_roe'].fillna(method='bfill')  # 后面的数据向前填充
#         t = t.fillna(method='pad')
#         roe_year = (talib.EMA(t, 4) * 4 / 100 + 1).fillna(method='bfill')
#         roe_half_year = (talib.EMA(t, 2) * 4 / 100 + 1).fillna(method='bfill')
#         # roe_year = (QA.EMA(data['q_dt_roe'], 4)*4/100+1).fillna(method='bfill')
#         # roe_half_year = (QA.EMA(data['q_dt_roe'], 2)*4/100+1).fillna(method='bfill')
#     # 近2年净资产收益率
#     asset_rise_2year = (data['equity_yoy'].shift(4) / 100 + 1) * (data['equity_yoy'] / 100 + 1)
#     asset_rise_3year = (data['equity_yoy'].shift(8) / 100 + 1) * asset_rise_2year
#     data.loc[:, 'equity3_pb7'] = np.round(np.power(np.power(asset_rise_3year, 1 / 3), 7), 2)
#     # print(len(asset_rise_3year[asset_rise_3year.isnull()]))
#     data.loc[:, 'equity2_pb7'] = np.round(np.power(np.power(asset_rise_2year, 1 / 2), 7), 2)
#     # data.loc[:, 'roe_year_pb6'] = np.round(np.power(roe_year, 6),2)
#     # data.loc[:, 'roe_year_pb7'] = np.round(np.power(roe_year, 7),2)
#     # data.loc[:, 'roe_half_year_pb7'] = np.round(np.power(roe_half_year, 7), 2)
#     data.loc[:, 'netasset'] = data.ebit / data.ebit_ps * data.bps  # 净资产
#     data.loc[:, 'q_ocf'] = data.q_opincome * data.q_ocf_to_or  # 单季度经营活动产生的现金流量
#     data.loc[:, 'cash'] = data.ebit / data.ebit_ps * data.cfps  # 现金流
#     if data['q_ocf'].isnull().all():
#         data.loc[:, 'q4_ocf'] = np.nan
#     else:
#         t = data['q_ocf'].fillna(method='bfill')  # 后面的数据向前填充
#         t = t.fillna(method='pad')
#         data.loc[:, 'q4_ocf'] = (talib.EMA(t, 4)).fillna(method='bfill')  # QA.EMA(data['q_ocf'], 4)
#     if data['q_opincome'].isnull().all():
#         data.loc[:, 'q4_opincome'] = np.nan
#     else:
#         t = data['q_opincome'].fillna(method='bfill')  # 后面的数据向前填充
#         t = t.fillna(method='pad')
#         data.loc[:, 'q4_opincome'] = (talib.EMA(t, 4)).fillna(method='bfill')
#     if data['q_dtprofit'].isnull().all():
#         data.loc[:, 'q4_dtprofit'] = np.nan
#     else:
#         t = data['q_dtprofit'].fillna(method='bfill')  # 后面的数据向前填充
#         t = t.fillna(method='pad')
#         data.loc[:, 'q4_dtprofit'] = (talib.EMA(t, 4)).fillna(method='bfill')
#     data.q_gsprofit_margin.fillna(method='pad', inplace=True)
#     data.q_gsprofit_margin.fillna(method='bfill', inplace=True)
#     gs = data.q_gsprofit_margin[0:3].append(data.q_gsprofit_margin)  # 凑够长度，反正前几条数据也没有计算意义
#     gs = gs.fillna(method='bfill')
#     gs = gs.fillna(method='pad')
#     for i in range(data.q_gsprofit_margin.shape[0]):
#         RegUtil.calc_regress_deg(gs[i:i + 4], show=False)
#     return data
#fin.groupby('ts_code').apply(_ver_indicator3)  与_ver_indicator0做对比，可看出Pandas 与 spark集群下同样内存计算的时间差异
#df1 = pd.read_csv('/usr/local/spark/basic_temp_20180101_20181231bk.csv')
df1 =  spark.read.csv( 'hdfs://testCDH2.local/tmp/spark/basic_temp_20180101_20181231bk.csv',header = True)
df = df1.select('industry','trade_date','ts_code','circ_mv','close','float_share')
for i in range(8):
    temp = df1.select(fn.col('industry'),fn.col('ts_code'),fn.col('circ_mv'),fn.col('close'),fn.col('float_share'),fn.concat(fn.lit("201"+str(i)),fn.substring(fn.col("trade_date"), 4,8)).alias('trade_date'))
    df = df.union(temp);

for i in range(9):
    temp = df1.select(fn.col('industry'), fn.col('ts_code'), fn.col('circ_mv'), fn.col('close'), fn.col('float_share'), fn.concat(fn.lit("202" + str(i)), fn.substring(fn.col("trade_date"), 4, 8)).alias('trade_date'))
    df = df.union(temp);
for k in range(3,9):
    for i in range(9):
        temp = df1.select(fn.col('industry'), fn.col('ts_code'), fn.col('circ_mv'), fn.col('close'), fn.col('float_share'), fn.concat(fn.lit("20" +str(k) +str(i)), fn.substring(fn.col("trade_date"), 4, 8)).alias('trade_date'))
        df = df.union(temp);

# df1.loc[:,'trade_date'] = df1.trade_date.astype(str)
# df2 = df1.iloc[:,:6]
# for i in range(3):
#     temp = df1.iloc[:,:6]
#     temp.loc[:,'trade_date']  = '201'+str(i)+temp.trade_date.str[4:8]
#     df2 = pd.concat([df2,temp])
# for i in range(0,6):
#     temp = df1.iloc[:,:6]
#     temp.loc[:,'trade_date']  = '202'+str(i)+temp.trade_date.str[4:8]
#     df2 = pd.concat([df2,temp])
print('#############################date repeat #################'+time.strftime("%a %b %d %H:%M:%S %Y", time.localtime()))
# print(df2.shape[0])
# df = spark.createDataFrame(df2)

#df.collect()
df.cache()
calc_timing(df,"trade_date<'20110930'")
calc_timing(df,"trade_date<'20180930'")
print(df.count())
time.sleep(60*10)





#./bin/spark-submit --driver-memory 6G --conf spark.default.parallelism=48 --conf spark.sql.shuffle.partitions=24  --master spark://hadoop1:7077 --py-files /usr/local/spark/quantaxis.zip  /usr/local/spark/spark_test.py

#spark2-submit --driver-memory 6G --conf spark.default.parallelism=48 --conf spark.sql.shuffle.partitions=24  --master testcdh2.local://hadoop1:7077  spark_test.py

