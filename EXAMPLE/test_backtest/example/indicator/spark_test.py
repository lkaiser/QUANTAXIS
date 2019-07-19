# -*- coding: utf-8 -*-
import QUANTAXIS as QA
from QUANTAXIS.QAFetch import QATusharePro as pro
import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from QUANTAXIS.ML import RegUtil
from pyspark.sql.types import *
from pyspark.sql.functions import *
import copy
import talib

fin = pro.QA_fetch_get_finindicator(start='20150101', end='20181231')
stock = pro.QA_SU_stock_info()
fin_spark = spark.createDataFrame(fin)
stock_spark = spark.createDataFrame(stock)

p_struct = ['equity2_pb7', 'equity3_pb7', 'roe_year_pb7', 'roe_half_year_pb7', 'netasset', 'cash', 'q_ocf',
            'q4_opincome', 'q_dtprofit', 'gross_margin_poly', 'q_gsprofit_margin_poly', 'inv_turn_poly', 'fa_turn_poly']
p_struct = ['equity2_pb7', 'equity3_pb7', 'netasset', 'cash', 'q_ocf', 'q4_ocf', 'q4_opincome', 'q4_dtprofit']
p = copy.deepcopy(fin_spark.schema)
list(map(lambda x: p.add(StructField(x, DoubleType())), p_struct))


@pandas_udf(p, PandasUDFType.GROUPED_MAP)
def _ver_indicator(data):
    if (data['q_dt_roe'].isnull().all()):
        roe_half_year = roe_year = np.array([np.nan] * data.shape[0])
    else:
        t = data['q_dt_roe'].fillna(method='bfill')  # 后面的数据向前填充
        t = t.fillna(method='pad')
        roe_year = (talib.EMA(t, 4) * 4 / 100 + 1).fillna(method='bfill')
        roe_half_year = (talib.EMA(t, 2) * 4 / 100 + 1).fillna(method='bfill')
        # roe_year = (QA.EMA(data['q_dt_roe'], 4)*4/100+1).fillna(method='bfill')
        # roe_half_year = (QA.EMA(data['q_dt_roe'], 2)*4/100+1).fillna(method='bfill')
    # 近2年净资产收益率
    asset_rise_2year = (data['equity_yoy'].shift(4) / 100 + 1) * (data['equity_yoy'] / 100 + 1)
    asset_rise_3year = (data['equity_yoy'].shift(8) / 100 + 1) * asset_rise_2year
    data.loc[:, 'equity3_pb7'] = np.round(np.power(np.power(asset_rise_3year, 1 / 3), 7), 2)
    # print(len(asset_rise_3year[asset_rise_3year.isnull()]))
    data.loc[:, 'equity2_pb7'] = np.round(np.power(np.power(asset_rise_2year, 1 / 2), 7), 2)
    # data.loc[:, 'roe_year_pb6'] = np.round(np.power(roe_year, 6),2)
    # data.loc[:, 'roe_year_pb7'] = np.round(np.power(roe_year, 7),2)
    # data.loc[:, 'roe_half_year_pb7'] = np.round(np.power(roe_half_year, 7), 2)
    data.loc[:, 'netasset'] = data.ebit / data.ebit_ps * data.bps  # 净资产
    data.loc[:, 'q_ocf'] = data.q_opincome * data.q_ocf_to_or  # 单季度经营活动产生的现金流量
    data.loc[:, 'cash'] = data.ebit / data.ebit_ps * data.cfps  # 现金流
    if data['q_ocf'].isnull().all():
        data.loc[:, 'q4_ocf'] = np.nan
    else:
        t = data['q_ocf'].fillna(method='bfill')  # 后面的数据向前填充
        t = t.fillna(method='pad')
        data.loc[:, 'q4_ocf'] = (talib.EMA(t, 4)).fillna(method='bfill')  # QA.EMA(data['q_ocf'], 4)
    if data['q_opincome'].isnull().all():
        data.loc[:, 'q4_opincome'] = np.nan
    else:
        t = data['q_opincome'].fillna(method='bfill')  # 后面的数据向前填充
        t = t.fillna(method='pad')
        data.loc[:, 'q4_opincome'] = (talib.EMA(t, 4)).fillna(method='bfill')
    if data['q_dtprofit'].isnull().all():
        data.loc[:, 'q4_dtprofit'] = np.nan
    else:
        t = data['q_dtprofit'].fillna(method='bfill')  # 后面的数据向前填充
        t = t.fillna(method='pad')
        data.loc[:, 'q4_dtprofit'] = (talib.EMA(t, 4)).fillna(method='bfill')
    data.q_gsprofit_margin.fillna(method='pad', inplace=True)
    data.q_gsprofit_margin.fillna(method='bfill', inplace=True)
    gs = data.q_gsprofit_margin[0:3].append(data.q_gsprofit_margin)  # 凑够长度，反正前几条数据也没有计算意义
    gs = gs.fillna(method='bfill')
    gs = gs.fillna(method='pad')
    for i in range(data.q_gsprofit_margin.shape[0]):
        RegUtil.calc_regress_deg(gs[i:i + 4], show=False)
    return data


fin_spark.groupby('ts_code').apply(_ver_indicator).toPandas()


@pandas_udf(fin_spark.schema, PandasUDFType.GROUPED_MAP)
def _ver_indicator(data):
    from statsmodels import api as sm, regression
    gs = data.q_gsprofit_margin[0:3].append(data.q_gsprofit_margin)  # 凑够长度，反正前几条数据也没有计算意义
    gs = gs.fillna(method='bfill')
    gs = gs.fillna(method='pad')
    # RegUtil.calc_regress_deg(gs[0:4], show=False)
    x = np.arange(0, 4)
    x = sm.add_constant(x)
    model = regression.linear_model.OLS(gs[0:4], x).fit()
    # for i in range(data.q_gsprofit_margin.shape[0]):
    return data


fin_spark.groupby('ts_code').apply(_ver_indicator).toPandas()


def _ver_indicator2(data):
    for key in fin_spark.rdd.map(lambda x: x["ts_code"]).distinct().collect():
        cur = fin_spark.filter(fin_spark.ts_code == key)
        s = pd.Series(cur.select('q_gsprofit_margin').collect()).fillna(method='bfill')
        s = s.fillna(method='pad')
        gs = s[0:3].append(s)  # 凑够长度，反正前几条数据也没有计算意义
        new_col = [RegUtil.calc_regress_deg(gs[i:i + 4], show=False) for i in range(s.shape[0])]
        cur.withColumn('q_gsprofit_margin_poly', new_col)
        # fin_spark.filter(fin_spark.ts_code==key).withColumn(colName: String, col: Column)

        # def set_poly()
        # poly = udf(set_poly, DoubleType())
        # cur.select(col('q_gsprofit_margin'),)


_ver_indicator2(fin_spark)


@pandas_udf(p, PandasUDFType.GROUPED_MAP)
def _ver_indicator(data):
    '''
    纵轴指标
    :param data:
    :return:
    '''
    # print(data['q_dt_roe'].head())
    if (data['q_dt_roe'].isnull().all()):
        roe_half_year = roe_year = np.array([np.nan] * data.shape[0])
    else:
        roe_year = (QA.EMA(data['q_dt_roe'], 4) * 4 / 100 + 1).fillna(method='bfill')
        roe_half_year = (QA.EMA(data['q_dt_roe'], 2) * 4 / 100 + 1).fillna(method='bfill')
    # 近2年净资产收益率
    asset_rise_2year = (data['equity_yoy'].shift(4) / 100 + 1) * (data['equity_yoy'] / 100 + 1)
    # 近3年净资产收益率
    asset_rise_3year = (data['equity_yoy'].shift(8) / 100 + 1) * asset_rise_2year
    data.loc[:, 'equity3_pb7'] = np.round(np.power(np.power(asset_rise_3year, 1 / 3), 7), 2)
    # print(len(asset_rise_3year[asset_rise_3year.isnull()]))
    data.loc[:, 'equity2_pb7'] = np.round(np.power(np.power(asset_rise_2year, 1 / 2), 7), 2)
    # data.loc[:, 'roe_year_pb6'] = np.round(np.power(roe_year, 6),2)
    data.loc[:, 'roe_year_pb7'] = np.round(np.power(roe_year, 7), 2)
    data.loc[:, 'roe_half_year_pb7'] = np.round(np.power(roe_half_year, 7), 2)

    '''
    fcff  float 自由现金流
    ocfps   float 每股经营活动产生的现金流量净额
    cfps  float 每股现金流量净额
    ebit_ps float 每股息税前利润
    ebit 息税前利润
    bps  每股净资产
    profit_dedt 扣除非经常性损益后的净利润
    q_dtprofit 扣除非经常损益后的单季度净利润
    q_opincome 经营活动单季度净收益
    q_ocf_to_or 经营活动产生的现金流量净额／经营活动净收益(单季度)
    '''
    data.loc[:, 'netasset'] = data.ebit / data.ebit_ps * data.bps  # 净资产
    data.loc[:, 'cash'] = data.ebit / data.ebit_ps * data.cfps  # 现金流
    data.loc[:, 'q_ocf'] = data.q_opincome * data.q_ocf_to_or  # 单季度经营活动产生的现金流量
    if data['q_ocf'].isnull().all():
        data.loc[:, 'q4_ocf'] = np.nan
    else:
        data.loc[:, 'q4_ocf'] = QA.EMA(data['q_ocf'], 4)
    if data['q_opincome'].isnull().all():
        data.loc[:, 'q4_opincome'] = np.nan
    else:
        data.loc[:, 'q4_opincome'] = QA.EMA(data['q_opincome'], 4)
    if data['q_dtprofit'].isnull().all():
        data.loc[:, 'q4_dtprofit'] = np.nan
    else:
        data.loc[:, 'q4_dtprofit'] = QA.EMA(data['q_dtprofit'], 4)

    data.q_gsprofit_margin.fillna(method='pad', inplace=True)
    data.q_gsprofit_margin.fillna(method='bfill', inplace=True)
    if data['q_gsprofit_margin'].isnull().all():
        data.loc[:, 'q_gsprofit_margin_poly'] = np.nan
    else:
        gs = data.q_gsprofit_margin[0:3].append(data.q_gsprofit_margin)  # 凑够长度，反正前几条数据也没有计算意义
        data.loc[:, 'q_gsprofit_margin_poly'] = [RegUtil.calc_regress_deg(gs[i:i + 4], show=False) for i in
                                                 range(data.q_gsprofit_margin.shape[0])]  # 计算连续4季度(单季度)毛利趋势

    data.gross_margin.fillna(method='pad', inplace=True)
    data.gross_margin.fillna(method='bfill', inplace=True)
    if data['gross_margin'].isnull().all():
        data.loc[:, 'gross_margin_poly'] = np.nan
    else:
        gs = data.gross_margin[0:3].append(data.gross_margin)  # 凑够长度，反正前几条数据也没有计算意义
        data.loc[:, 'gross_margin_poly'] = [RegUtil.calc_regress_deg(gs[i:i + 4], show=False) for i in
                                            range(data.gross_margin.shape[0])]  # 计算连续4季度毛利趋势

    data.inv_turn.fillna(method='pad', inplace=True)
    data.inv_turn.fillna(method='bfill', inplace=True)
    if data['inv_turn'].isnull().all():
        data.loc[:, 'inv_turn_poly'] = np.nan
    else:
        gs = data.inv_turn[0:3].append(data.inv_turn)  # 凑够长度，反正前几条数据也没有计算意义
        data.loc[:, 'inv_turn_poly'] = [RegUtil.calc_regress_deg(gs[i:i + 4], show=False) for i in
                                        range(data.inv_turn.shape[0])]  # 计算连续4季度存货周转率趋势

    data.fa_turn.fillna(method='pad', inplace=True)
    data.fa_turn.fillna(method='bfill', inplace=True)
    if data['fa_turn'].isnull().all():
        data.loc[:, 'fa_turn_poly'] = np.nan
    else:
        gs = data.fa_turn[0:3].append(data.fa_turn)  # 凑够长度，反正前几条数据也没有计算意义
        data.loc[:, 'fa_turn_poly'] = [RegUtil.calc_regress_deg(gs[i:i + 4], show=False) for i in
                                       range(data.fa_turn.shape[0])]  # 计算连续4季度固定资产周转率趋势
    return data

    self.finacial = fin_spark.groupby('ts_code').apply(_ver_indicator).collect().toPandas()