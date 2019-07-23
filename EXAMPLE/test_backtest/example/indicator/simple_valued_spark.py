# -*- coding: utf-8 -*-
import QUANTAXIS as QA
from QUANTAXIS.QAFetch import QATusharePro as pro
import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark import SparkContext,SparkConf
from pyspark.sql.session import SparkSession
from QUANTAXIS.ML import RegUtil
from pyspark.sql.types import StructType,DoubleType,StructField,StringType
#from pyspark.sql.functions import
import copy
import talib
import logging
import time

import os
import tushare as ts
ts.set_token('336338dd7818a35bcf3e313c120ec8e36328cdfd2df0ea820a534bb4')
#pd.set_option('display.float_format', lambda x: '%.3f' % x)
#pd.set_option('display.max_columns',5, 'display.max_rows', 100)
# ^t  制表符   urtraedit 默认用2个空格表示，python命令行中执行需要将制表符和空格统一，要么都用空格，要么都用制表符
spark = SparkSession.builder.appName("my app").getOrCreate()
log4jLogger = spark._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger('__FILE__')
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
class simpleValued:
    def __init__(self,start,end):
        self.start = start
        self.end = end
        self.basic_temp_name = '/usr/local/spark/basic_temp_' +start+'_'+end +'.csv'
        if not (os.path.isfile(self.basic_temp_name)):
            start_2years_bf = str(int(start[0:4]) - 3)
            self.finacial = pro.QA_fetch_get_finindicator(start=start_2years_bf,end=end)
            self.income = pro.QA_fetch_get_income(start=start_2years_bf, end=end)
            self.asset = pro.QA_fetch_get_assetAliability(start=start_2years_bf, end=end)
            basic = pro.QA_fetch_get_dailyindicator(start=start, end=end).sort_values(['ts_code','trade_date'], ascending = True)
            self.basic = spark.createDataFrame(basic)
            self.basic.repartition('ts_code')
            self.basic.cache()
            self.stock = pro.QA_SU_stock_info()
            self.dailymarket = None
            self.industry = None


    def indcators_prepare(self,basic):
        """
            简单价值判断，近1年 roe,近半年roe，近3年净资产增速，近2年净资产增速。价值法6年pb、7年pb
         """
        #daily = pro.QA_fetch_get_dailyindicator(start=start,end=end)
        fin_spark = spark.createDataFrame(self.finacial)
        fin_spark.cache()
        fin_spark.repartition('ts_code')
        p_struct = ['roe_year_pb7', 'roe_half_year_pb7', 'cash','q_ocf','q4_ocf','q4_opincome','q4_dtprofit','gross_margin_poly',  'q_gsprofit_margin_poly', 'inv_turn_poly', 'fa_turn_poly']
        p = copy.deepcopy(fin_spark.schema)
        list(map(lambda x: p.add(StructField(x, DoubleType())), p_struct))
        @pandas_udf(p, PandasUDFType.GROUPED_MAP)
        def _ver_indicator(data):
            '''
            纵轴指标
            :param data:
            :return:
            '''
            #data = data.sort_values(by='trade_date')
            if (data['q_dt_roe'].isnull().all()):
                roe_half_year = roe_year = np.array([np.nan] * data.shape[0])
            else:
                t = data['q_dt_roe'].fillna(method='bfill')  # 后面的数据向前填充
                t = t.fillna(method='pad') #前向填充
                roe_year = (talib.EMA(t, 4) * 4 / 100 + 1).fillna(method='bfill')
                roe_half_year = (talib.EMA(t, 2) * 4 / 100 + 1).fillna(method='bfill')
            #近2年净资产收益率
            #asset_rise_2year = (data['equity_yoy'].shift(4)/100+1)*(data['equity_yoy']/100+1)
            # 近3年净资产收益率
            #asset_rise_3year = (data['equity_yoy'].shift(8)/100+1) * asset_rise_2year
            #data.loc[:,'equity3_pb7'] = np.round(np.power(np.power(asset_rise_3year,1/3),7),2)
            #print(len(asset_rise_3year[asset_rise_3year.isnull()]))
            #data.loc[:, 'equity2_pb7'] = np.round(np.power(np.power(asset_rise_2year,1/2), 7),2)
            #data.loc[:, 'roe_year_pb6'] = np.round(np.power(roe_year, 6),2)
            data.loc[:, 'roe_year_pb7'] = np.round(np.power(roe_year, 7),2)
            data.loc[:, 'roe_half_year_pb7'] = np.round(np.power(roe_half_year, 7), 2)

            ''' 
            fcff  float 自由现金流
            ocfps 	float	每股经营活动产生的现金流量净额
            cfps	float	每股现金流量净额
            ebit_ps	float	每股息税前利润
            ebit 息税前利润
            bps  每股净资产
            profit_dedt 扣除非经常性损益后的净利润
            q_dtprofit 扣除非经常损益后的单季度净利润
            q_opincome 经营活动单季度净收益
            q_ocf_to_or 经营活动产生的现金流量净额／经营活动净收益(单季度)
            '''
            #data.loc[:, 'netasset'] = data.ebit / data.ebit_ps * data.bps  # 净资产
            data.q_opincome.fillna(method='bfill',inplace = True)
            data.q_opincome.fillna(method='pad', inplace=True)
            data.q_ocf_to_or.fillna(method='bfill', inplace=True)
            data.q_ocf_to_or.fillna(method='pad', inplace=True)
            data.q_dtprofit.fillna(method='bfill', inplace=True)
            data.q_dtprofit.fillna(method='pad', inplace=True)
            data.loc[:, 'q_ocf'] = data.q_opincome * data.q_ocf_to_or  # 单季度经营活动产生的现金流量
            data.loc[:, 'cash'] = data.ebit / data.ebit_ps * data.cfps  # 现金流
            if data['q_ocf'].isnull().all():
                data.loc[:, 'q4_ocf'] = np.nan
            else:
                data.loc[:, 'q4_ocf'] = (talib.EMA(data['q_ocf'], 4)).fillna(method='bfill')  # QA.EMA(data['q_ocf'], 4)
            if data['q_opincome'].isnull().all():
                data.loc[:, 'q4_opincome'] = np.nan
            else:
                data.loc[:, 'q4_opincome'] = (talib.EMA(data['q_opincome'], 4)).fillna(method='bfill')
            if data['q_dtprofit'].isnull().all():
                data.loc[:, 'q4_dtprofit'] = np.nan
            else:
                data.loc[:, 'q4_dtprofit'] = (talib.EMA(data['q_dtprofit'], 4)).fillna(method='bfill')
            data.q_gsprofit_margin.fillna(method='pad', inplace=True)
            data.q_gsprofit_margin.fillna(method='bfill', inplace=True)
            if data['q_gsprofit_margin'].isnull().all():
                data.loc[:, 'q_gsprofit_margin_poly'] = np.nan
            else:
                gs = data.q_gsprofit_margin[0:3].append(data.q_gsprofit_margin)#凑够长度，反正前几条数据也没有计算意义
                data.loc[:,'q_gsprofit_margin_poly'] = [RegUtil.calc_regress_deg(gs[i:i+4], show=False) for i in range(data.q_gsprofit_margin.shape[0])]#计算连续4季度(单季度)毛利趋势
            data.gross_margin.fillna(method='pad', inplace=True)
            data.gross_margin.fillna(method='bfill', inplace=True)
            if data['gross_margin'].isnull().all():
                data.loc[:, 'gross_margin_poly'] = np.nan
            else:
                gs = data.gross_margin[0:3].append(data.gross_margin)  # 凑够长度，反正前几条数据也没有计算意义
                data.loc[:, 'gross_margin_poly'] = [RegUtil.calc_regress_deg(gs[i:i + 4], show=False) for i in range(data.gross_margin.shape[0])]  # 计算连续4季度毛利趋势
            data.inv_turn.fillna(method='pad', inplace=True)
            data.inv_turn.fillna(method='bfill', inplace=True)
            if data['inv_turn'].isnull().all():
                data.loc[:, 'inv_turn_poly'] = np.nan
            else:
                gs = data.inv_turn[0:3].append(data.inv_turn)  # 凑够长度，反正前几条数据也没有计算意义
                data.loc[:, 'inv_turn_poly'] = [RegUtil.calc_regress_deg(gs[i:i + 4], show=False) for i in range(data.inv_turn.shape[0])]  # 计算连续4季度存货周转率趋势
            data.fa_turn.fillna(method='pad', inplace=True)
            data.fa_turn.fillna(method='bfill', inplace=True)
            if data['fa_turn'].isnull().all():
                data.loc[:, 'fa_turn_poly'] = np.nan
            else:
                gs = data.fa_turn[0:3].append(data.fa_turn)  # 凑够长度，反正前几条数据也没有计算意义
                data.loc[:, 'fa_turn_poly'] = [RegUtil.calc_regress_deg(gs[i:i + 4], show=False) for i in range(data.fa_turn.shape[0])]  # 计算连续4季度固定资产周转率趋势
            return data
        self.finacial = fin_spark.groupby('ts_code').apply(_ver_indicator).toPandas() #不是toPandas 慢，是延迟计算的原因，transform里的操作耗费3分钟
        #self.finacial.to_csv('/usr/local/spark/modified.finace-2018.csv')
        #self.finacial = pd.read_csv('/usr/local/spark/modified.finace-2018.csv')


        # def _hor_indicator(data):
        #     '''
        #     横轴指标
        #     :param data:
        #     :return:
        #     '''
        #     print('aaa')
        #     pass
        add_struct = ['roe_year_pb7','roe_half_year_pb7','q_dt_roe','gross_margin_poly','roe_yearly','q_gsprofit_margin_poly','inv_turn_poly','fa_turn_poly','opincome_of_ebt','dtprofit_to_profit','ocf_to_opincome','debt_to_assets','op_to_ebt','tbassets_to_totalassets']
        p1 = copy.deepcopy(basic.schema)
        list(map(lambda x: p1.add(StructField(x, DoubleType())), add_struct))
        finacial = self.finacial
        @pandas_udf(p1, PandasUDFType.GROUPED_MAP)
        def _indicatorCp(key,data):
            df = pd.concat([data,pd.DataFrame(columns=add_struct,dtype='float')])
            fin = finacial[finacial.ts_code==key[0]]
            for i in range(0, fin.shape[0]):
                if i+1 < fin.shape[0]:
                    query = (df.trade_date >= fin.iloc[i].ann_date) & (df.trade_date < fin.iloc[i + 1].ann_date)
                else:
                    query = df.trade_date >= fin.iloc[i].ann_date
                df.loc[query, ['roe_half_year_pb7']] = fin.iloc[i].roe_half_year_pb7
                df.loc[query, ['roe_year_pb7']] = fin.iloc[i].roe_year_pb7
                df.loc[query, ['q_dt_roe']] = fin.iloc[i].q_dt_roe
                df.loc[query, ['gross_margin_poly']] = fin.iloc[i].gross_margin_poly
                df.loc[query, ['roe_yearly']] = fin.iloc[i].roe_yearly
                df.loc[query, ['q_gsprofit_margin_poly']] = fin.iloc[i].q_gsprofit_margin_poly
                df.loc[query, ['inv_turn_poly']] = fin.iloc[i].inv_turn_poly
                df.loc[query, ['fa_turn_poly']] = fin.iloc[i].fa_turn_poly
                df.loc[query, ['opincome_of_ebt']] = fin.iloc[i].opincome_of_ebt #经营活动净收益/利润总额
                df.loc[query, ['dtprofit_to_profit']] = fin.iloc[i].dtprofit_to_profit #扣除非经常损益后的净利润/净利润
                df.loc[query, ['ocf_to_opincome']] = fin.iloc[i].ocf_to_opincome #经营活动产生的现金流量净额/经营活动净收益
                df.loc[query, ['debt_to_assets']] = fin.iloc[i].debt_to_assets  # 资产负债率
                df.loc[query, ['op_to_ebt']] = fin.iloc[i].op_to_ebt  # 营业利润／利润总额
                df.loc[query, ['tbassets_to_totalassets']] = fin.iloc[i].tbassets_to_totalassets  # 有形资产/总资产



            #print(basic.loc[:,['equity_pb6','equity_pb7','roe_year_pb6','roe_year_pb7']].head())
            return df

            #basic = self.basic
        #  basic 日交易基本数据
        # 增加 equity2_pb7、equity3_pb7、roe_year_pb7等几列指标,_indicatorCp运算时间过长，要20多分钟，需要
        # 对中间结果加以保存,后续考虑通过并发框架实施
        return basic.groupby('ts_code').apply(_indicatorCp)

    def non_finacal_top5_valued(self,data=None):
        """
        简单价值判断，近1年 roe,近半年roe，近3年净资产增速，近2年净资产增速。价值法6年pb、7年pb
        """
        # basic = self.basic[self.basic==df[0].code]
        # dailymarket = self.dailymarket[(self.dailymarket.statype=='all')&(self.dailymarket.trade_date>=basic[0].trade_date)&(self.dailymarket.trade_date<=basic[-1].trade_date)]
        # basic.equity_pb6/basic.pb
        # td = basic.merge(dailymarket[['trade_date', 'equity_pb6top5', 'equity_pb7top5','roe_year_pb6top5','roe_year_pb7top5']], left_on='trade_date', right_on='trade_date', how='left').set_index(basic.index)
        # td
        #log = log4jLogger.LogManager.getLogger('__FILE__')
        #log('#############break###############')

        def _before_fiter(data,stock,fin):
            '''
            非
            :param data:
            :param stock:
            :param fin:
            :return:
            '''
            non_finacial_codes = stock[(stock.industry != '银行') & (stock.industry != '保险')].ts_code.values
            basic = data.filter(data.ts_code.isin(non_finacial_codes.tolist()))
            #non_finacial_codes = stock[(stock.industry != '银行') & (stock.industry != '保险')].ts_code.values
            #basic = data[data.ts_code.isin(non_finacial_codes)]

            return basic

        basic = self.basic if data is None else data
        basic.cache()
        #print(basic)
        basic = _before_fiter(basic,self.stock,self.finacial)
        basic.cache()
        basic = self.indcators_prepare(basic)
        basic.cache()
        #basic.count()

        #p = copy.deepcopy(basic.schema)
        #p.add(StructField('rmflag', DoubleType()))

        finacial = self.finacial #= pd.read_csv('/usr/local/spark/modified.finace-2018.csv')
        asset = self.asset
        end = self.end
        @pandas_udf(basic.schema, PandasUDFType.GROUPED_MAP)
        def _af_fiter(key,df):
            '''
            af_fiter 季度级指标过滤，有些季度级指标不合格，不代表整个周期不合格，这种情况下只需过滤当期不合格的数据
            垃圾排除大法 剔除商誉过高、现金流不充裕，主营利润占比低、资产负债率过高、存货占比、应收占比
            '''
            fin = finacial[finacial.ts_code == key[0]]
            ast = asset[asset.ts_code == key[0]]
            rm = None
            rms = []
            if fin.shape[0]:
                # fin.withColumn('rmflag',0)
                # fin.where('q4_ocf/q4_opincome').map
                fin.loc[:, 'rmflag'] = 0
                fin.loc[fin.q4_ocf/fin.q4_opincome<0.6,'rmflag'] = 1 #经营活动现金流/经营活动净利润 <0.6的不要了
                fin.loc[fin.q4_opincome / fin.q4_dtprofit < 0.7,'rmflag'] = 1  # 经营活动净收益/净利润 <0.7的不要了（投资收益什么的不靠谱）
            if ast.shape[0]:
                ast.loc[:, 'rmflag'] = 0
                ast.loc[ast.goodwill / ast.total_hldr_eqy_exc_min_int > 0.2,'rmflag'] = 1  # 商誉占比
                ast.loc[ast.inventories / ast.total_hldr_eqy_exc_min_int > 0.3,'rmflag'] = 1  # 存货占比
                ast.loc[(ast.notes_receiv + ast.accounts_receiv) / ast.total_hldr_eqy_exc_min_int > 0.2,'rmflag'] = 1  # 应收占比

            for i in range(ast.shape[0]): #举例 20171231 0; 20180331  1;20180630  1;20180930 0;20181231 1  ,则  20180331-20180930之间,20181231-end之间的全删除
                if ast.iloc[i].rmflag == 1 and not rm:
                    rm = ast.iloc[i].ann_date
                if ast.iloc[i].rmflag == 0 and not rm:
                    rms.append((rm, ast.iloc[i].ann_date))
                    rm = None
            if not rm:
                rms.append((rm, end))
                rm = None

            for i in range(fin.shape[0]): #逻辑同上面的ast，可以和ast里的日期重复，但凡不符合都删除
                if fin.iloc[i].rmflag == 1 and not rm:
                    rm = fin.iloc[i].ann_date
                if fin.iloc[i].rmflag == 0 and not rm:
                    rms.append((rm, fin.iloc[i].ann_date))
                    rm = None
            if not rm:
                rms.append((rm, end))
                rm = None

            data = df
            for k in rms:
                data = data[~((data.trade_date >= k[0]) & (data.trade_date < k[1]))]
            return data

        basic = basic.groupby('ts_code').apply(_af_fiter)
        basic.cache()
        #basic.coalesce(1).write.csv(path='/usr/local/spark/modified.basic-2018.csv', header=True, sep=",", mode='overwrite')#write.option("header", "true").csv('/usr/local/spark/modified.basic-2018.csv') #coalesce 合并分区 spark_df.write.csv(path=file, header=True, sep=",", mode='overwrite')
        #basic = spark.read.csv('/usr/local/spark/modified.basic-2018.csv', header=True)
        #basic.toPandas().to_csv('/usr/local/spark/modified2.basic-2018.csv')



        # 每日统计指标
        columns = [ 'cnt', 'mean', 'std', 'min', 'per25', 'per50', 'per85', 'per90', 'per95', 'max']
        p1 = StructType().add("category", StringType(), True).add("statype", StringType(), True).add("trade_date", StringType(), True)
        list(map(lambda x: p1.add(StructField(x, DoubleType())), columns))
        columns.insert(0,'category')
        @pandas_udf(p1, PandasUDFType.GROUPED_MAP)
        def _dailystat(key,df):
            # rs = []
            # non_finacial_codes = self.stock[(self.stock.industry != '银行') & (self.stock.industry != '保险')].ts_code.values
            # non_finacial = df[df.ts_code.isin(non_finacial_codes)]
            non_finacial = df
            non_finacial.loc[:, 'roe_half_year_pb7_pb'] = np.round(non_finacial.loc[:, 'roe_half_year_pb7'] / non_finacial.loc[:, 'pb'], 3)
            non_finacial.loc[:, 'roe_year_pb7_pb'] = np.round(non_finacial.loc[:, 'roe_year_pb7'] / non_finacial.loc[:, 'pb'], 3)
            # 太假的不要，干扰数据，净资产本季报之后发生变化>1.1的排除
            non_finacial = non_finacial.loc[(non_finacial.roe_year_pb7_pb < 11)]
            st = non_finacial.loc[:, ['roe_half_year_pb7_pb', 'roe_year_pb7_pb']].describe([.25, .5, .85, .90, .95]).T.reset_index(level=0)
            st.columns = columns
            st.loc[:, 'statype'] = 'non_finacial'
            st.loc[:, 'trade_date'] = key[0]

            cu = non_finacial.loc[:, ['roe_half_year_pb7_pb', 'roe_year_pb7_pb']]
            median = cu.median()
            mad = abs(cu - median).median()
            cu[cu - (median - mad * 3 * 1.4826) < 0] = np.array((median - mad * 3 * 1.4826).tolist() * cu.shape[0]).reshape((cu.shape[0], cu.columns.size))# 去极值,把小于median - mad * 3 * 1.4826设为median - mad * 3 * 1.4826
            cu[cu - (median + mad * 3 * 1.4826) > 0] = np.array((median + mad * 3 * 1.4826).tolist() * cu.shape[0]).reshape((cu.shape[0], cu.columns.size))# 去极值,把大于median + mad * 3 * 1.4826设为median + mad * 3 * 1.4826

            st2 = cu.describe([.25, .5, .85, .90, .95]).T.reset_index(level=0)
            st2.columns = columns
            st2.loc[:, 'statype'] = 'non_finacial'
            st2.loc[:, 'trade_date'] = key[0]
            st2.category = st2.category + '_mad'
            return pd.concat([st, st2])
            # rs.append(st)
            # return pd.concat(rs)
            #return st

        # print(self.basic.loc[:,['equity_pb6','equity_pb7','roe_year_pb6','roe_year_pb7']].head())
        # pass
        #dailymarket = pd.read_csv('/usr/local/spark/dailymarket-2018.csv')
        dailymarket = basic.groupby('trade_date').apply(_dailystat).toPandas()
        # def _f(s):
        #     print(s)
        #     if s['category'] == 'equity2_pb7_pb':
        #         print(s.per90)
        # dailymarket[dailymarket.statype == 'non_finacial'].apply(_f,axis=1)
        # print(dailymarket.columns)
        # print(dailymarket.head(2))
        # print('#############break###############')
        # dailymarket2 = pd.read_csv('/usr/local/spark/dailymarket-2018.csv')
        # print(dailymarket2.columns)
        # print(dailymarket2.head(2))
        #dailymarket.to_csv('/usr/local/spark/dailymarket-2018.csv')

        add_struct = [ 'roe_buy', 'roe_sell', 'half_roe_buy',
                      'half_roe_sell', 'buy_mad', 'sell_mad', 'roe_buy_mad', 'roe_sell_mad',
                      'half_roe_buy_mad', 'half_roe_sell_mad']
        p2 = copy.deepcopy(basic.schema)
        list(map(lambda x: p2.add(StructField(x, DoubleType())), add_struct))

        @pandas_udf(p2, PandasUDFType.GROUPED_MAP)
        def _top10(key, df2):
            '''equity2_pb7  2年净资产增速对应pb, /实际pb 得出价值倍数，找出价值倍数大于95%数据，这个指标十有八九不靠谱,还不如用roe_year_pb7
                df.equity2_pb7 / df.pb 预计涨幅
            '''
            df = pd.concat([df2, pd.DataFrame(columns=add_struct, dtype='float')])
            dmt = dailymarket[dailymarket.trade_date == key[0]]
            if dmt.shape[0]>0:
                if (dmt[dmt.statype == 'non_finacial'].shape[0] == 0):
                    print(dmt)
                else:
                    dm = dmt[dmt.statype == 'non_finacial']
                    df.loc[:, 'roe_buy'] = df.roe_year_pb7 / df.pb - dm[dm.category == 'roe_year_pb7_pb'].per90.values[0]
                    df.loc[:, 'roe_sell'] = dm[dm.category == 'roe_year_pb7_pb'].per90.values[0] - df.roe_year_pb7 / df.pb - 0.3
                    df.loc[:, 'half_roe_buy'] = df.roe_half_year_pb7 / df.pb - dm[dm.category == 'roe_half_year_pb7_pb'].per90.values[0]
                    df.loc[:, 'half_roe_sell'] = dm[dm.category == 'roe_half_year_pb7_pb'].per90.values[0] - df.roe_half_year_pb7 / df.pb - 0.3

                    df.loc[:, 'roe_buy_mad'] = df.roe_year_pb7 / df.pb - dm[dm.category == 'roe_year_pb7_pb_mad'].per90.values[0]
                    df.loc[:, 'roe_sell_mad'] = dm[dm.category == 'roe_year_pb7_pb_mad'].per90.values[0] - df.roe_year_pb7 / df.pb - 0.3
                    df.loc[:, 'half_roe_buy_mad'] = df.roe_half_year_pb7 / df.pb - dm[dm.category == 'roe_half_year_pb7_pb_mad'].per90.values[0]
                    df.loc[:, 'half_roe_sell_mad'] = dm[dm.category == 'roe_half_year_pb7_pb_mad'].per90.values[0] - df.roe_half_year_pb7 / df.pb - 0.3
            return df

        #print(basic.loc[:,['ts_code','trade_date']].head())
        return basic.groupby('trade_date').apply(_top10)
        #return basic.groupby(level=1, sort=False).apply(_top5).set_index(['trade_date', 'ts_code'])

    def industry_trend_top10(self,data):
        start_3years_bf = str(int(self.start[0:4]) - 3)+self.start[4:8]
        industry_daily = pro.QA_fetch_get_industry_daily(start=start_3years_bf, end=self.end).sort_values(['industry','trade_date'], ascending = True)
        industry_daily = spark.createDataFrame(industry_daily)

        new_struct = ['q_dtprofit_ttm_poly', 'q_gr_poly', 'q_profit_poly', 'q_dtprofit_poly', 'q_opincome_poly', 'industry_roe', 'industry_pe', 'roe_ttm', 'industry_pe_ttm']
        p1 = StructType()
        p1.add(StructField('trade_date', StringType()))
        p1.add(StructField('industry', StringType()))
        list(map(lambda x: p1.add(StructField(x, DoubleType())), new_struct))
        start = self.start
        end = self.end
        @pandas_udf(p1, PandasUDFType.GROUPED_MAP)
        def _trend(key,data):
            '''
            行业趋势 计算总市值,流通市值,总扣非盈利,总净资产,总资产,总成交量,
            :param data:
            :return:
            '''

            dates = [str(int(start[0:4]) - 3) + '0831',str(int(start[0:4]) - 3) + '1031',
                     str(int(start[0:4]) - 2) + '0431', str(int(start[0:4]) - 2) + '0831',
                     str(int(start[0:4]) - 2) + '1031', str(int(start[0:4]) - 1) + '0431',
                     str(int(start[0:4]) - 1) + '0831', str(int(start[0:4]) - 1) + '1031']
            #print(data.iloc[-1])
            _lam_f = lambda x, y: y[y.trade_date <= x].iloc[-1] if y[y.trade_date <= x].shape[0]>0 else None
            resampledf = pd.DataFrame(list(filter(lambda x:x is not None,map(_lam_f, dates,[data]*8))))#dates.apply() #每个行业每天都数据，resampledf 取指定date前的最新一条数据,没有则不取，相当于取季报出来前一天的数据
            #map(lambda x,y:np.where(y[y.a>x].shape[0]>0,y[y.a>x].iloc[-1],None),[3,5],[df]*2)
            col = ['trade_date', 'industry']
            col = col+new_struct
            indicator = pd.DataFrame(columns=col)
            df = data[data.trade_date >= start]
            df.reset_index(drop=True)
            for index,item in df.iterrows():
            # roe 、总资产、净利润、货币资金、存货、净资产类同处理
                #try:
                if item.trade_date[4:8] <= "0831" and item.trade_date[4:8] > "0431" and item.trade_date[0:4] + '0431' not in dates:
                    dates.append([item.trade_date[0:4] + '0431'])
                    t = list(filter(lambda x:x is not None,map(_lam_f, [item.trade_date[0:4] + '0431'],[data])))
                    if t is not None:
                        resampledf = resampledf.append(t)
                if item.trade_date[4:8] <= "1031" and item.trade_date[4:8] > "0831" and item.trade_date[0:4] + '0831' not in dates:
                    dates.append([item.trade_date[0:4] + '0831'])
                    t = list(filter(lambda x: x is not None, map(_lam_f, [item.trade_date[0:4] + '0831'], [data])))
                    if t is not None:
                        resampledf = resampledf.append(t)
                if item.trade_date[4:8] > "1031" and item.trade_date[0:4] + '1031' not in dates:
                    dates.append([item.trade_date[0:4] + '1031'])
                    t = list(filter(lambda x: x is not None, map(_lam_f, [item.trade_date[0:4] + '1031'], [data])))
                    if t is not None:
                        resampledf = resampledf.append(t)
                resample = resampledf.append(list(map(_lam_f, [item.trade_date], [data])))#每次循环最新一天都要替换，所以最新一天不能赋值给 resampledf,只能给resample
                resample = resample.dropna(how='all')
                ind = -8 if resample.shape[0]>8 else -resample.shape[0]
                #print(resample.loc[:, ['industry', 'trade_date', 'q_dtprofit']].head())
                # fit, p1 = RegUtil.regress_y_polynomial(resample[-8:].q_gr_ttm, poly=3, show=False)
                # fit, p2 = RegUtil.regress_y_polynomial(resample[-8:].q_profit_ttm, poly=3, show=False)
                fit, p3 = RegUtil.regress_y_polynomial(resample[ind:].q_dtprofit_ttm, poly=3, show=False)
                # fit, p4 = RegUtil.regress_y_polynomial(resample[-8:].q_opincome_ttm, poly=3, show=False)
                fit, p5 = RegUtil.regress_y_polynomial(resample[ind:].q_gr, poly=3, show=False)
                fit, p6 = RegUtil.regress_y_polynomial(resample[ind:].q_profit, poly=3, show=False)
                fit, p7 = RegUtil.regress_y_polynomial(resample[ind:].q_dtprofit, poly=3, show=False)
                fit, p8 = RegUtil.regress_y_polynomial(resample[ind:].q_opincome, poly=3, show=False)
                roe = item.q_dtprofit / item.total_hldr_eqy_exc_min_int
                pe = item.ind_total_mv*10000/item.q_dtprofit
                roe_ttm = item.q_dtprofit_ttm / item.total_hldr_eqy_exc_min_int
                pe_ttm = item.ind_total_mv*10000/item.q_dtprofit_ttm
                #print(indicator.columns)
                #print([item.trade_date,key[0],p3(8),p5(8),p6(8),p7(8),p8(8),roe,pe,roe_ttm,pe_ttm])
                indicator.loc[index] = [item.trade_date,key[0],p3(8),p5(8),p6(8),p7(8),p8(8),roe,pe,roe_ttm,pe_ttm]
                #print(indicator.loc[index])
            return indicator
        industry_daily = industry_daily.groupby("industry").apply(_trend).cache()
        #cond = [df.name == df3.name, df.age == df3.age]
        stock_spark = spark.createDataFrame(self.stock)
        df = data.join(stock_spark,[ 'ts_code'], "inner")
        df = df.join(industry_daily,['industry', 'trade_date'],"inner").cache()
        #df = pd.merge(data, self.stock.loc[:, ['ts_code', 'industry']], left_on='ts_code', right_on='ts_code', how="inner")  # 找到每只code的行业，剔除缺少行业的
        #df = pd.merge(df, industry_daily, left_on=['industry', 'trade_date'], right_on=['industry', 'trade_date'], how="inner")  # 合并code及其对应的行业数据，剔除行业样本太少的

        #df.to_pickle('test2.pkl')
        # 每日统计指标,数据丢失太严重，17w数据，有2.8w的q_dtprofit丢失，只好用前向或者后向填充，其他指标丢失更严重，失去统计意义
        new2_struct = ['cnt', 'mean', 'std', 'min', 'per25', 'per50', 'per85', 'per90', 'per95', 'max']
        p2 = StructType()
        p2.add(StructField('category', StringType()))
        p2.add(StructField('industry', StringType()))
        list(map(lambda x: p2.add(StructField(x, DoubleType())), new2_struct))
        @pandas_udf(p2, PandasUDFType.GROUPED_MAP)
        def _dailystat(key,df):
            d = df.loc[:, ['q_dtprofit_ttm_poly','q_gr_poly','q_profit_poly','q_dtprofit_poly','q_opincome_poly','industry_roe','industry_pe','roe_ttm','industry_pe_ttm']]
            st = d.describe([.25, .5, .85, .90, .95]).T.reset_index(level=0)
            col = ['category']
            col = col+new2_struct
            st.columns = col
            st.loc[:, 'industry'] = key[0]

            #mad 去极值法
            #第一步，找出所有因子的中位数
            #Xmedian；第二步，得到每个因子与中位数的绝对偏差值
            #Xi−Xmedian；第三步，得到绝对偏差值的中位数
            #MAD；最后，确定参数n，从而确定合理的范围为[Xmedian−nMAD, Xmedian + nMAD]，并针对超出合理范围的因子值做如下的调整：
            median = d.median()
            mad = abs(d - median).median()
            d[d - (median - mad * 3 * 1.4826) < 0] = np.array((median - mad * 3 * 1.4826).tolist()*d.shape[0]).reshape((d.shape[0],d.columns.size))
            d[d - (median + mad * 3 * 1.4826) > 0] = np.array((median + mad * 3 * 1.4826).tolist()*d.shape[0]).reshape((d.shape[0],d.columns.size))

            st2 = d.describe([.25, .5, .85, .90, .95]).T.reset_index(level=0)
            st2.columns = col
            st2.loc[:, 'industry'] = key[0]
            st2.category = st2.category+'_mad'
            return pd.concat([st, st2])

        dailymarket = industry_daily.groupby('trade_date').apply(_dailystat).toPandas()

        add3_struct = ['industry_roe_buy', 'industry_pe_buy', 'q_dtprofit_poly_buy', 'industry_roe_ttm_buy', 'industry_pe_ttm_buy', 'q_dtprofit_ttm_poly_buy', 'industry_roe_buy_mad', 'industry_pe_buy_mad', 'q_dtprofit_poly_buy_mad', 'industry_roe_ttm_buy_mad', 'industry_pe_ttm_buy_mad', 'q_dtprofit_ttm_poly_buy_mad']
        p3 = copy.deepcopy(df.schema)
        list(map(lambda x: p3.add(StructField(x, DoubleType())), add3_struct))
        @pandas_udf(p3, PandasUDFType.GROUPED_MAP)
        def _top10(key, df2):
            df = pd.concat([df2, pd.DataFrame(columns=add3_struct, dtype='float')])
            market = dailymarket[dailymarket.industry == key[0]]
            if market.shape[0]:
                df.loc[:, 'industry_roe_buy'] = df.industry_roe - market[market.category == 'industry_roe'].per90.values[0]
                df.loc[:, 'industry_pe_buy'] = df.industry_pe - market[market.category == 'industry_pe'].per85.values[0]
                df.loc[:, 'q_dtprofit_poly_buy'] = df.q_dtprofit_poly - market[market.category == 'q_dtprofit_poly'].per85.values[0]
                df.loc[:, 'industry_roe_ttm_buy'] = df.roe_ttm - market[market.category == 'roe_ttm'].per90.values[0]
                df.loc[:, 'industry_pe_ttm_buy'] = df.industry_pe_ttm - market[market.category == 'industry_pe_ttm'].per85.values[0]
                df.loc[:, 'q_dtprofit_ttm_poly_buy'] = df.q_dtprofit_ttm_poly - market[market.category == 'q_dtprofit_ttm_poly'].per85.values[0]
                df.loc[:, 'industry_roe_buy_mad'] = df.industry_roe - market[market.category == 'industry_roe_mad'].per90.values[0]
                df.loc[:, 'industry_pe_buy_mad'] = df.industry_pe - market[market.category == 'industry_pe_mad'].per85.values[0]
                df.loc[:, 'q_dtprofit_poly_buy_mad'] = df.q_dtprofit_poly - market[market.category == 'q_dtprofit_poly_mad'].per85.values[0]
                df.loc[:, 'industry_roe_ttm_buy_mad'] = df.roe_ttm - market[market.category == 'roe_ttm_mad'].per90.values[0]
                df.loc[:, 'industry_pe_ttm_buy_mad'] = df.industry_pe_ttm - market[market.category == 'industry_pe_ttm_mad'].per85.values[0]
                df.loc[:, 'q_dtprofit_ttm_poly_buy_mad'] = df.q_dtprofit_ttm_poly - market[market.category == 'q_dtprofit_ttm_poly_mad'].per85.values[0]
            return df
            #pass

        return df.groupby('trade_date').apply(_top10)

    def simpleStrategy(self):
        df = pd.read_csv(self.basic_temp_name).set_index(['trade_date', 'ts_code'], drop=False)
        df.loc[:, 'buy'] = (df.roe_buy > 0) & (df.half_roe_buy > df.roe_buy) & (df.industry_roe_buy > 0 & (df.roe_yearly > 10) & (df.opincome_of_ebt > 85) & (df.debt_to_assets < 70))
        df.loc[:, 'sell'] = df.roe_sell > 0
        return df


if __name__ == '__main__':
    #logger = logging.getLogger('__FILE__')
    #logging.warning('###############wtf#######################')


    # log4jLogger = spark._jvm.org.apache.log4j
    # LOGGER = log4jLogger.LogManager.getLogger(__name__)
    # LOGGER.info('###############wtf2#######################')
    # finacial = pd.read_csv('/usr/local/spark/finace-2018.csv')
    # basic = pd.read_csv('/usr/local/spark/basic-2018.csv')

    #spark.sparkContext.setLogLevel("INFO")

    #df = spark.createDataFrame(basic.loc[:,['ts_code','trade_date']])
    #print('###############fy###############')
    sv = simpleValued('20180101','20181231')
    print(time.strftime("%a %b %d %H:%M:%S %Y", time.localtime()))
    df = sv.non_finacal_top5_valued()
    # basic = pd.read_csv('/usr/local/spark/basic-2018.csv')
    # basic = spark.createDataFrame(basic)
    df = sv.industry_trend_top10(df)
    df2 = df.toPandas()
    df2.set_index(['trade_date', 'ts_code'], drop=False)
    df2.to_csv('/usr/local/spark/result.csv')
    print(time.strftime("%a %b %d %H:%M:%S %Y", time.localtime()))
    # finacial = pd.read_csv('/usr/local/spark/finace-2018.csv')
    # t = time.time()
    # log.warn(t)
    # fin_spark = spark.createDataFrame(finacial)
    # t2 = time.time()
    # log.warn("create daframe spend "+str((t2-t)))
    # df = fin_spark.toPandas()
    # log.warn("toPandas spend "+str((time.time() - t2)))
    # df1 = sv.industry_trend_top10(df)
    # df1.toPandas().set_index(['trade_date', 'ts_code'], drop=False)
    #df1.to_csv('/usr/local/spark/result.csv')
    #sv.udf_test(df)

#D:\work\spark\spark-2.4.3-bin-hadoop2.7\bin\spark-submit --py-files D:\work\QUANTAXIS\quantaxis.zip  D:\work\QUANTAXIS\EXAMPLE\test_backtest\example\indicator\simple_valued_spark.py
#./bin/spark-submit --driver-memory 6G  --conf spark.debug.maxToStringFields=100  --master spark://hadoop1:7077 --py-files /usr/local/spark/quantaxis.zip  /usr/local/spark/simple_valued_spark.py



# conf=SparkConf()
# #conf.setMaster("spark://172.16.17.51:7077")
# conf.setAppName("python test application")
# #
# # # logFile="hdfs://hadoop241:8020/user/root/testfile"
# sc=SparkContext(conf=conf)
# # # logData=sc.textFile(logFile).cache()
# #
#
# lines = sc.textFile("/usr/local/spark/catalina.2019-05-22.out")
# t0 = time()
# #lines.count()
# csv_data  = lines.map(lambda x: x.split("-"))
# csv_data.count()
# head_rows = csv_data.take(5)
# tt = time() - t0
# print("Count completed in {} seconds".format(round(tt,3)))
# print(head_rows)
