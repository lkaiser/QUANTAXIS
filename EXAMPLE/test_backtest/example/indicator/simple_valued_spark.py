# -*- coding: utf-8 -*-
import QUANTAXIS as QA
from QUANTAXIS.QAFetch import QATusharePro as pro
import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark import SparkContext,SparkConf
from pyspark.sql.session import SparkSession
from QUANTAXIS.ML import RegUtil
from pyspark.sql.types import *
from pyspark.sql.functions import *
import copy
import talib

import os
import tushare as ts
ts.set_token('336338dd7818a35bcf3e313c120ec8e36328cdfd2df0ea820a534bb4')
#pd.set_option('display.float_format', lambda x: '%.3f' % x)
#pd.set_option('display.max_columns',5, 'display.max_rows', 100)
# ^t  制表符   urtraedit 默认用2个空格表示，python命令行中执行需要将制表符和空格统一，要么都用空格，要么都用制表符
class simpleValued:
    def __init__(self,start,end):
        self.start = start
        self.end = end
        self.basic_temp_name = 'basic_temp_' +start+'_'+end +'.pkl'
        start_2years_bf = str(int(start[0:4]) - 3)
        self.finacial = pro.QA_fetch_get_finindicator(start=start_2years_bf,end=end)
        self.income = pro.QA_fetch_get_income(start=start_2years_bf, end=end)
        self.asset = pro.QA_fetch_get_assetAliability(start=start_2years_bf, end=end)
        basic = pro.QA_fetch_get_dailyindicator(start=start, end=end).sort_values(['ts_code','trade_date'], ascending = True)
        self.basic = spark.createDataFrame(basic)
        self.stock = pro.QA_SU_stock_info()
        self.dailymarket = None
        self.industry = None

    def indcators_prepare(self,basic):
        """
            简单价值判断，近1年 roe,近半年roe，近3年净资产增速，近2年净资产增速。价值法6年pb、7年pb
         """
        #daily = pro.QA_fetch_get_dailyindicator(start=start,end=end)
        fin_spark = spark.createDataFrame(self.finacial)
        p_struct = ['equity2_pb7', 'equity3_pb7', 'roe_year_pb7', 'roe_half_year_pb7', 'netasset', 'cash','q_ocf','q4_opincome','q_dtprofit','gross_margin_poly',  'q_gsprofit_margin_poly', 'inv_turn_poly', 'fa_turn_poly']
        p = copy.deepcopy(fin_spark.schema)
        list(map(lambda x: p.add(StructField(x, DoubleType())), p_struct))
        @pandas_udf(p, PandasUDFType.GROUPED_MAP)
        def _ver_indicator(data):
            '''
            纵轴指标
            :param data:
            :return:
            '''
            import talib
            if (data['q_dt_roe'].isnull().all()):
                roe_half_year = roe_year = np.array([np.nan] * data.shape[0])
            else:
                t = data['q_dt_roe'].fillna(method='bfill')  # 后面的数据向前填充
                t = t.fillna(method='pad')
                roe_year = (talib.EMA(t, 4) * 4 / 100 + 1).fillna(method='bfill')
                roe_half_year = (talib.EMA(t, 2) * 4 / 100 + 1).fillna(method='bfill')
            #近2年净资产收益率
            asset_rise_2year = (data['equity_yoy'].shift(4)/100+1)*(data['equity_yoy']/100+1)
            # 近3年净资产收益率
            asset_rise_3year = (data['equity_yoy'].shift(8)/100+1) * asset_rise_2year
            data.loc[:,'equity3_pb7'] = np.round(np.power(np.power(asset_rise_3year,1/3),7),2)
            #print(len(asset_rise_3year[asset_rise_3year.isnull()]))
            data.loc[:, 'equity2_pb7'] = np.round(np.power(np.power(asset_rise_2year,1/2), 7),2)
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

        self.finacial = fin_spark.groupby('ts_code').apply(_ver_indicator).collect().toPandas()

        # def _hor_indicator(data):
        #     '''
        #     横轴指标
        #     :param data:
        #     :return:
        #     '''
        #     print('aaa')
        #     pass
        #
        #
        # self.finacial = self.finacial.groupby('trade_date').apply(_hor_indicator)
        # struct_field_map = {'equity2_pb7': StringType(),
        #                     'equity3_pb7': TimestampType(),
        #                     'double': DoubleType(),
        #                     'int': IntegerType(),
        #                     'none': NullType()}
        # fields = [StructField(k, struct_field_map[v], True) for k, v in columns]
        add_struct = ['equity2_pb7','equity3_pb7','roe_year_pb7','roe_half_year_pb7','equity_rejust','q_dt_roe','gross_margin_poly','roe_yearly','q_gsprofit_margin_poly','inv_turn_poly','fa_turn_poly','opincome_of_ebt','dtprofit_to_profit','ocf_to_opincome','debt_to_assets','op_to_ebt','tbassets_to_totalassets']
        p1 = copy.deepcopy(basic.schema)
        list(map(lambda x: p1.add(StructField(x, DoubleType())), add_struct))
        finacial = self.finacial
        @pandas_udf(p1, PandasUDFType.GROUPED_MAP)
        def _indicatorCp(key,data):
            df = pd.concat([data,pd.DataFrame(columns=['equity2_pb7','equity3_pb7','roe_year_pb7','roe_half_year_pb7','equity_rejust','q_dt_roe','gross_margin_poly','roe_yearly','q_gsprofit_margin_poly','inv_turn_poly','fa_turn_poly','opincome_of_ebt','dtprofit_to_profit','ocf_to_opincome','debt_to_assets','op_to_ebt','tbassets_to_totalassets'],dtype='float')])
            fin = finacial[finacial.ts_code==key]
            for i in range(0, fin.shape[0]):
                if i+1 < fin.shape[0]:
                    query = (df.trade_date >= fin.iloc[i].ann_date) & (df.trade_date < fin.iloc[i + 1].ann_date)
                else:
                    query = df.trade_date >= fin.iloc[i].ann_date
                df.loc[query, ['equity2_pb7']] = fin.iloc[i].equity2_pb7
                df.loc[query, ['equity3_pb7']] = fin.iloc[i].equity3_pb7
                df.loc[query, ['roe_half_year_pb7']] = fin.iloc[i].roe_half_year_pb7
                df.loc[query, ['roe_year_pb7']] = fin.iloc[i].roe_year_pb7
                df.loc[query, ['equity_rejust']] = np.round(df.loc[query].total_mv / df.loc[query].pb * 10000 / fin.iloc[i].netasset,2)
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
        #print(basic)
        basic = _before_fiter(basic,self.stock,self.finacial)

        basic = self.indcators_prepare(basic)

        p = copy.deepcopy(basic.schema)
        p.add(StructField('rmflag', DoubleType()))

        finacial = self.finacial
        asset = self.asset
        end = self.end
        @pandas_udf(p, PandasUDFType.GROUPED_MAP)
        def _af_fiter(key,df):
            '''
            af_fiter 季度级指标过滤，有些季度级指标不合格，不代表整个周期不合格，这种情况下只需过滤当期不合格的数据
            垃圾排除大法 剔除商誉过高、现金流不充裕，主营利润占比低、资产负债率过高、存货占比、应收占比
            '''
            fin = finacial[finacial.ts_code == key]
            ast = asset[asset.ts_code == key]
            rm = None
            rms = []
            if fin.count():
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



        # 每日统计指标
        columns = [ 'cnt', 'mean', 'std', 'min', 'per25', 'per50', 'per85', 'per90', 'per95', 'max']
        p1 = StructType().add("category", StringType(), True).add("statype", StringType(), True)
        map(lambda x: p1.add(StructField(x, DoubleType())), columns)
        columns.insert(0,'category')
        @pandas_udf(p1, PandasUDFType.GROUPED_MAP)
        def _dailystat(key,df):
            # rs = []
            # non_finacial_codes = self.stock[(self.stock.industry != '银行') & (self.stock.industry != '保险')].ts_code.values
            # non_finacial = df[df.ts_code.isin(non_finacial_codes)]
            non_finacial = df

            non_finacial.loc[:, 'equity2_pb7_pb'] = np.round(non_finacial.loc[:, 'equity2_pb7'] / non_finacial.loc[:, 'pb'], 3)
            non_finacial.loc[:, 'equity3_pb7_pb'] = np.round(non_finacial.loc[:, 'equity3_pb7'] / non_finacial.loc[:, 'pb'], 3)
            non_finacial.loc[:, 'roe_half_year_pb7_pb'] = np.round(non_finacial.loc[:, 'roe_half_year_pb7'] / non_finacial.loc[:, 'pb'], 3)
            non_finacial.loc[:, 'roe_year_pb7_pb'] = np.round(non_finacial.loc[:, 'roe_year_pb7'] / non_finacial.loc[:, 'pb'], 3)
            # 太假的不要，干扰数据，净资产本季报之后发生变化>1.1的排除
            non_finacial = non_finacial.loc[(non_finacial.equity2_pb7 < 11) & (non_finacial.equity_rejust < 1.1) & (non_finacial.roe_year_pb7_pb < 11)]
            st = non_finacial.loc[:, ['equity2_pb7_pb', 'equity3_pb7_pb', 'roe_half_year_pb7_pb', 'roe_year_pb7_pb']].describe([.25, .5, .85, .90, .95]).T.reset_index(level=0)
            st.columns = columns
            st.loc[:, 'statype'] = 'non_finacial'
            st.index = [key] * 4

            cu = non_finacial.loc[:, ['equity2_pb7_pb', 'equity3_pb7_pb', 'roe_half_year_pb7_pb', 'roe_year_pb7_pb']]
            median = cu.median()
            mad = abs(cu - median).median()
            cu[cu - (median - mad * 3 * 1.4826) < 0] = np.array((median - mad * 3 * 1.4826).tolist() * cu.shape[0]).reshape((cu.shape[0], cu.columns.size))
            cu[cu - (median + mad * 3 * 1.4826) > 0] = np.array((median + mad * 3 * 1.4826).tolist() * cu.shape[0]).reshape((cu.shape[0], cu.columns.size))

            st2 = cu.describe([.25, .5, .85, .90, .95]).T.reset_index(level=0)
            st2.columns = ['category', 'cnt', 'mean', 'std', 'min', 'per25', 'per50', 'per85', 'per90', 'per95', 'max']
            st2.loc[:, 'statype'] = 'non_finacial'
            st2.index = [key] * 4
            st2.category = st2.category + '_mad'
            return pd.concat([st, st2])
            # rs.append(st)
            # return pd.concat(rs)
            #return st

        # print(self.basic.loc[:,['equity_pb6','equity_pb7','roe_year_pb6','roe_year_pb7']].head())
        # pass
        dailymarket = basic.groupby('trade_date').apply(_dailystat)

        add_struct = ['buy', 'sell', 'roe_buy', 'roe_sell', 'half_roe_buy',
                      'half_roe_sell', 'buy_mad', 'sell_mad', 'roe_buy_mad', 'roe_sell_mad',
                      'half_roe_buy_mad', 'half_roe_sell_mad']
        p2 = copy.deepcopy(basic.schema)
        map(lambda x: p2.add(StructField(x, DoubleType())), add_struct)

        @pandas_udf(p2, PandasUDFType.GROUPED_MAP)
        def _top10(key, df):
            '''equity2_pb7  2年净资产增速对应pb, /实际pb 得出价值倍数，找出价值倍数大于95%数据，这个指标十有八九不靠谱,还不如用roe_year_pb7
                df.equity2_pb7 / df.pb 预计涨幅
            '''
            if key in dm.index.levels[0]:
                dm = dm.loc[key]
                if (dm[dm.statype == 'non_finacial'].shape[0] == 0):
                    print(dm)
                else:
                    dm = dm[dm.statype == 'non_finacial']
                    df.loc[:, 'buy'] = df.equity2_pb7 / df.pb - dm[dm.category == 'equity2_pb7_pb'].per90[0]
                    df.loc[:, 'sell'] = dm[dm.category == 'equity2_pb7_pb'].per90[0] - df.equity2_pb7 / df.pb - 0.3
                    df.loc[:, 'roe_buy'] = df.roe_year_pb7 / df.pb - dm[dm.category == 'roe_year_pb7_pb'].per90[0]
                    df.loc[:, 'roe_sell'] = dm[dm.category == 'roe_year_pb7_pb'].per90[
                                                0] - df.roe_year_pb7 / df.pb - 0.3
                    df.loc[:, 'half_roe_buy'] = df.roe_half_year_pb7 / df.pb - \
                                                dm[dm.category == 'roe_half_year_pb7_pb'].per90[0]
                    df.loc[:, 'half_roe_sell'] = dm[dm.category == 'roe_half_year_pb7_pb'].per90[
                                                     0] - df.roe_half_year_pb7 / df.pb - 0.3

                    df.loc[:, 'buy_mad'] = df.equity2_pb7 / df.pb - dm[dm.category == 'equity2_pb7_pb_mad'].per90[0]
                    df.loc[:, 'sell_mad'] = dm[dm.category == 'equity2_pb7_pb_mad'].per90[
                                                0] - df.equity2_pb7 / df.pb - 0.3
                    df.loc[:, 'roe_buy_mad'] = df.roe_year_pb7 / df.pb - dm[dm.category == 'roe_year_pb7_pb_mad'].per90[
                        0]
                    df.loc[:, 'roe_sell_mad'] = dm[dm.category == 'roe_year_pb7_pb_mad'].per90[
                                                    0] - df.roe_year_pb7 / df.pb - 0.3
                    df.loc[:, 'half_roe_buy_mad'] = df.roe_half_year_pb7 / df.pb - \
                                                    dm[dm.category == 'roe_half_year_pb7_pb_mad'].per90[0]
                    df.loc[:, 'half_roe_sell_mad'] = dm[dm.category == 'roe_half_year_pb7_pb_mad'].per90[
                                                         0] - df.roe_half_year_pb7 / df.pb - 0.3
                    return df

        #print(basic.loc[:,['ts_code','trade_date']].head())
        return basic.groupby('trade_date').apply(_top10)#.set_index(['trade_date', 'ts_code'],drop=False)
        #return basic.groupby(level=1, sort=False).apply(_top5).set_index(['trade_date', 'ts_code'])

    def udf_test(self,data):
        @pandas_udf("opincome_of_ebt double, dtprofit_to_profit double,ocf_to_opincome double,debt_to_assets double,op_to_ebt double,tbassets_to_totalassets double", PandasUDFType.GROUPED_MAP)
        def _indicatorCp(key,data):
            df = pd.concat([data, pd.DataFrame(
                columns=['opincome_of_ebt', 'dtprofit_to_profit', 'ocf_to_opincome', 'debt_to_assets', 'op_to_ebt', 'tbassets_to_totalassets'], dtype='float')])
            fin = self.finacial[self.finacial.ts_code == key]
            for i in range(0, fin.shape[0]):
                if i + 1 < fin.shape[0]:
                    query = (df.trade_date >= fin.iloc[i].ann_date) & (df.trade_date < fin.iloc[i + 1].ann_date)
                else:
                    query = df.trade_date >= fin.iloc[i].ann_date
                df.loc[query, ['opincome_of_ebt']] = fin.iloc[i].opincome_of_ebt  # 经营活动净收益/利润总额
                df.loc[query, ['dtprofit_to_profit']] = fin.iloc[i].dtprofit_to_profit  # 扣除非经常损益后的净利润/净利润
                df.loc[query, ['ocf_to_opincome']] = fin.iloc[i].ocf_to_opincome  # 经营活动产生的现金流量净额/经营活动净收益
                df.loc[query, ['debt_to_assets']] = fin.iloc[i].debt_to_assets  # 资产负债率
                df.loc[query, ['op_to_ebt']] = fin.iloc[i].op_to_ebt  # 营业利润／利润总额
                df.loc[query, ['tbassets_to_totalassets']] = fin.iloc[i].tbassets_to_totalassets  # 有形资产/总资产
            return df.loc[:,['opincome_of_ebt', 'dtprofit_to_profit', 'ocf_to_opincome', 'debt_to_assets', 'op_to_ebt', 'tbassets_to_totalassets']]
        data.groupby('ts_code').apply(_indicatorCp).count()

    def pandas_test(self,data):
        def _indicatorCp(data):
            df = pd.concat([data, pd.DataFrame(
                columns=['opincome_of_ebt', 'dtprofit_to_profit', 'ocf_to_opincome', 'debt_to_assets', 'op_to_ebt', 'tbassets_to_totalassets'], dtype='float')])
            fin = self.finacial[self.finacial.ts_code == data.name]
            for i in range(0, fin.shape[0]):
                if i + 1 < fin.shape[0]:
                    query = (df.trade_date >= fin.iloc[i].ann_date) & (df.trade_date < fin.iloc[i + 1].ann_date)
                else:
                    query = df.trade_date >= fin.iloc[i].ann_date
                df.loc[query, ['opincome_of_ebt']] = fin.iloc[i].opincome_of_ebt  # 经营活动净收益/利润总额
                df.loc[query, ['dtprofit_to_profit']] = fin.iloc[i].dtprofit_to_profit  # 扣除非经常损益后的净利润/净利润
                df.loc[query, ['ocf_to_opincome']] = fin.iloc[i].ocf_to_opincome  # 经营活动产生的现金流量净额/经营活动净收益
                df.loc[query, ['debt_to_assets']] = fin.iloc[i].debt_to_assets  # 资产负债率
                df.loc[query, ['op_to_ebt']] = fin.iloc[i].op_to_ebt  # 营业利润／利润总额
                df.loc[query, ['tbassets_to_totalassets']] = fin.iloc[i].tbassets_to_totalassets  # 有形资产/总资产
            return df.loc[:,['opincome_of_ebt', 'dtprofit_to_profit', 'ocf_to_opincome', 'debt_to_assets', 'op_to_ebt', 'tbassets_to_totalassets']]
        data.groupby('ts_code').apply(_indicatorCp)


if __name__ == '__main__':
    print('wtf')
    # finacial = pd.read_csv('/usr/local/spark/finace-2018.csv')
    # basic = pd.read_csv('/usr/local/spark/basic-2018.csv')
    spark = SparkSession.builder.appName("my app").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    #df = spark.createDataFrame(basic.loc[:,['ts_code','trade_date']])
    sv = simpleValued('20180101','20181231')
    sv.non_finacal_top5_valued()
    #sv.udf_test(df)

#D:\work\spark\spark-2.4.3-bin-hadoop2.7\bin\spark-submit --py-files D:\work\QUANTAXIS\quantaxis.zip  D:\work\QUANTAXIS\EXAMPLE\test_backtest\example\indicator\simple_valued_spark.py
#./bin/spark-submit --py-files /usr/local/spark/quantaxis.zip  /usr/local/spark/simple_valued_spark.py



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
