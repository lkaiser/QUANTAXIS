# -*- coding: utf-8 -*-
import QUANTAXIS as QA
from QUANTAXIS.QAFetch import QATusharePro as pro
import pandas as pd
import numpy as np
import datetime
import os
from QUANTAXIS.ML import RegUtil
#pd.set_option('display.float_format', lambda x: '%.3f' % x)
#pd.set_option('display.max_columns',5, 'display.max_rows', 100)
class simpleValued:

    def __init__(self,start,end):
        self.start = start
        self.end = end
        self.basic_temp_name = 'basic_temp_' +start+'_'+end +'.pkl'
        start_2years_bf = str(int(start[0:4]) - 3)
        self.finacial = pro.QA_fetch_get_finindicator(start=start_2years_bf,end=end)
        self.income = pro.QA_fetch_get_income(start=start_2years_bf, end=end)
        self.asset = pro.QA_fetch_get_assetAliability(start=start_2years_bf, end=end)
        if (os.path.isfile(self.basic_temp_name)):
            self.basic = pd.read_pickle(self.basic_temp_name)
        else:
            basic = pro.QA_fetch_get_dailyindicator(start=start,end=end)
            #print(basic.head().loc[:,['ts_code','trade_date','close','pe']])
            self.basic = basic.sort_values(['ts_code','trade_date'], ascending = True)
        self.stock = pro.QA_SU_stock_info()
        self.dailymarket = None
        self.industry = None


    def indcators_prepare(self,basic):
        """
            简单价值判断，近1年 roe,近半年roe，近3年净资产增速，近2年净资产增速。价值法6年pb、7年pb
         """
        #daily = pro.QA_fetch_get_dailyindicator(start=start,end=end)

        def _indicator(data):
            #print(data['q_dt_roe'].head())
            if(data['q_dt_roe'].isnull().all()):
                roe_half_year = roe_year = np.array([np.nan] * data.shape[0])
            else:
                roe_year = QA.EMA(data['q_dt_roe'], 4)*4/100+1
                roe_half_year = QA.EMA(data['q_dt_roe'], 2)*4/100+1
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
            data.loc[:,'netasset'] = data.ebit/data.ebit_ps*data.bps #净资产
            data.loc[:,'cash'] = data.ebit/data.ebit_ps*data.cfps #现金流
            data.loc[:,'q_ocf'] = data.q_opincome*data.q_ocf_to_or #单季度经营活动产生的现金流量
            if data['q_ocf'].isnull().all():
                data.loc[:, 'q4_ocf'] = np.nan
            else:
                data.loc[:,'q4_ocf'] = QA.EMA(data['q_ocf'], 4)
            if data['q_opincome'].isnull().all():
                data.loc[:, 'q4_opincome'] = np.nan
            else:
                data.loc[:, 'q4_opincome'] = QA.EMA(data['q_opincome'], 4)
            if data['q_dtprofit'].isnull().all():
                data.loc[:, 'q4_dtprofit'] = np.nan
            else:
                data.loc[:, 'q4_dtprofit'] = QA.EMA(data['q_dtprofit'], 4)
            return data

        self.finacial = self.finacial.groupby('ts_code').apply(_indicator)

        def _indicatorCp(data):
            fin = self.finacial[self.finacial.ts_code==data.name]
            #ast = self.asset.loc[self.asset.ts_code==data.name]
            for i in range(0, fin.shape[0]):
                if i+1 < fin.shape[0]:
                    query = (data.trade_date >= fin.iloc[i].ann_date) & (data.trade_date < fin.iloc[i + 1].ann_date)
                    data.loc[query, ['equity2_pb7']] = fin.iloc[i].equity2_pb7
                    data.loc[query, ['equity3_pb7']] = fin.iloc[i].equity3_pb7
                    data.loc[query, ['roe_half_year_pb7']] = fin.iloc[i].roe_half_year_pb7
                    data.loc[query, ['roe_year_pb7']] = fin.iloc[i].roe_year_pb7
                    data.loc[query, ['roe_half_year_pb7']] = fin.iloc[i].roe_half_year_pb7
                    data.loc[query, ['equity_rejust']] = np.round(data.loc[query].total_mv / data.loc[query].pb * 10000 /fin.iloc[i].netasset ,2)
                else:
                    query = data.trade_date >= fin.iloc[i].ann_date
                    data.loc[query, ['equity2_pb7']] = fin.iloc[i].equity2_pb7
                    data.loc[query, ['equity3_pb7']] = fin.iloc[i].equity3_pb7
                    data.loc[query, ['roe_half_year_pb7']] = fin.iloc[i].roe_half_year_pb7
                    data.loc[query, ['roe_year_pb7']] = fin.iloc[i].roe_year_pb7
                    data.loc[query, ['equity_rejust']] = np.round(data.loc[query].total_mv / data.loc[query].pb * 10000 / fin.iloc[i].netasset,2)
            #print(basic.loc[:,['equity_pb6','equity_pb7','roe_year_pb6','roe_year_pb7']].head())
            return data

        # 获取每日equity_pb6、roe_year_pb6
        if not (os.path.isfile(self.basic_temp_name)):
            #basic = self.basic
            basic.loc[:, 'equity2_pb7'] = 0
            basic.loc[:, 'equity3_pb7'] = 0
            basic.loc[:, 'roe_year_pb7'] = 0
            basic.loc[:, 'roe_half_year_pb7'] = 0
            basic.loc[:, 'equity_rejust'] = 0
            basic.groupby('ts_code').apply(_indicatorCp)
            basic.to_pickle(self.basic_temp_name)

        return basic

    def non_finacal_top5_valued(self,data):
        """
        简单价值判断，近1年 roe,近半年roe，近3年净资产增速，近2年净资产增速。价值法6年pb、7年pb
        """
        # basic = self.basic[self.basic==df[0].code]
        # dailymarket = self.dailymarket[(self.dailymarket.statype=='all')&(self.dailymarket.trade_date>=basic[0].trade_date)&(self.dailymarket.trade_date<=basic[-1].trade_date)]
        # basic.equity_pb6/basic.pb
        # td = basic.merge(dailymarket[['trade_date', 'equity_pb6top5', 'equity_pb7top5','roe_year_pb6top5','roe_year_pb7top5']], left_on='trade_date', right_on='trade_date', how='left').set_index(basic.index)
        # td
        non_finacial_codes = self.stock[(self.stock.industry != '银行') & (self.stock.industry != '保险')].ts_code.values
        basic = data
        if basic is None:
            basic = self.basic[self.basic.ts_code.isin(non_finacial_codes)]

        def _trash_fiter(df):
            '''垃圾排除大法 剔除商誉过高、现金流不充裕，主营利润占比低、资产负债率过高、存货占比、应收占比'''
            fin = self.finacial[self.finacial.ts_code == df.name]
            ast = self.asset.loc[self.asset.ts_code == df.name]
            rm = None
            rms = []
            if fin.shape[0]:
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
                rms.append((rm, self.end))
                rm = None

            for i in range(fin.shape[0]): #逻辑同上面的ast，可以和ast里的日期重复，但凡不符合都删除
                if fin.iloc[i].rmflag == 1 and not rm:
                    rm = fin.iloc[i].ann_date
                if fin.iloc[i].rmflag == 0 and not rm:
                    rms.append((rm, fin.iloc[i].ann_date))
                    rm = None
            if not rm:
                rms.append((rm, self.end))
                rm = None

            data = df
            for k in rms:
                data = data[~((data.trade_date >= k[0]) & (data.trade_date < k[1]))]
                return data
        basic = basic.groupby('ts_code',as_index=False).apply(_trash_fiter)

        basic = self.indcators_prepare(basic)

        # 每日统计指标
        def _dailystat(df):
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
            st = non_finacial.loc[:, ['equity2_pb7_pb', 'equity3_pb7_pb', 'roe_half_year_pb7_pb', 'roe_year_pb7_pb']].describe([.25, .5, .75, .85, .95]).T.reset_index(level=0)
            st.columns = ['category', 'cnt', 'mean', 'std', 'min', 'per25', 'per50', 'per75', 'per85', 'per95', 'max']
            st.loc[:, 'statype'] = 'non_finacial'
            st.index = [df.name] * 4
            # rs.append(st)
            # return pd.concat(rs)
            return st

        # print(self.basic.loc[:,['equity_pb6','equity_pb7','roe_year_pb6','roe_year_pb7']].head())
        # pass
        dailymarket = basic.groupby('trade_date').apply(_dailystat)


        def _top5(df,dailymarket):
            '''equity2_pb7  2年净资产增速对应pb, /实际pb 得出价值倍数，找出价值倍数大于95%数据，这个指标十有八九不靠谱,还不如用roe_year_pb7'''
            if df.name in dailymarket.index.levels[0]:
                dailymarket = dailymarket.loc[df.name]
                if (dailymarket[dailymarket.statype == 'non_finacial'].shape[0] == 0):
                    print(dailymarket)
                else:
                    dailymarket = dailymarket[dailymarket.statype == 'non_finacial']
                    df.loc[:, 'buy'] = df.equity2_pb7 / df.pb - dailymarket[dailymarket.category == 'equity2_pb7_pb'].per95[0]
                    df.loc[:, 'sell'] = dailymarket[dailymarket.category == 'equity2_pb7_pb'].per95[0] - df.equity2_pb7 / df.pb - 0.3
                    df.loc[:, 'roe_buy'] = df.roe_year_pb7 / df.pb - dailymarket[dailymarket.category == 'roe_year_pb7_pb'].per95[0]
                    df.loc[:, 'roe_sell'] = dailymarket[dailymarket.category == 'roe_year_pb7_pb'].per95[0] - df.roe_year_pb7 / df.pb - 0.3
                    df.loc[:, 'half_roe_buy'] = df.roe_half_year_pb7 / df.pb - dailymarket[dailymarket.category == 'roe_half_year_pb7_pb'].per95[0]
                    df.loc[:, 'half_roe_sell'] = dailymarket[dailymarket.category == 'roe_half_year_pb7_pb'].per95[0] - df.roe_half_year_pb7 / df.pb - 0.3
                    return df

        return basic.groupby('trade_date',as_index=False).apply(_top5,dailymarket=dailymarket).set_index(['trade_date', 'ts_code'],drop=False)
        #return basic.groupby(level=1, sort=False).apply(_top5).set_index(['trade_date', 'ts_code'])


    def industry_trend_top10(self,data):
        industry_daily = pro.QA_fetch_get_finindicator(start=self.start-3, end=self.end).sort_values(['ts_code','trade_date'], ascending = True)
        def _trend(data):
            '''
            行业趋势 计算总市值,流通市值,总扣非盈利,总净资产,总资产,总成交量,
            :param data:
            :return:
            '''
            df = data[data.trade_date>=self.start]
            dates = [str(int(self.start.trade_date[0:4]) - 2) + '0430', str(int(self.start.trade_date[0:4]) - 2) + '0830',
                     str(int(self.start.trade_date[0:4]) - 2) + '1031', str(int(self.start.trade_date[0:4]) - 1) + '0430',
                     str(int(self.start.trade_date[0:4]) - 1) + '0830', str(int(self.start.trade_date[0:4]) - 1) + '1031']
            resampledf = dates.apply(lambda x, y: y[y.trade_date < x].tail(1), y=data)

            indicator = pd.DataFrame(columns=['trade_date','industry','q_gr_ttm_poly','q_dtprofit_ttm_poly','q_opincome_ttm_poly','q_gr_poly','q_profit_poly','q_dtprofit_poly','q_opincome_poly','roe','pe'])
            for index,item in df.iterrows:
            # roe 、总资产、净利润、货币资金、存货、净资产类同处理

                if item.trade_date[4:8] <= "0831" and item.trade_date[4:8] > "0431" and item.trade_date[0:4] + '0431' not in dates:
                    dates.append([item.trade_date[0:4] + '0431'].apply(lambda x, y: y[y.trade_date < x].tail(1), y=data))
                    resampledf.append([item.trade_date[0:4] + '0431', item.trade_date])
                if item.trade_date[4:8] <= "1231" and item.trade_date[4:8] > "0831" and item.trade_date[0:4] + '0831' not in dates:
                    dates.append([item.trade_date[0:4] + '0831'].apply(lambda x, y: y[y.trade_date < x].tail(1), y=data))
                    resampledf.append([item.trade_date[0:4] + '0831', item.trade_date])
                resample = resampledf.append(item.trade_date.apply(lambda x, y: y[y.trade_date < x].tail(1), y=data))
                fit, p1 = RegUtil.regress_y_polynomial(resample[-8:].q_gr_ttm, poly=3, show=False)
                fit, p2 = RegUtil.regress_y_polynomial(resample[-8:].q_profit_ttm, poly=3, show=False)
                fit, p3 = RegUtil.regress_y_polynomial(resample[-8:].q_dtprofit_ttm, poly=3, show=False)
                fit, p4 = RegUtil.regress_y_polynomial(resample[-8:].q_opincome_ttm, poly=3, show=False)
                fit, p5 = RegUtil.regress_y_polynomial(resample[-8:].q_gr, poly=3, show=False)
                fit, p6 = RegUtil.regress_y_polynomial(resample[-8:].q_profit, poly=3, show=False)
                fit, p7 = RegUtil.regress_y_polynomial(resample[-8:].q_dtprofit, poly=3, show=False)
                fit, p8 = RegUtil.regress_y_polynomial(resample[-8:].q_opincome, poly=3, show=False)
                roe = item.q_dtprofit_ttm / item.total_hldr_eqy_exc_min_int
                pe = item.ind_total_mv/item.q_dtprofit_ttm
                indicator.loc[index] = [item.trade_date,item.name,p1(8),p2(8),p3(8),p4(8),p5(8),p6(8),p7(8),p8(8),roe,pe]
            return df
        industry_daily = industry_daily.groupby("industry").apply(_trend)
        df = pd.merge(data, self.stock.loc[:, ['ts_code', 'industry']], left_on=['ts_code','trade_date'], right_on='ts_code', how="inner")  # 剔除缺少行业的
        df = pd.merge(df, industry_daily, left_on=['industry', 'trade_date'], right_on=['industry', 'date'], how="inner")  # 剔除行业样本太少的


        # 每日统计指标
        def _dailystat(df):
            st = df.loc[:, ['q_gr_ttm_poly','q_dtprofit_ttm_poly','q_opincome_ttm_poly','q_gr_poly','q_profit_poly','q_dtprofit_poly','q_opincome_poly','roe','pe']].describe([.25, .5, .75, .85, .95]).T.reset_index(level=0)
            st.columns = ['category', 'cnt', 'mean', 'std', 'min', 'per25', 'per50', 'per75', 'per85', 'per95', 'max']
            st.index = [df.name] * 9
            return st

        dailymarket = industry_daily.groupby('trade_date').apply(_dailystat)

        def _top10(df,dailymarket):
            if df.name in dailymarket.index.levels[0]:
                dailymarket = dailymarket.loc[df.name]
                df.loc[:, 'industry_roe_buy'] = df.roe - dailymarket[dailymarket.category == 'roe'].per95[0]
                df.loc[:, 'industry_pe_buy'] = df.roe - dailymarket[dailymarket.category == 'pe'].per85[0]
                df.loc[:, 'q_dtprofit_poly'] = df.roe - dailymarket[dailymarket.category == 'q_dtprofit_poly'].per85[0]
            pass

        return df.groupby('trade_date', as_index=False).apply(_top10, dailymarket=dailymarket).set_index(['trade_date', 'ts_code'], drop=False)

    def simpleStrategy(self):
        df = self.non_finacal_top5_valued()
        df = self.industry_trend_top10(df)
        #df.loc[:'f_buy'] =
        return df




    def price_trend(self,df):

        pass

if __name__ == '__main__':
    #finacial = pro.QA_fetch_get_finindicator(start='20100101', end='20181231',code=['006160.SH','002056.SZ'])
    # sv  = simpleValued('20180101','20180930')
    # sv.indcators_prepare()
    # df = sv.non_finacal_top5_valued()

    #asset = pro.QA_fetch_get_assetAliability(start='20160101', end='20180930')
    #print(asset.head())
    print(QA.EMA(pd.Series([3.1,4.1,5.1,6.1,7.1,8.1,9.1,10.1]),4))


    #fin = finacial.groupby('ts_code').apply(_indicator)
