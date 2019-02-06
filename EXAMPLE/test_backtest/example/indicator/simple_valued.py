# -*- coding: utf-8 -*-
import QUANTAXIS as QA
from QUANTAXIS.QAFetch import QATusharePro as pro
import pandas as pd
import numpy as np

class simpleValued:

    def __init__(self,start,end):
        start_2years_bf = str(int(start) - 2)
        self.finacial = pro.QA_fetch_get_finindicator(start=start_2years_bf,end=end)
        self.asset = pro.QA_fetch_get_assetAliability(start=start_2years_bf, end=end)
        self.basic = pro.QA_fetch_get_dailyindicator(start=start,end=end)
        self.stock = pro.QA_SU_stock_info()
        self.dailymarket = None


    def indcators_prepare(self):
        """
            简单价值判断，近1年 roe,近半年roe，近3年净资产增速，近2年净资产增速。价值法6年pb、7年pb
         """
        #daily = pro.QA_fetch_get_dailyindicator(start=start,end=end)

        def _indicator(data):
            #print(data['q_dt_roe'].head())
            if(data['q_dt_roe'].isnull().all()):
                roe_half_year = roe_year = np.array([np.nan] * data.shape[0])
            else:
                roe_year = QA.EMA(data['q_dt_roe'], 4)*4
                roe_half_year = QA.EMA(data['q_dt_roe'], 2)*4
            #近2年净资产收益率
            asset_rise_2year = data['equity_yoy'].shift(4)*data['equity_yoy']
            # 近3年净资产收益率
            asset_rise_3year = data['equity_yoy'].shift(8) * asset_rise_2year
            data.loc[:,'equity_pb6'] = np.power(asset_rise_3year,6);
            data.loc[:, 'equity_pb7'] = np.power(asset_rise_3year, 7);
            data.loc[:, 'roe_year_pb6'] = np.power(roe_year, 6);
            data.loc[:, 'roe_year_pb7'] = np.power(roe_year, 7);
            return data

        self.finacial = self.finacial.groupby('ts_code').apply(_indicator)


        basic = self.basic
        basic.loc[:,'equity_pb6'] = 0
        basic.loc[:,'equity_pb7'] = 0
        basic.loc[:,'roe_year_pb6'] = 0
        basic.loc[:,'roe_year_pb7'] = 0
        basic.loc[:,'equity_rejust'] = 0

        def _indicatorCp(data):
            fin = self.finacial[self.finacial.ts_code==data.name]
            ast = self.asset.loc[self.asset.ts_code==data.name]
            for i in range(0, len(fin)):
                #equity_rejust
                #print(index)
                #print(index)
                #print( '@@@@@fin.rows='+str(fin.shape[0]))
                if i+1 < fin.shape[0]:
                    query = (data.trade_date >= fin.iloc[i].ann_date) & (data.trade_date < fin.iloc[i + 1].ann_date)
                    data.loc[query, ['equity_pb6']] = fin.iloc[i].equity_pb6
                    data.loc[query, ['equity_pb7']] = fin.iloc[i].equity_pb7
                    data.loc[query, ['roe_year_pb6']] = fin.iloc[i].roe_year_pb6
                    data.loc[query, ['roe_year_pb7']] = fin.iloc[i].roe_year_pb7
                    data.loc[query, ['equity_rejust']] = data.loc[query].total_mv / data.loc[query].pb * 10000 / ast[ast.ann_date == fin.iloc[i].ann_date].total_hldr_eqy_exc_min_int
                else:
                    query = data.trade_date >= fin.iloc[i].ann_date
                    data.loc[query, ['equity_pb6']] = fin.iloc[i].equity_pb6
                    data.loc[query, ['equity_pb7']] = fin.iloc[i].equity_pb7
                    data.loc[query, ['roe_year_pb6']] = fin.iloc[i].roe_year_pb6
                    data.loc[query, ['roe_year_pb7']] = fin.iloc[i].roe_year_pb7
                    data.loc[query, ['equity_rejust']] = data.loc[query].total_mv / data.loc[query].pb * 10000 / ast[ast.ann_date == fin.iloc[i].ann_date].total_hldr_eqy_exc_min_int
            #print(basic.loc[:,['equity_pb6','equity_pb7','roe_year_pb6','roe_year_pb7']].head())
            return data

        # 获取每日equity_pb6、roe_year_pb6
        self.basic = basic.groupby('ts_code').apply(_indicatorCp)



        #每日统计指标
        def _dailystat(df):
            rs = []
            non_finacial_codes = self.stock[(self.stock != '银行') & (self.stock != '证券')].ts_code.values
            non_finacial = df[~df.ts_code.isin(non_finacial_codes)]
            st = non_finacial.loc[:,['equity_pb6','equity_pb7','roe_year_pb6','roe_year_pb7']].describe([.05, .15, .25,.5,.75]).T.reset_index(level=0)
            st.loc[:,'statype'] ='non_finacial'
            st.index = [df.name]*4
            rs.append(st)
            return pd.concat(rs)
        #print(self.basic.loc[:,['equity_pb6','equity_pb7','roe_year_pb6','roe_year_pb7']].head())
        self.dailymarket = self.basic.groupby('trade_date').apply(_dailystat)



    def top5_valued(self,df):
        """
        简单价值判断，近1年 roe,近半年roe，近3年净资产增速，近2年净资产增速。价值法6年pb、7年pb
        """
        # basic = self.basic[self.basic==df[0].code]
        # dailymarket = self.dailymarket[(self.dailymarket.statype=='all')&(self.dailymarket.trade_date>=basic[0].trade_date)&(self.dailymarket.trade_date<=basic[-1].trade_date)]
        # basic.equity_pb6/basic.pb
        # td = basic.merge(dailymarket[['trade_date', 'equity_pb6top5', 'equity_pb7top5','roe_year_pb6top5','roe_year_pb7top5']], left_on='trade_date', right_on='trade_date', how='left').set_index(basic.index)
        # td
        def _top5(df):
            dailymarket = self.dailymarket[(self.dailymarket.statype == 'non-finacial') & (self.dailymarket.trade_date == df[0].trade_date)]
            buy = df.equity_pb6/df.pb - dailymarket.equity_pb6top5
            sell = df.equity_pb6/df.pb - dailymarket.equity_pb6top15
            return pd.DataFrame({'buy': buy, 'sell': sell})
        return self.basic.groupby(level=1, sort=False).apply(_top5).set_index(['trade_date', 'ts_code'])

if __name__ == '__main__':
    #finacial = pro.QA_fetch_get_finindicator(start='20100101', end='20181231',code=['006160.SH','002056.SZ'])
    sv  = simpleValued('20180101','20180930')
    sv.indcators_prepare()

    #asset = pro.QA_fetch_get_assetAliability(start='20160101', end='20180930')
    #print(asset.head())

    def _indicator(data):
        #print(data.head())
        roe_year = QA.EMA(data['q_dt_roe'], 4) * 4
        roe_half_year = QA.EMA(data['q_dt_roe'], 2) * 4
        # 近2年净资产收益率
        asset_rise_2year = data['equity_yoy'].shift(4) * data['equity_yoy']
        # 近3年净资产收益率
        asset_rise_3year = data['equity_yoy'].shift(8) * asset_rise_2year
        equity_pb6 = np.power(asset_rise_3year, 6);
        equity_pb7 = np.power(asset_rise_3year, 7);
        roe_year_pb6 = np.power(roe_year, 6);
        roe_year_pb7 = np.power(roe_year, 7);
        return pd.DataFrame({'roe_year': roe_year, 'roe_half_year': roe_half_year, 'asset_rise_2year': asset_rise_2year, 'asset_rise_3year': asset_rise_3year, 'equity_pb6': equity_pb6, 'roe_year_pb6': roe_year_pb6})


    #fin = finacial.groupby('ts_code').apply(_indicator)
