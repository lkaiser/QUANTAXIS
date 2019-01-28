# -*- coding: utf-8 -*-
import QUANTAXIS as QA
from QUANTAXIS.QAFetch import QATusharePro as pro
import pandas as pd
import numpy as np

class simpleValued:


    def allValues(self,start,end):
        """
            简单价值判断，近1年 roe,近半年roe，近3年净资产增速，近2年净资产增速。价值法6年pb、7年pb
         """
        #daily = pro.QA_fetch_get_dailyindicator(start=start,end=end)
        start_2years_bf = str(int(start)-2)
        finacial = pro.QA_fetch_get_finindicator(start=start_2years_bf,end=end)
        def _indicator(data):
            roe_year = QA.EMA(data['q_dt_roe'], 4)*4
            roe_half_year = QA.EMA(data['q_dt_roe'], 2)*4
            #近2年净资产收益率
            asset_rise_2year = data['equity_yoy'].shift(4)*data['equity_yoy']
            # 近3年净资产收益率
            asset_rise_3year = data['equity_yoy'].shift(8) * asset_rise_2year
            equity_pb6 = np.power(asset_rise_3year,6);
            equity_pb7 = np.power(asset_rise_3year, 7);
            roe_year_pb6 = np.power(roe_year, 6);
            roe_year_pb7 = np.power(roe_year, 7);

        finacial = finacial.groupby('ts_code').apply(_indicator)

        basic = pro.QA_SU_save_stock_daily_basic(start=start,end=end)

        #净资产变动调整
        def equity_rejust(data,fin):
            #if data['total_mv']/data['pb']>
            pass

        def _indicatorCp(data):
            fin = finacial.loc[data[0]]
            for index,rp in fin.iterrows():
                if index == 0:

                    data.loc['trade_date'<rp.ann_date].equity_pb6 = rp.equity_pb6
                else:
                    data.loc['trade_date' < rp.ann_date & 'trade_date' > rp.iloc[index-1].ann_date].equity_pb6 = rp.equity_pb6

        # 获取每日equity_pb6、roe_year_pb6
        basic = basic.groupby('ts_code').apply(_indicatorCp)

        #每日统计指标
        def _dailystat(data):
            pass
        basic = basic.groupby('trade_date').apply(_dailystat)




    def top5_valued(dataframe, start, end):
        """
        简单价值判断，近1年 roe,近半年roe，近3年净资产增速，近2年净资产增速。价值法6年pb、7年pb
        """
        codes = dataframe.code.unique()
        st = QA.QA_util_datetime_to_strdate(QA.QA_util_add_months(start,-36))
        dt = QA.QA_fetch_financial_report_adv(codes, st, end, ltype='CN')
        roe_year = QA.EMA(dt.data['006'],4)
        roe_half_year = QA.EMA(dt.data['006'], 2)
        asset_rise_3year = QA.EMA(dt.data['006'], 12)
        asset_rise_2year = QA.EMA(dt.data['006'], 8)
        value_pb6 = asset_rise_3year.pow(6)
        value_pb7 = asset_rise_3year.pow(7)
        roe_year_pb6 = roe_year.pow(6)
        roe_year_pb7 = roe_year.pow(7)


        # DIFF = QA.EMA(CLOSE, SHORT) - QA.EMA(CLOSE, LONG)
        # DEA = QA.EMA(DIFF, M)
        # MACD = 2*(DIFF-DEA)
        #
        # CROSS_JC = QA.CROSS(DIFF, DEA)
        # CROSS_SC = QA.CROSS(DEA, DIFF)
        # ZERO = 0
        # return pd.DataFrame({'DIFF': DIFF, 'DEA': DEA, 'MACD': MACD, 'CROSS_JC': CROSS_JC, 'CROSS_SC': CROSS_SC, 'ZERO': ZERO})

if __name__ == '__main__':
    finacial = pro.QA_fetch_get_finindicator(start='20100101', end='20181231',code=['006160.SH','002056.SZ'])


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


    fin = finacial.groupby('ts_code').apply(_indicator)
