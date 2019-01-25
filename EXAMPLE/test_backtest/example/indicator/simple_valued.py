# -*- coding: utf-8 -*-
import QUANTAXIS as QA
from QUANTAXIS.QAFetch import QATusharePro as pro
import pandas as pd

class simpleValued:


    def allValues(self,start,end):
        """
            简单价值判断，近1年 roe,近半年roe，近3年净资产增速，近2年净资产增速。价值法6年pb、7年pb
         """
        #daily = pro.QA_fetch_get_dailyindicator(start=start,end=end)
        finacial = pro.QA_fetch_get_finindicator(start=start,end=end)
        def _indicator(data):
            roe_year = QA.EMA(data['roe'], 4)
            roe_half_year = QA.EMA(data['roe'], 2)
            asset_rise_3year = QA.EMA(data['equity_yoy'], 12)
        finacial.groupby('ts_code').apply(_indicator)


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


        DIFF = QA.EMA(CLOSE, SHORT) - QA.EMA(CLOSE, LONG)
        DEA = QA.EMA(DIFF, M)
        MACD = 2*(DIFF-DEA)

        CROSS_JC = QA.CROSS(DIFF, DEA)
        CROSS_SC = QA.CROSS(DEA, DIFF)
        ZERO = 0
        return pd.DataFrame({'DIFF': DIFF, 'DEA': DEA, 'MACD': MACD, 'CROSS_JC': CROSS_JC, 'CROSS_SC': CROSS_SC, 'ZERO': ZERO})
