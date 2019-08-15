# -*- coding: utf-8 -*-
import QUANTAXIS as QA
from QUANTAXIS.QAFetch import QATusharePro as pro
import pandas as pd
import numpy as np
import datetime
import os
from QUANTAXIS.ML import RegUtil
import time
#pd.set_option('display.float_format', lambda x: '%.3f' % x)
#pd.set_option('display.max_columns',5, 'display.max_rows', 100)
class simpleValued:

    def __init__(self,start,end):
        self.start = start
        self.end = end
        self.basic_temp_name = 'D:\\work\\QUANTAXIS\EXAMPLE\\test_backtest\\example\\indicator\\basic_temp_' +start+'_'+end +'.csv'
        #if not (os.path.isfile(self.basic_temp_name)):
        start_2years_bf = str(int(start[0:4]) - 5)
        start_1years_bf = str(int(start[0:4]) - 1)
        self.finacial = pro.QA_fetch_get_finindicator(start=start_2years_bf,end=end)
        self.income = pro.QA_fetch_get_income(start=start_2years_bf, end=end)
        self.asset = pro.QA_fetch_get_assetAliability(start=start_2years_bf, end=end)
        self.basic_1more = pro.QA_fetch_get_dailyindicator(start=start_1years_bf,end=end)
        self.basic_1adj = pro.QA_fetch_get_daily_adj(start=start_1years_bf,end=end)
        self.basic_1more = self.basic_1more.merge(self.basic_1adj,on=['trade_date','ts_code'],how='inner')
            #print(basic.head().loc[:,['ts_code','trade_date','close','pe']])
        self.basic = self.basic_1more[self.basic_1more.trade_date>=start].sort_values(['ts_code','trade_date'], ascending = True)
        self.stock = pro.QA_SU_stock_info()
        self.dailymarket = None
        self.industry = None


    def indcators_prepare(self,basic):
        """
            简单价值判断，近1年 roe,近半年roe，近3年净资产增速，近2年净资产增速。价值法6年pb、7年pb
         """
        #daily = pro.QA_fetch_get_dailyindicator(start=start,end=end)

        def _ver_indicator(data):
            '''
            纵轴指标
            :param data:
            :return:
            '''
            #print(data['q_dt_roe'].head())
            if(data['q_dt_roe'].isnull().all()):
                roe_half_year = roe_year = np.array([np.nan] * data.shape[0])
            else:
                roe_year = (QA.EMA(data['q_dt_roe'], 4)*4/100+1).fillna(method='bfill') # 连续4个季度取平均，再乘以4得到年化roe，跟同花顺略有不同，同花顺以年为单位，1季度*4，二季度*2，三季度*4/3，我这个比较平稳
                roe_half_year = (QA.EMA(data['q_dt_roe'], 2)*4/100+1).fillna(method='bfill') # 连续2个继续取平均，再乘以4得到半年化，敏感性比年化的要强
            #近2年净资产收益率
            # asset_rise_2year = (data['equity_yoy'].shift(4)/100+1)*(data['equity_yoy']/100+1)
            # 近3年净资产收益率
            # asset_rise_3year = (data['equity_yoy'].shift(8)/100+1) * asset_rise_2year
            # data.loc[:,'equity3_pb7'] = np.round(np.power(np.power(asset_rise_3year,1/3),7),2)
            #print(len(asset_rise_3year[asset_rise_3year.isnull()]))
            # data.loc[:, 'equity2_pb7'] = np.round(np.power(np.power(asset_rise_2year,1/2), 7),2)
            #data.loc[:, 'roe_year_pb6'] = np.round(np.power(roe_year, 6),2)
            data.q_dt_roe.fillna(data.q_dt_roe.mean(), inplace=True)
            for i in range(2,6):
                if data.shape[0]>=i*4:
                    data.loc[:, 'roe_av'+str(i)] = data.q_dt_roe[0:i*4].mean()/i
                else:
                    data.loc[:, 'roe_av' + str(i)] = None
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
            data.ebit.fillna(method='pad', inplace=True)
            data.ebit.fillna(method='bfill', inplace=True)
            data.ebit_ps.fillna(method='pad', inplace=True)
            data.ebit_ps.fillna(method='bfill', inplace=True)
            data.bps.fillna(method='pad', inplace=True)
            data.bps.fillna(method='bfill', inplace=True)
            data.cfps.fillna(method='pad', inplace=True)
            data.cfps.fillna(method='bfill', inplace=True)
            data.q_opincome.fillna(method='pad', inplace=True)
            data.q_opincome.fillna(method='bfill', inplace=True)
            data.q_dtprofit.fillna(method='pad', inplace=True)
            data.q_dtprofit.fillna(method='bfill', inplace=True)
            data.q_ocf_to_or.fillna(method='pad', inplace=True)
            data.q_ocf_to_or.fillna(method='bfill', inplace=True)

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

        self.finacial = self.finacial.groupby('ts_code').apply(_ver_indicator)

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
        def _indicatorCp(data):
            '''
            需要在每日中体现的指标都在col中添加
            :param data:
            :return:
            '''
            col = ['roe_av2','roe_av3','roe_av4','roe_av5','roe_year_pb7','roe_half_year_pb7','q_dt_roe','gross_margin_poly','roe_yearly','q_gsprofit_margin_poly','inv_turn_poly','fa_turn_poly','opincome_of_ebt','dtprofit_to_profit','ocf_to_opincome','debt_to_assets','op_to_ebt','tbassets_to_totalassets']
            df = pd.concat([data,pd.DataFrame(columns=col,dtype='float')])
            fin = self.finacial[self.finacial.ts_code==data.name]
            for i in range(0, fin.shape[0]):
                if i+1 < fin.shape[0]:
                    query = (df.trade_date >= fin.iloc[i].ann_date) & (df.trade_date < fin.iloc[i + 1].ann_date)
                else:
                    query = df.trade_date >= fin.iloc[i].ann_date
                df.loc[query, col] = fin.iloc[i][col].values
                # df.loc[query, ['opincome_of_ebt']] = fin.iloc[i].opincome_of_ebt #经营活动净收益/利润总额
                # df.loc[query, ['dtprofit_to_profit']] = fin.iloc[i].dtprofit_to_profit #扣除非经常损益后的净利润/净利润
                # df.loc[query, ['ocf_to_opincome']] = fin.iloc[i].ocf_to_opincome #经营活动产生的现金流量净额/经营活动净收益
                # df.loc[query, ['debt_to_assets']] = fin.iloc[i].debt_to_assets  # 资产负债率
                # df.loc[query, ['op_to_ebt']] = fin.iloc[i].op_to_ebt  # 营业利润／利润总额
                # df.loc[query, ['tbassets_to_totalassets']] = fin.iloc[i].tbassets_to_totalassets  # 有形资产/总资产
            return df

            #basic = self.basic
        #  basic 日交易基本数据
        # 增加 equity2_pb7、equity3_pb7、roe_year_pb7等几列指标,_indicatorCp运算时间过长，要20多分钟，需要
        # 对中间结果加以保存,后续考虑通过并发框架实施
        if not (os.path.isfile(self.basic_temp_name)):
            df = basic.groupby('ts_code').apply(_indicatorCp)
            df.to_pickle(self.basic_temp_name)
        else:
            df = pd.read_pickle(self.basic_temp_name)
        return df

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
            目前只完成简单银行、保险剔除，主要是本人不懂金融类企业
            上市不足2年的剔除
            :param data:
            :param stock:
            :param fin:
            :return:
            '''
            non_finacial_codes = stock[(stock.industry != '银行') & (stock.industry != '保险')].ts_code.values
            basic = data[data.ts_code.isin(non_finacial_codes)]
            start_2years_bf = str(int(self.start[0:4]) - 2)+self.start[4:8]
            basic = data[data.ts_code.isin(stock[stock.list_date<start_2years_bf].ts_code.values)]
            return basic

        basic = data
        if basic is None:
            basic = self.basic

        basic = _before_fiter(basic,self.stock,self.finacial)

        basic = self.indcators_prepare(basic)

        def _af_fiter(df):
            '''
            af_fiter 季度级指标过滤，有些季度级指标不合格，不代表整个周期不合格，这种情况下只需过滤当期不合格的数据
            垃圾排除大法 剔除商誉过高、现金流不充裕，主营利润占比低、资产负债率过高、存货占比、应收占比
            TODO 1累计3年经营现金流为负 自由现金流为负
            TODO 2 roe 白名单制， 过去3-5年平均roe 超过10 的进来
            TODO 3 短期偿债能力，有息负债比率
            '''
            fin = self.finacial.loc[self.finacial.ts_code == df.name]
            ast = self.asset.loc[self.asset.ts_code == df.name]
            rm = None
            rms = []
            if fin.shape[0]:
                fin.loc[:, 'rmflag'] = 0
                fin.loc[fin.q4_ocf/fin.q4_opincome<0.55,'rmflag'] = 1 #经营活动现金流/经营活动净利润 <0.6的不要了
                fin.loc[fin.q4_opincome / fin.q4_dtprofit < 0.8,'rmflag'] = 1  # 经营活动净收益/净利润 <0.7的不要了（投资收益什么的不靠谱）
            if ast.shape[0]:
                ast.loc[:, 'rmflag'] = 0
                ast.loc[ast.goodwill / ast.total_hldr_eqy_exc_min_int > 0.2,'rmflag'] = 1  # 商誉占比
                ast.loc[ast.inventories / ast.total_hldr_eqy_exc_min_int > 0.38,'rmflag'] = 1  # 存货占比
                ast.loc[(ast.notes_receiv + ast.accounts_receiv) / ast.total_hldr_eqy_exc_min_int > 0.3,'rmflag'] = 1  # 应收占比

            for i in range(ast.shape[0]): #举例 20171231 0; 20180331  1;20180630  1;20180930 0;20181231 1  ,则  20180331-20180930之间,20181231-end之间的全删除
                if ast.iloc[i].rmflag == 1 and not rm:
                    rm = ast.iloc[i].ann_date
                if ast.iloc[i].rmflag == 0 and rm:
                    rms.append((rm, ast.iloc[i].ann_date))
                    rm = None
            if rm:
                rms.append((rm, self.end))
                rm = None
            for i in range(fin.shape[0]): #逻辑同上面的ast，可以和ast里的日期重复，但凡不符合都删除
                if fin.iloc[i].rmflag == 1 and not rm:
                    rm = fin.iloc[i].ann_date
                if fin.iloc[i].rmflag == 0 and rm:
                    rms.append((rm, fin.iloc[i].ann_date))
                    rm = None
            if rm:
                rms.append((rm, self.end))
                rm = None
            data = df
            for k in rms:
                data = data[~((data.trade_date >= k[0]) & (data.trade_date < k[1]))]
            return data
        basic = basic.groupby('ts_code',as_index=False).apply(_af_fiter)



        # 每日统计指标
        def _dailystat(df):
            non_finacial = df
            col = ['category', 'cnt', 'mean', 'std', 'min', 'per25', 'per50', 'per85', 'per90', 'per95', 'max']
            ind = [ 'roe_half_year_pb7_pb', 'roe_year_pb7_pb']
            non_finacial.loc[:, 'roe_half_year_pb7_pb'] = np.round(non_finacial.loc[:, 'roe_half_year_pb7'] / non_finacial.loc[:, 'pb'], 3)
            non_finacial.loc[:, 'roe_year_pb7_pb'] = np.round(non_finacial.loc[:, 'roe_year_pb7'] / non_finacial.loc[:, 'pb'], 3)
            # 太假的不要，干扰数据，净资产本季报之后发生变化>1.1的排除
            non_finacial = non_finacial.loc[(non_finacial.roe_year_pb7_pb < 11)]
            st = non_finacial.loc[:, ind].describe([.25, .5, .85, .90, .95]).T.reset_index(level=0)
            st.columns = col
            st.loc[:, 'statype'] = 'non_finacial'
            st.index = [df.name] * len(ind)

            cu = non_finacial.loc[:, ind]
            median = cu.median()
            mad = abs(cu - median).median()
            cu[cu - (median - mad * 3 * 1.4826) < 0] = np.array((median - mad * 3 * 1.4826).tolist() * cu.shape[0]).reshape((cu.shape[0], cu.columns.size))
            cu[cu - (median + mad * 3 * 1.4826) > 0] = np.array((median + mad * 3 * 1.4826).tolist() * cu.shape[0]).reshape((cu.shape[0], cu.columns.size))

            st2 = cu.describe([.25, .5, .85, .90, .95]).T.reset_index(level=0)
            st2.columns = col
            st2.loc[:, 'statype'] = 'non_finacial'
            st2.index = [df.name] * len(ind)
            st2.category = st2.category + '_mad'
            return pd.concat([st, st2])

        # print(self.basic.loc[:,['equity_pb6','equity_pb7','roe_year_pb6','roe_year_pb7']].head())
        # pass
        dailymarket = basic.groupby('trade_date').apply(_dailystat)


        def _top10(df,dailymarket):
            '''equity2_pb7  2年净资产增速对应pb, /实际pb 得出价值倍数，找出价值倍数大于95%数据，这个指标十有八九不靠谱,还不如用roe_year_pb7
                df.equity2_pb7 / df.pb 预计涨幅
            '''
            if df.name in dailymarket.index.levels[0]:
                dailymarket = dailymarket.loc[df.name]
                if (dailymarket[dailymarket.statype == 'non_finacial'].shape[0] == 0):
                    print(dailymarket)
                else:
                    dailymarket = dailymarket[dailymarket.statype == 'non_finacial']
                    df.loc[:, 'roe_pb7'] = df.roe_year_pb7 / df.pb# - dailymarket[dailymarket.category == 'roe_year_pb7_pb'].per90[0]
                    df.loc[:,'market_pb7_90'] = dailymarket[dailymarket.category == 'roe_year_pb7_pb'].per90[0]
                    df.loc[:, 'half_roe_pb7'] = df.roe_half_year_pb7 / df.pb #- dailymarket[dailymarket.category == 'roe_half_year_pb7_pb'].per90[0]
                    df.loc[:, 'half_market_pb7_90'] = dailymarket[dailymarket.category == 'roe_half_year_pb7_pb'].per90[0] #- df.roe_half_year_pb7 / df.pb - 0.3

                    df.loc[:, 'roe_pb7_mad'] = df.roe_year_pb7 / df.pb # - dailymarket[dailymarket.category == 'roe_year_pb7_pb_mad'].per90[0]
                    df.loc[:, 'market_pb7_90_mad'] = dailymarket[dailymarket.category == 'roe_year_pb7_pb_mad'].per90[0] #- df.roe_year_pb7 / df.pb - 0.3
                    df.loc[:, 'half_roe_pb7_mad'] = df.roe_half_year_pb7 / df.pb  #dailymarket[dailymarket.category == 'roe_half_year_pb7_pb_mad'].per90[0]
                    df.loc[:, 'half_market_pb7_90_mad'] = dailymarket[dailymarket.category == 'roe_half_year_pb7_pb_mad'].per90[0] #- df.roe_half_year_pb7 / df.pb - 0.3
                    return df

        return basic.groupby('trade_date',as_index=False).apply(_top10,dailymarket=dailymarket).set_index(['trade_date', 'ts_code'],drop=False)
        #return basic.groupby(level=1, sort=False).apply(_top5).set_index(['trade_date', 'ts_code'])


    def industry_trend_top10(self,data):
        """
                行业判断指标，增加行业roe、pe、收益等指标的趋势判断,单只股票的趋势不具备代表性，行业的趋势则表明整体行环境。
                选取整体行业趋势向好的个股，具有更高胜率
                """
        start_3years_bf = str(int(self.start[0:4]) - 3)+self.start[4:8]
        industry_daily = pro.QA_fetch_get_industry_daily(start=start_3years_bf, end=self.end).sort_values(['industry','trade_date'], ascending = True)
        def _trend(data):
            '''
            行业趋势 计算总市值,流通市值,总扣非盈利,总净资产,总资产,总成交量,
            :param data:
            :return:
            '''

            dates = [str(int(self.start[0:4]) - 3) + '0831',str(int(self.start[0:4]) - 3) + '1031',
                     str(int(self.start[0:4]) - 2) + '0431', str(int(self.start[0:4]) - 2) + '0831',
                     str(int(self.start[0:4]) - 2) + '1031', str(int(self.start[0:4]) - 1) + '0431',
                     str(int(self.start[0:4]) - 1) + '0831', str(int(self.start[0:4]) - 1) + '1031']
            #print(data.iloc[-1])
            _lam_f = lambda x, y: y[y.trade_date < x].iloc[-1] if y[y.trade_date < x].shape[0]>0 else None
            resampledf = pd.DataFrame(list(filter(lambda x:x is not None,map(_lam_f, dates,[data]*8))))#dates.apply() #每个行业每天都数据，resampledf 取指定dates的最新一条数据
            #map(lambda x,y:np.where(y[y.a>x].shape[0]>0,y[y.a>x].iloc[-1],None),[3,5],[df]*2)
            indicator = pd.DataFrame(columns=['trade_date','industry','q_dtprofit_ttm_poly','q_gr_poly','q_profit_poly','q_dtprofit_poly','q_opincome_poly','roe','pe','roe_ttm','pe_ttm'])
            df = data[data.trade_date >= self.start]
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
                ind = -8 if resample.shape[0]>8 else -resample.shape[0]  #默认拟合8条也就是2年数据（不太可能超过2年，行业季度数据不会丢失，个股有可能），没达到2年有多少取多少
                #print(resample.loc[:, ['industry', 'trade_date', 'q_dtprofit']].head())
                # fit, p1 = RegUtil.regress_y_polynomial(resample[-8:].q_gr_ttm, poly=3, show=False)
                # fit, p2 = RegUtil.regress_y_polynomial(resample[-8:].q_profit_ttm, poly=3, show=False)
                fit, p3 = RegUtil.regress_y_polynomial(resample[ind:].q_dtprofit_ttm, poly=3, show=False)  #p3 ..p8都是拟合曲线导数，计算pn(abs(ind)) 即可获取最新一季度趋势
                # fit, p4 = RegUtil.regress_y_polynomial(resample[-8:].q_opincome_ttm, poly=3, show=False)
                fit, p5 = RegUtil.regress_y_polynomial(resample[ind:].q_gr, poly=3, show=False)
                fit, p6 = RegUtil.regress_y_polynomial(resample[ind:].q_profit, poly=3, show=False)
                fit, p7 = RegUtil.regress_y_polynomial(resample[ind:].q_dtprofit, poly=3, show=False)
                fit, p8 = RegUtil.regress_y_polynomial(resample[ind:].q_opincome, poly=3, show=False)
                roe = item.q_dtprofit / item.total_hldr_eqy_exc_min_int
                pe = item.ind_total_mv*10000/item.q_dtprofit
                roe_ttm = item.q_dtprofit_ttm / item.total_hldr_eqy_exc_min_int
                pe_ttm = item.ind_total_mv*10000/item.q_dtprofit_ttm
                indicator.loc[index] = [item.trade_date,data.name,p3(abs(ind)),p5(abs(ind)),p6(abs(ind)),p7(abs(ind)),p8(abs(ind)),roe,pe,roe_ttm,pe_ttm] #
            return indicator
        industry_daily = industry_daily.groupby("industry",as_index=False).apply(_trend)
        df = pd.merge(data, self.stock.loc[:, ['ts_code', 'industry']], left_on='ts_code', right_on='ts_code', how="inner")  # 找到每只code的行业，剔除缺少行业的
        industry_daily.rename(columns=({'pe': 'industry_pe','roe':'industry_roe','pe_ttm':'industry_pe_ttm'}), inplace = True)
        df = pd.merge(df, industry_daily, left_on=['industry', 'trade_date'], right_on=['industry', 'trade_date'], how="inner")  # 合并code及其对应的行业数据，剔除行业样本太少的

        #df.to_pickle('test2.pkl')
        # 每日统计指标,数据丢失太严重，17w数据，有2.8w的q_dtprofit丢失，只好用前向或者后向填充，其他指标丢失更严重，失去统计意义
        def _dailystat(df):
            d = df.loc[:, ['q_dtprofit_ttm_poly','q_gr_poly','q_profit_poly','q_dtprofit_poly','q_opincome_poly','industry_roe','industry_pe','roe_ttm','industry_pe_ttm']]
            st = d.describe([.25, .5, .85, .90, .95]).T.reset_index(level=0)
            st.columns = ['category', 'cnt', 'mean', 'std', 'min', 'per25', 'per50', 'per85', 'per90', 'per95', 'max']
            st.index = [df.name] * 9

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
            st2.columns = ['category', 'cnt', 'mean', 'std', 'min', 'per25', 'per50', 'per85', 'per90', 'per95', 'max']
            st2.index = [df.name] * 9
            st2.category = st2.category+'_mad'
            return pd.concat([st, st2])

        dailymarket = industry_daily.groupby('trade_date').apply(_dailystat)

        def _top10(df,dailymarket):
            if df.name in dailymarket.index.levels[0]:
                dailymarket = dailymarket.loc[df.name]
                df.loc[:, 'industry_roe_buy'] = df.industry_roe - dailymarket[dailymarket.category == 'industry_roe'].per90[0]
                df.loc[:, 'industry_pe_buy'] = df.industry_pe - dailymarket[dailymarket.category == 'industry_pe'].per85[0]
                df.loc[:, 'q_dtprofit_poly'] = df.q_dtprofit_poly - dailymarket[dailymarket.category == 'q_dtprofit_poly'].per85[0]
                df.loc[:, 'industry_roe_ttm_buy'] = df.roe_ttm - dailymarket[dailymarket.category == 'roe_ttm'].per90[0]
                df.loc[:, 'industry_pe_ttm_buy'] = df.industry_pe_ttm - dailymarket[dailymarket.category == 'industry_pe_ttm'].per85[0]
                df.loc[:, 'q_dtprofit_ttm_poly'] = df.q_dtprofit_ttm_poly - dailymarket[dailymarket.category == 'q_dtprofit_ttm_poly'].per85[0]
                df.loc[:, 'industry_roe_buy_mad'] = df.industry_roe - dailymarket[dailymarket.category == 'industry_roe_mad'].per90[0]
                df.loc[:, 'industry_pe_buy_mad'] = df.industry_pe - dailymarket[dailymarket.category == 'industry_pe_mad'].per85[0]
                df.loc[:, 'q_dtprofit_poly_mad'] = df.q_dtprofit_poly - dailymarket[dailymarket.category == 'q_dtprofit_poly_mad'].per85[0]
                df.loc[:, 'industry_roe_ttm_buy_mad'] = df.roe_ttm - dailymarket[dailymarket.category == 'roe_ttm_mad'].per90[0]
                df.loc[:, 'industry_pe_ttm_buy_mad'] = df.industry_pe_ttm - dailymarket[dailymarket.category == 'industry_pe_ttm_mad'].per85[0]
                df.loc[:, 'q_dtprofit_ttm_poly_mad'] = df.q_dtprofit_ttm_poly - dailymarket[dailymarket.category == 'q_dtprofit_ttm_poly_mad'].per85[0]
                return df
            #pass

        return df.groupby('trade_date', as_index=False).apply(_top10, dailymarket=dailymarket).set_index(['trade_date', 'ts_code'], drop=False)

    def simpleStrategy(self):

        df = pd.read_csv(self.basic_temp_name,dtype={'trade_date':str,'circ_mv':np.float32}).set_index(['trade_date', 'ts_code'], drop=False)
        df.loc[:,'buy'] = (df.roe_buy>0) & (df.half_roe_buy>df.roe_buy) & (df.industry_roe_buy_mad>0) &(df.roe_yearly>10) &(df.opincome_of_ebt>85) &(df.debt_to_assets<70)
        df.loc[:,'sell'] = df.roe_sell>0
        #stock_signal = pd.read_pickle('test.pkl')
        #df.loc[:'f_buy'] =
        #print(df.head())
        return df

    def regression(self,df):
        med = df.roe_buy.median()
        """
            回归基础池，分大类进行回归，roe大于中间值，且近2季度roe趋势向上，且            
        """
        df.loc[:, 'buy'] = (df.roe_buy > med) & (df.half_roe_buy > df.roe_buy) & (df.industry_roe_buy_mad > -0.001) & (
                    df.roe_yearly > 10) & (df.opincome_of_ebt > 85) & (df.debt_to_assets < 70)

        # def _first(df):
        #     df = df.sort_values(['trade_date'], ascending=True)
        #     return df.iloc[0]
        first = df[df.buy &(df.trade_date>'20180101') &(df.trade_date<'20180210')].groupby('ts_code',as_index=False).first()
        second = df[df.ts_code.isin(first.ts_code.values) &(df.trade_date<'20180530')]
        first.rename(columns={'trade_date': 'first_day','close':'f_close'}, inplace=True)
        second = second.merge(first.loc[:,['ts_code','f_close','first_day']],on=['ts_code'],how='inner')

        def _max(df):
            b = df[df.trade_date>=df.first_day]
            if b.shape[0]>0:
                #return [b.close.max(), b.close.min(), df.name]
                return pd.DataFrame([{'max_close':b.close.max(),'min_close':b.close.min(),'ts_code':df.name}])
            else:
                return None
        train = second.groupby('ts_code',as_index=False).apply(_max)
        train = train.merge(first,on='ts_code',how='inner')

        import statsmodels.api as sm
        y = train.max_close/train.f_close
        x = train[['roe_buy','half_roe_buy','industry_roe_buy_mad','roe_yearly','roe_ttm','opincome_of_ebt','pb','pe','debt_to_assets','q_dt_roe']]

        #null 检查，
        #



        #计算未来3,6个月的最大涨幅，总共也就1年的数据，可以拿1,4,7,这3个月做未来3月涨幅回归，1,6 这2个月做未来6个月涨幅回归


    def price_trend(self,df):
        '''
        计算连续10日，连续20成交量上涨程度，最近10日，3月，半年，1年最高涨幅，最近5日振幅，10日振幅
        :param df:
        :return:
        '''
        df2 = self.basic_1more.loc[:,['ts_code','trade_date','close','turnover_rate']]
        df2.last_close = df2.close.shift(1)
        df2.rise = df2.close/df2.last_close
        f1 = lambda x:np.abs(x.rise*100-100).mean() #振幅，正负都算,以100为中心,统计均值
        f2 = lambda x:x.max()/x.min() #最高涨幅,最高/最低
        f3 = lambda x:x[x.shape[0]/2:x.shape[0]].sum()/x[0:x.shape[0]/2].sum() #成交量上涨程度，统计区间平分两段，后段除前段

        df2.loc[:,'wave_5'] = df2.rise.rolling(5).apply(f1)
        df2.loc[:,'wave_10'] = df2.rise.rolling(10).apply(f1)
        df2.loc[:,'rise_10'] = df2.close.rolling(10).apply(f2)
        df2.loc[:,'rise_60'] = df2.close.rolling(60).apply(f2)
        df2.loc[:,'rise_250'] = df2.turnover_rate.rolling(250).apply(f2)
        df2.loc[:,'vol_10'] = df2.turnover_rate.rolling(20).apply(f3)
        df2.loc[:,'vol_20'] = df2.turnover_rate.rolling(40).apply(f3)

        df = df.merge(df2[df2.trade_date>=self.start],on=['ts_code','trade_date'],how='inner')

        return df

    def time_choice(self,df):
        '''
        你需要择时
        :param df:
        :return:
        '''

        pass

if __name__ == '__main__':
    #finacial = pro.QA_fetch_get_finindicator(start='20100101', end='20181231',code=['006160.SH','002056.SZ'])

    print('wtf')
    sv = simpleValued('20180101','20181231')
    print(time.strftime("%a %b %d %H:%M:%S %Y", time.localtime()))
    df = sv.non_finacal_top5_valued()
    df = sv.industry_trend_top10(df)
    df = sv.price_trend(df)
    df.to_pickle('basic-2018.pkl')
    #df.to_csv('basic-2018.csv')
    print(time.strftime("%a %b %d %H:%M:%S %Y", time.localtime()))


    #sv.simpleStrategy()
    # fit, p = RegUtil.regress_y_polynomial([5,7,4,3.6,6,7,9,6,7], poly=3, show=True)
    # print(fit)
    # print(p)
    # print(p(3))

    # finacial = pro.QA_fetch_get_finindicator(start='20150101', end='20181231')
    # #finacial.to_pickle('finace-2018.pkl')
    # finacial.to_csv('finace-2018.csv')
    # basic = pro.QA_fetch_get_dailyindicator(start='20180101', end='20181231')
    # basic.to_csv('basic-2018.csv')
    #basic.to_pickle('basic-2018.pkl')
    #sv.indcators_prepare()
    # df = sv.non_finacal_top5_valued()

    #asset = pro.QA_fetch_get_assetAliability(start='20160101', end='20180930')
    #print(asset.head())
    #print(QA.EMA(pd.Series([3.1,4.1,5.1,6.1,7.1,8.1,9.1,10.1]),4))


    #fin = finacial.groupby('ts_code').apply(_indicator)
