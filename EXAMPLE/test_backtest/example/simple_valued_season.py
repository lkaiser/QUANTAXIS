#-*-coding:utf-8-*-
import math
import QUANTAXIS as QA
from QUANTAXIS.QAFetch import QATusharePro as pro
from indicator.simple_valued import simpleValued
from QUANTAXIS.QAData import QA_DataStruct_Stock_day
from QUANTAXIS.QAFetch.QAQuery import QA_fetch_stock_day

import pandas as pd

try:
    if QA.__version__>'1.1.0':
        pass
    else:
        print('quantaxis version < 1.1.0; please upgrade quantaxis.\npip install quantaxis==1.1.0')
        exit()
except Exception as e:
    print('quantaxis version < 1.1.0; please upgrade quantaxis.\npip install quantaxis==1.1.0')
    exit()

class simpleValuedSeason:
    def __init__(self):
        self.ac = self.rs = None
        self.Broker = QA.QA_BacktestBroker()


    def simple_backtest(self,AC:QA.QA_Account, code, start:str, end:str):
        '''
        简单价值评估回测
        :param AC: QA_Account
        :param code: 股票代码
        :type list or str
        :param start: 回测开始时间
        :param end: 回测结束时间
        :return:
        '''
        #
        #DATA = pro.QA_SU_save_stock_daily(code, start, end)
        # todo 计算信号
        #sv = simpleValued(start,end)
        #sv.indcators_prepare()
        #stock_signal = sv.non_finacal_top5_valued()
        #stock_signal.to_pickle('test.pkl')
        stock_signal = pd.read_pickle('test.pkl')
        #print(stock_signal.head())
        stock_signal.loc[:,'open'] = stock_signal.loc[:,'high'] = stock_signal.loc[:,'low'] = stock_signal['close']
        stock_signal.loc[:,'vol'] = 10000000
        stock_signal.loc[:,'date'] = stock_signal['trade_date'].str[0:4]+'-'+stock_signal['trade_date'].str[4:6]+'-'+stock_signal['trade_date'].str[6:8]#['trade_date']
        #print(stock_signal.columns)
        #stock_signal.loc[:, 'code'] = stock_signal['ts_code']
        for item in stock_signal.index.levels[0]:#按日期循环，stock_signal 多重索引  trade_date,ts_code
            peroid_item = stock_signal.xs(item,
                    level=0,
                    drop_level=False)
            # 可卖数量
            buy_candidates = peroid_item[peroid_item.buy>0]
            sell_candidates = peroid_item[peroid_item.sell>0]
            elimits = []
            if not AC.hold_table().empty:
                #print(type(peroid_item.ts_code))
                elimits = set(AC.hold_table().index).difference(peroid_item.ts_code.values)#mutiple index  index0 为日期 index1 为ts_code基本面发生变化，直接清空
            if not sell_candidates.empty or not elimits.empty:
                sells = set(sell_candidates.ts_code).intersection([] if AC.hold_table().empty else set(AC.hold_table().index)).union(elimits)
                for code in sells:
                    cur_account_sotck_code_sell_available_amount = AC.sell_available.get(code, 0)
                    if (cur_account_sotck_code_sell_available_amount > 0):
                        time = peroid_item.trade_date[0]
                        time = time[0:4] + '-' + time[4:6] + '-' + time[6:8]
                        mk = QA_fetch_stock_day(code=code,
                                                 start=time,
                                                 end=time,
                                                 format='json')
                        if not mk is None:
                            #print(cur_account_sotck_code_sell_available_amount)
                            order = AC.send_order(
                                code=code, time=time, towards=QA.ORDER_DIRECTION.SELL,
                                order_model=QA.ORDER_MODEL.CLOSE, amount_model=QA.AMOUNT_MODEL.BY_AMOUNT,
                                amount=cur_account_sotck_code_sell_available_amount, price=0,
                            )
                            print("#########here"+str(order))
                            sd = peroid_item[peroid_item.ts_code == code].loc[:, ['open', 'close', 'high', 'low', 'vol','date']]
                            self.Broker.receive_order(QA.QA_Event(order=order,market_data=QA_DataStruct_Stock_day(sd)))
                            trade_mes = self.Broker.query_orders(AC.account_cookie, 'filled')
                            #print(trade_mes.columns)
                            #print("sell trade_mes"+str(trade_mes.loc[:,['trade_time','code','towards']]))
                            res = trade_mes.loc[order.account_cookie, order.realorder_id]
                            order.trade(res.trade_id, res.trade_price,
                                        res.trade_amount, res.trade_time)
            cash = AC.cash_available
            if cash and not buy_candidates.empty:
                buys = buy_candidates[0:5] if len(buy_candidates)>5  else buy_candidates
                #print(buys)
                for code in buys.ts_code:
                    time = peroid_item.trade_date[0]
                    time = time[0:4] + '-' + time[4:6] + '-' + time[6:8]
                    mk = QA_fetch_stock_day(code=code,
                                            start=time,
                                            end=time,
                                            format='json')
                    if not mk is None:
                    #print("code="+code)
                        avgPrice = peroid_item[peroid_item.ts_code==code].close[0]
                        maxMoney = cash/len(buys)
                        if maxMoney>100000:
                            print('maxMoney='+str(maxMoney))
                            order = AC.send_order(
                                code=code, time=time, towards=QA.ORDER_DIRECTION.BUY,
                                order_model=QA.ORDER_MODEL.CLOSE,
                                amount_model=QA.AMOUNT_MODEL.BY_MONEY,
                                money=maxMoney,
                                price=avgPrice,
                            )
                            if order is not None and order:
                                sd = peroid_item[peroid_item.ts_code==code].loc[:,['open','close','high','low','vol','date']]
                                self.Broker.receive_order(QA.QA_Event(order=order,market_data=QA_DataStruct_Stock_day(sd)))
                                trade_mes = self.Broker.query_orders(AC.account_cookie, 'filled')
                                #print("buy trade_mes"+str(trade_mes.loc[:,['trade_time','code','towards']]))
                                res = trade_mes.loc[order.account_cookie, order.realorder_id]
                                order.trade(res.trade_id, res.trade_price,
                                            res.trade_amount, res.trade_time)
            AC.settle()

    def day_test(self):
        # 策略名称
        strategy_name = 'MACD_JCSC'
        # 用户cookie
        user_cookie = 'user1'
        # 组合cookie
        portfolio_cookie = 'win300'
        # 账户cookie
        account_cookie = 'bba'
        benchmark_code = '000300'
        initial_cash = 200000
        initial_hold = {}
        # 交易佣金
        commission_coeff = 0.00025
        # 印花税
        tax_coeff = 0.00

        # backtest_code_list = QA.QA_fetch_stock_block_adv().code[0:10]
        backtest_code_list = '000001'
        backtest_start_date = '20180101'
        backtest_end_date = '20180421'

        #Broker = QA.QA_BacktestBroker()
        AC = QA.QA_Account(
            strategy_name=strategy_name,
            user_cookie=user_cookie,
            portfolio_cookie=portfolio_cookie,
            account_cookie=account_cookie,
            init_hold=initial_hold,
            init_cash=initial_cash,
            commission_coeff=commission_coeff,
            tax_coeff=tax_coeff,
            market_type=QA.MARKET_TYPE.STOCK_CN,
            frequence=QA.FREQUENCE.DAY
        )
        # 设置初始资金
        AC.reset_assets(20000000)

        # 回测
        #svs = simpleValuedSeason()
        self.simple_backtest(AC, backtest_code_list, backtest_start_date, backtest_end_date)

        # AC.save()

        # 结果
        # print(AC.message)
        print(AC.history_table)
        self.ac = AC;

        # 分析
        # print(AC.end_date)
        #AC = QA.QA_Account().from_message(QA.QA_fetch_account({'account_cookie': 'bba'})[0])
        risk = QA.QA_Risk(account=AC, benchmark_code=benchmark_code, benchmark_type=QA.MARKET_TYPE.INDEX_CN, if_fq=False)
        self.rs = AC;

        # risk.save()
        # risk.account

        # print(risk.message)
        fig = risk.plot_assets_curve()

        fig.plot()
        fig.show()
        #s.ac.trade[s.ac.trade['000420.SZ'] > 0].loc[:, ['000420.SZ']]
        #s.ac.history_table.loc[:,['datetime','code','price','amount','tax']]
        #s.ac.daily_cash

if __name__ == '__main__':
    # 策略名称
    strategy_name = 'svs'
    # 用户cookie
    user_cookie = 'user1'
    # 组合cookie
    portfolio_cookie = 'win300'
    # 账户cookie
    account_cookie = 'bba'
    benchmark_code = '000300'
    initial_cash = 200000
    initial_hold = {}
    # 交易佣金
    commission_coeff = 0.00025
    # 印花税
    tax_coeff = 0.00

    # backtest_code_list = QA.QA_fetch_stock_block_adv().code[0:10]
    backtest_code_list = '000001'
    backtest_start_date = '20180101'
    backtest_end_date = '20180421'

    Broker = QA.QA_BacktestBroker()
    AC = QA.QA_Account(
        strategy_name=strategy_name,
        user_cookie=user_cookie,
        portfolio_cookie=portfolio_cookie,
        account_cookie=account_cookie,
        init_hold=initial_hold,
        init_cash=initial_cash,
        commission_coeff=commission_coeff,
        tax_coeff=tax_coeff,
        market_type=QA.MARKET_TYPE.STOCK_CN,
        frequence=QA.FREQUENCE.DAY
    )
    # 设置初始资金
    AC.reset_assets(20000000)

    # 回测
    svs = simpleValuedSeason()
    svs.simple_backtest(AC, backtest_code_list, backtest_start_date, backtest_end_date)

    #AC.save()

    # 结果
    #print(AC.message)
    print(AC.history_table)


    # 分析
    #print(AC.end_date)
    AC = QA.QA_Account().from_message(QA.QA_fetch_account({'account_cookie': 'bba'})[0])
    risk = QA.QA_Risk(account=AC, benchmark_code=benchmark_code, benchmark_type=QA.MARKET_TYPE.INDEX_CN, if_fq=False)

    #risk.save()
    #risk.account

    #print(risk.message)
    fig=risk.plot_assets_curve()

    fig.plot()
    fig.show()