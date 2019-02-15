#-*-coding:utf-8-*-
import math
import QUANTAXIS as QA
from QUANTAXIS.QAFetch import QATusharePro as pro
from indicator.simple_valued import simpleValued

try:
    if QA.__version__>'1.1.0':
        pass
    else:
        print('quantaxis version < 1.1.0; please upgrade quantaxis.\npip install quantaxis==1.1.0')
        exit()
except Exception as e:
    print('quantaxis version < 1.1.0; please upgrade quantaxis.\npip install quantaxis==1.1.0')
    exit()

def simple_backtest(AC:QA.QA_Account, code, start:str, end:str):
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
    sv = simpleValued(start,end)
    sv.indcators_prepare()
    stock_signal = sv.non_finacal_top5_valued()
    for item in stock_signal.index.levels[0]:
        peroid_item = stock_signal.xs(item,
                level=0,
                drop_level=False)
        # 可卖数量
        buy_candidates = peroid_item[peroid_item.buy>0]
        sell_candidates = peroid_item[peroid_item.sell<0]
        elimits = AC.hold_table().code.diff(peroid_item.ts_code)#基本面发生变化，直接清空
        if not sell_candidates.empty or not elimits.empty:
            sells = sell_candidates.ts_code.intersection(AC.hold_table().code).union(elimits)
            for code in sells:
                cur_account_sotck_code_sell_available_amount = AC.sell_available.get(code, 0)
                order = AC.send_order(
                    code=code, time=peroid_item.trade_date[0], towards=QA.ORDER_DIRECTION.SELL,
                    order_model=QA.ORDER_MODEL.CLOSE, amount_model=QA.AMOUNT_MODEL.BY_AMOUNT,
                    amount=cur_account_sotck_code_sell_available_amount, price=0,
                )
                Broker.receive_order(QA.QA_Event(order=order))
                trade_mes = Broker.query_orders(AC.account_cookie, 'filled')
                res = trade_mes.loc[order.account_cookie, order.realorder_id]
                order.trade(res.trade_id, res.trade_price,
                            res.trade_amount, res.trade_time)
        cash = AC.cash_available
        if cash and buy_candidates:
            buys = buy_candidates[0:5] if len(buy_candidates)>5  else buy_candidates
            for code in buys:
                avgPrice = peroid_item[peroid_item.code==code].close
                maxMoney = cash/len(buy_candidates)
                order = AC.send_order(
                    code=code, time=peroid_item.trade_date[0], towards=QA.ORDER_DIRECTION.BUY,
                    order_model=QA.ORDER_MODEL.MARKET,
                    amount_model=QA.AMOUNT_MODEL.BY_MONEY,
                    money=maxMoney,
                    price=avgPrice,
                )
                Broker.receive_order(QA.QA_Event(order=order))
                trade_mes = Broker.query_orders(AC.account_cookie, 'filled')
                res = trade_mes.loc[order.account_cookie, order.realorder_id]
                order.trade(res.trade_id, res.trade_price,
                            res.trade_amount, res.trade_time)
        AC.settle()

if __name__ == '__main__':
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
    tax_coeff = 0.0015

    # backtest_code_list = QA.QA_fetch_stock_block_adv().code[0:10]
    backtest_code_list = '000001'
    backtest_start_date = '2018-01-01'
    backtest_end_date = '2018-08-21'

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
    # AC.reset_assets(20000000)

    # 回测
    simple_backtest(AC, backtest_code_list, backtest_start_date, backtest_end_date)

    AC.save()

    # 结果
    print(AC.message)
    print(AC.history_table)



    # 分析
    risk = QA.QA_Risk(account=AC, benchmark_code=benchmark_code, benchmark_type=QA.MARKET_TYPE.INDEX_CN, if_fq=False)
    risk.save()

    print(risk.message)
    fig=risk.plot_assets_curve()

    fig.plot()
