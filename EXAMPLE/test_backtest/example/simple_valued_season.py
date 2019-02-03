#-*-coding:utf-8-*-
import math
import QUANTAXIS as QA
from QUANTAXIS.QAFetch import QATusharePro as pro
from indicator.simple_valued import simple_valued

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
    stock_signal = sv.top5_valued()
    for item in stock_signal.index.levels[0]:
        peroid_item = stock_signal.xs(item,
                level=0,
                drop_level=False)
        # 可卖数量
        buy_candidates = peroid_item[peroid_item.buy>=0]
        sell_candidates = peroid_item[peroid_item.sell<=0]
        if not sell_candidates.empty:
            sells = sell_candidates.union(AC.hold_table().code.unique())
            for code in sells:
                cur_account_sotck_code_sell_available_amount = AC.sell_available.get(code, 0)
                order = AC.send_order(
                    code=code, time=cur_stock_date, towards=QA.ORDER_DIRECTION.SELL,
                    order_model=QA.ORDER_MODEL.MARKET, amount_model=QA.AMOUNT_MODEL.BY_AMOUNT,
                    amount=cur_account_sotck_code_sell_available_amount, price=0,
                )
                Broker.receive_order(QA.QA_Event(order=order, market_data=stock_item))
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
                    code=cur_stock_code, time=cur_stock_date, towards=QA.ORDER_DIRECTION.BUY,
                    order_model=QA.ORDER_MODEL.MARKET,
                    amount_model=QA.AMOUNT_MODEL.BY_MONEY,
                    money=maxMoney,
                    price=avgPrice,
                )
                Broker.receive_order(QA.QA_Event(order=order, market_data=stock_item))
                trade_mes = Broker.query_orders(AC.account_cookie, 'filled')
                res = trade_mes.loc[order.account_cookie, order.realorder_id]
                order.trade(res.trade_id, res.trade_price,
                            res.trade_amount, res.trade_time)
        AC.settle()
