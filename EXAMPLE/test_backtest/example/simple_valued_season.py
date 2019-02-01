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
    for peroid_item in stock_signal.panel_gen:
        for stock_item in peroid_item.security_gen:

            cur_stock_code = stock_item.code[0]
            cur_stock_date = stock_item.date[0]
            # 可卖数量
            cur_account_sotck_code_sell_available_amount = AC.sell_available.get(cur_stock_code, 0)

            cur_sotck_operation_towards = None
            # todo 提取对应的信号
            cur_stock_signal = stock_signal.loc[cur_stock_date, cur_stock_code]['DIFF']
            # 剔除无效信号
            if cur_stock_signal is None or math.isnan(cur_stock_signal):
                continue

            if cur_stock_signal > 0 and cur_account_sotck_code_sell_available_amount == 0:
                # 买入信号, 持仓为0, 方向: 买入
                # 买入方式, QA.ORDER_DIRECTION.BUY, QA.ORDER_DIRECTION.BUY_CLOSE, QA.ORDER_DIRECTION.BUY_OPEN
                cur_sotck_operation_towards = QA.ORDER_DIRECTION.BUY

            elif cur_stock_signal > 0 and cur_account_sotck_code_sell_available_amount > 0:
                # 买入信号, 持仓不为0, 方向: 持有
                cur_sotck_operation_towards = None
                pass
            elif cur_stock_signal < 0 and cur_account_sotck_code_sell_available_amount == 0:
                # 卖出信号, 持仓为0, 忽略
                cur_sotck_operation_towards =None
                pass
            elif cur_stock_signal < 0 and cur_account_sotck_code_sell_available_amount > 0:
                # 买入信号, 持仓不为0, 方向: 卖出
                cur_sotck_operation_towards = QA.ORDER_DIRECTION.SELL

            if cur_sotck_operation_towards is None:
                continue

            order = None
            if cur_sotck_operation_towards == QA.ORDER_DIRECTION.BUY:
                # 97 % 仓 买入
                maxMoney = 0.97 * AC.cash_available
                # closePrice = stock_item.close[0]
                highPrice = stock_item.high[0]
                lowPrice = stock_item.low[0]
                avgPrice = (highPrice + lowPrice) / 2

                if round(avgPrice, 2) < avgPrice:
                    avgPrice = round(avgPrice, 2) + 0.01

                maxBuyAmount = int(maxMoney / (avgPrice * (1 + commission_coeff) * 100)) * 100

                print('cash_available {}'.format(AC.cash_available))
                print("可用资金: %s, 预计买入 %s , %s, %s, %s,股" % (maxMoney, cur_stock_code, cur_stock_date, avgPrice, maxBuyAmount))
                order = AC.send_order(
                    code=cur_stock_code, time=cur_stock_date, towards=cur_sotck_operation_towards,
                    order_model=QA.ORDER_MODEL.MARKET,
                    amount_model=QA.AMOUNT_MODEL.BY_MONEY,
                    money=maxMoney,
                    price=avgPrice,
                )
            elif cur_sotck_operation_towards == QA.ORDER_DIRECTION.SELL:
                # 市价 全仓卖出
                order = AC.send_order(
                    code=cur_stock_code, time=cur_stock_date, towards=cur_sotck_operation_towards,
                    order_model=QA.ORDER_MODEL.MARKET, amount_model=QA.AMOUNT_MODEL.BY_AMOUNT,
                    amount=cur_account_sotck_code_sell_available_amount, price=0,

                )

            if order is not None and order:
                Broker.receive_order(QA.QA_Event(order=order, market_data=stock_item))
                trade_mes = Broker.query_orders(AC.account_cookie, 'filled')
                res = trade_mes.loc[order.account_cookie, order.realorder_id]
                order.trade(res.trade_id, res.trade_price,
                            res.trade_amount, res.trade_time)
        # 当天结算
        AC.settle()