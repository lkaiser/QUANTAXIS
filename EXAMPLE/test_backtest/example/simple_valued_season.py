#-*-coding:utf-8-*-
import math
import QUANTAXIS as QA

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
    # 取数并前复权
    DATA = QA.QA_fetch_stock_day_adv(code, start, end).to_qfq()
    # todo 计算信号
    stock_signal = DATA.add_func(simple_valued, 12, 26, 9)