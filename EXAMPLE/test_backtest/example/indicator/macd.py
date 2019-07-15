# -*- coding: utf-8 -*-
import QUANTAXIS as QA
import pandas as pd
import functools

def MACD_JCSC(dataframe, SHORT=12, LONG=26, M=9):
    """
    1.DIF向上突破DEA，买入信号参考。
    2.DIF向下跌破DEA，卖出信号参考。
    """
    CLOSE = dataframe.close
    DIFF = QA.EMA(CLOSE, SHORT) - QA.EMA(CLOSE, LONG)
    DEA = QA.EMA(DIFF, M)
    MACD = 2*(DIFF-DEA)

    CROSS_JC = QA.CROSS(DIFF, DEA)
    CROSS_SC = QA.CROSS(DEA, DIFF)
    ZERO = 0
    return pd.DataFrame({'DIFF': DIFF, 'DEA': DEA, 'MACD': MACD, 'CROSS_JC': CROSS_JC, 'CROSS_SC': CROSS_SC, 'ZERO': ZERO})

from functools import wraps
def wrap3(func):
    @wraps(func)
    def call_it(*args, **kwargs):
        """wrap func: call_it2"""
        print('before call')
        return func(*args, **kwargs)
    return call_it
@wrap3
def hello3():
    """test hello 3"""
    print('hello world3')

def hello4():
    print('hello world4')

def delayed_function():
    if 1:
        a = 2
    else:
        a = 3
    print(a)

if __name__ == '__main__':
    print('wtf')
    hello3()
    a = functools.wraps(hello4)(delayed_function)
    a()