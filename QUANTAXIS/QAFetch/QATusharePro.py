# coding: utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2018 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import json
import pandas as pd
import tushare as ts

from QUANTAXIS.QAUtil import (DATABASE, QASETTING, QA_util_date_stamp,
                              QA_util_date_valid, QA_util_dict_remove_key,
                              QA_util_log_info, QA_util_code_tolist, QA_util_date_str2int, QA_util_date_int2str,
                              QA_util_sql_mongo_sort_DESCENDING,
                              QA_util_time_stamp, QA_util_to_json_from_pandas,
                              trade_date_sse)



def set_token(token=None):
    try:
        if token is None:
            token = QASETTING.get_config('TSPRO', 'token', None)
        else:
            QASETTING.set_config('TSPRO', 'token', token)
        ts.set_token(token)
    except:
        print('请升级tushare 至最新版本 pip install tushare -U')


def get_pro():
    try:
        set_token()
        pro = ts.pro_api()
    except Exception as e:
        if isinstance(e, NameError):
            print('请设置tushare pro的token凭证码')
        else:
            print('请升级tushare 至最新版本 pip install tushare -U')
            print(e)
        pro = None
    return pro




def QA_fetch_get_finindicator(start, end,code=None,collections=DATABASE.stock_report_finindicator_tushare):
    query = { "end_date": {
            "$lte": end,
            "$gte": start}}
    if code:
        query['ts_code'] = {'$in': code}
    cursor = collections.find(query, {"_id": 0}, batch_size=10000)#.sort([("ts_code",1),("end_date",1)])
    data = []
    i = 0
    for post in cursor:
        i = i+1
        #print(post)
        data.append(post)
    return pd.DataFrame(data).sort_values(['ts_code','end_date'], ascending = True)

def QA_fetch_get_assetAliability(start, end,code=None,collections=DATABASE.stock_report_assetliability_tushare):
    query = {"end_date": {
        "$lte": end,
        "$gte": start}}
    if code:
        query['ts_code'] = {'$in': code}
    cursor = collections.find(query, {"_id": 0}, batch_size=10000)#.sort([("ts_code",1),("end_date",1)])

    return pd.DataFrame([item for item in cursor]).sort_values(['ts_code','end_date'], ascending = True)

def QA_fetch_get_cashflow(start, end,code=None,collections=DATABASE.stock_report_cashflow_tushare):
    query = {"end_date": {
        "$lte": end,
        "$gte": start}}
    if code:
        query['ts_code'] = {'$in': code}
    cursor = collections.find(query, {"_id": 0}, batch_size=10000)#.sort([("ts_code",1),("end_date",1)])
    #df = pd.DataFrame([item for item in cursor])
    #print(df.head())
    return pd.DataFrame([item for item in cursor]).sort_values(['ts_code','end_date'], ascending = True)

def QA_fetch_get_income(start, end,code=None,collections=DATABASE.stock_report_income_tushare):
    query = {"end_date": {
        "$lte": end,
        "$gte": start}}
    if code:
        query['ts_code'] = {'$in': code}
    cursor = collections.find(query, {"_id": 0}, batch_size=10000)#.sort([("ts_code",1),("end_date",1)])

    return pd.DataFrame([item for item in cursor]).sort_values(['ts_code','end_date'], ascending = True)

def QA_SU_stock_info():
    pro = get_pro()
    return pro.stock_basic()

def QA_fetch_get_daily_adj(start, end,code=None,collections=DATABASE.stock_daily_adj_tushare):
    query = {"trade_date": {
        "$lte": end,
        "$gte": start}}
    if code:
        query['ts_code'] = {'$in': code}
    cursor = collections.find(query, {"_id": 0}, batch_size=10000)#.sort([("ts_code",1),("trade_date",1)])

    return pd.DataFrame([item for item in cursor])#sort_values(['ts_code','trade_date'], ascending = True)

def QA_fetch_get_dailyindicator(start, end,code=None,collections=DATABASE.stock_daily_basic_tushare):
    query = {"trade_date": {
        "$lte": end,
        "$gte": start}}
    if code:
        query['ts_code'] = {'$in': code}
    cursor = collections.find(query, {"_id": 0}, batch_size=10000)#.sort([("ts_code",1),("trade_date",1)])

    return pd.DataFrame([item for item in cursor])#sort_values(['ts_code','trade_date'], ascending = True)

def QA_fetch_get_industry_daily(start, end,industry=None,collections=DATABASE.industry_daily_tushare):
    query = {"trade_date": {
        "$lte": end,
        "$gte": start}}
    if industry:
        query['industry'] = {'$in': industry}
    cursor = collections.find(query, {"_id": 0}, batch_size=10000)#.sort([("ts_code",1),("trade_date",1)])

    return pd.DataFrame([item for item in cursor])#sort_values(['ts_code','trade_date'], ascending = True)

if __name__ == '__main__':
    fin = QA_fetch_get_finindicator(start='20100101', end='20181231',code=['006160.SH','002056.SZ'])
    #print(len(fin))

# test

# print(get_stock_day("000001",'2001-01-01','2010-01-01'))
# print(get_stock_tick("000001.SZ","2017-02-21"))
