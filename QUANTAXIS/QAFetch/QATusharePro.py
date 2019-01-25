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




def QA_fetch_get_finindicator(code,start, end,collections=DATABASE.stock_report_finindicator_tushare):
    query = { "end_date": {
            "$lte": end,
            "$gte": start}}
    if code:
        query['code'] = {'$in': code}
    cursor = collections.find(query, {"_id": 0}, batch_size=10000)

    return pd.DataFrame([item for item in cursor])


def QA_fetch_get_dailyindicator(code,start, end,collections=DATABASE.stock_daily_basic_tushare):
    cursor = collections.find({
        'code': {'$in': code}, "trade_date": {
            "$lte": end,
            "$gte": start}}, {"_id": 0}, batch_size=10000)

    return pd.DataFrame([item for item in cursor])

# test

# print(get_stock_day("000001",'2001-01-01','2010-01-01'))
# print(get_stock_tick("000001.SZ","2017-02-21"))
