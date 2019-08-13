#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2019-07-25 11:07:37 by lk
# Project: niuhuansuan2

from pyspider.libs.base_handler import *
import time
import pandas as pd
import re


class Handler(BaseHandler):
    crawl_config = {
    }

    @every(minutes=4 * 60)
    def on_start(self):
        pass
        self.crawl('https://price.21food.cn/quote_list06013003-p1.html', callback=self.index_page)

    @config(age=1)
    def index_page(self, response):
        tr = response.doc('.sjs_top_cent_erv > ul tr')
        now = time.strftime("%Y-%m-%d", time.localtime())
        arr = []
        for p in tr.items():
            arr.append((float(re.findall(r"\d+", p('td').eq(3).text().strip())[0]),p('td').eq(4).text().strip()))
        df = pd.DataFrame(data=arr,columns=['price','date'])
        dvc = df.date.value_counts()
        if dvc.values[0]>10:
            df = df[df.date==dvc.index[0]]
            price = df.price
            med = price.median().item()
            mad = abs(price - med).median()
            midmean = price.where(lambda x : abs(x-med)<mad*2).mean()
            midmean = round(midmean,2)

            return {
                "price_med": med,
                "price_midmean": midmean,
                "date":now
            }