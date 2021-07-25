# -*- coding: utf-8 -*-
import os
import re
import time
import json
import requests
import numpy as np

class WeiboDataFetcher(object):
    def fetch_weibo_data(self, cache_path, keyword = "暴雨互助", page=10, stop_if_repeat=False):
        '''
        Acquisite data from weibo
        Keyword : keyword for search
        Page : pages of data to scrape
        Stop if repeat : only scrape the latest one when True, also scrape history when False
        '''

        # load the cache
        self.data = np.load(cache_path+".npy", allow_pickle=True)[()]

        params = {
            'containerid': '100103type=1&q=' + keyword,
            'page_type': 'searchall',
            'page': page
        }
        url = 'https://m.weibo.cn/api/container/getIndex?'
        response = requests.get(url, params=params).text
        id_ls = re.findall('"id":"(.{16}?)",', response, re.DOTALL)
        detail_url = ['https://m.weibo.cn/detail/' + i for i in id_ls]

        cnt = 0
        for i in detail_url:
            weibo_id = i[-16:]
            try:
                if weibo_id in self.data:
                    if stop_if_repeat:
                        break
                    else:
                        continue
                else:
                    self.data[weibo_id] = dict()
                time.sleep(1)

                response = requests.get(i).text
                data = re.findall("var \$render_data = \[({.*})]\[0]", response, re.DOTALL)[0]
                data = json.loads(data)['status']

                created_at_time = data['created_at']
                log_text = data['text']
                log_text = re.sub('<.*?>', '', log_text)
                print(created_at_time, i, log_text)

                self.data[weibo_id]['time'] = created_at_time
                self.data[weibo_id]['link'] = i
                self.data[weibo_id]['post'] = log_text
                self.data[weibo_id]['valid'] = 1

                cnt += 1
            except Exception:
                del(self.data[weibo_id])
                print("weibo fetching error")

        print("[WeiboDataFetcher] Aquisite %d info" % cnt)

        np.save(cache_path, self.data)