# -*- coding: utf-8 -*-
import re
import json
import requests
import numpy as np
from tqdm import tqdm

class BaiduAPIWrapper(object):
    def __init__(self, API_KEY, SECRET_KEY):
        self.APIURL = 'https://aip.baidubce.com/oauth/2.0/token?grant_type=client_credentials&client_id=%s&client_secret=%s' % (
        API_KEY, SECRET_KEY)

    def extract_addresses_from_data(self, cache_path):
        '''
        Process address in weibo data by querying Baidu api
        '''

        data = np.load(cache_path+".npy", allow_pickle=True)[()]
        data_ids = data.keys()
        cnt = 0
        for id in tqdm(data_ids):
            if not 'address' in data[id]:
                data[id]['address'] = ''
                data[id]['location'] = ''
                address_query = self.extract_address_from_text(data[id]['post'])
                data[id]['address'] = address_query['address']
                data[id]['location'] = address_query['location']
                cnt += 1
        print("Query %d info"%cnt)
        np.save(cache_path, data)


    def preprocess_text(self, content):
        '''
        Preprocessing the content from weibo to get an accurate address
        '''
        content = list(content)
        lst = -1
        if "#" in content:
            for i in range(len(content)):
                if content[i] == "#":
                    if lst != 0:
                        for j in range(lst, i + 1):
                            content[j] = ' '
                        lst = -1
                    else:
                        lst = i
        content = ''.join(content)

        punctuation = r"""!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~“”？#?，！【】（）、。：；’‘……￥·"""

        content = re.sub(r'[{}]+'.format(punctuation), ' ', content)
        return content.strip()

    def extract_address_from_text(self, content):
        '''
        Query content in Baidu to get the address
        '''
        content = self.preprocess_text(content)
        
        response = requests.get(self.APIURL)

        Result = dict()
        Result['address'] = ''
        Result['location'] = ''

        if response:
            rjson = response.json()
            access_token = rjson['access_token']

            r = requests.post("https://aip.baidubce.com/rpc/2.0/nlp/v1/address?access_token=%s" % access_token,
                              json={'text': content})

            data = json.loads(r.text)

            if not 'province' in data:
                return Result

            location = data['province'] + data['city'] + data['town']

            if (len(location) == 0):
                return Result

            latitude = data['lat']
            longitude = data['lng']

            Result['address'] = location
            Result['location'] = {"lng": longitude, "lat": latitude}

        return Result
