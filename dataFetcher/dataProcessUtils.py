# -*- coding: utf-8 -*-
import re
import csv
import json
import time
import datetime
import requests
import numpy as np
import pandas as pd
from tqdm import tqdm

def data_date_valid(cache_path):
    data = np.load(cache_path + ".npy", allow_pickle=True)[()]

    ID = data.keys()

    for id in ID:
        if not 'dated' in data[id]:
            data[id]['outdated'] = False

        Time = data[id]['time']
        Time = time.strptime(Time, "%a %b %d %H:%M:%S %z %Y")
        Today = time.localtime()

        delttime = (time.mktime(Today)-time.mktime(Time)) / (24 * 60 * 60)

        if delttime > 3:
            data[id]['outdated'] = True

    np.save(cache_path, data)

def data_link_valid(cache_path):
    data = np.load(cache_path + ".npy", allow_pickle=True)[()]

    ID = data.keys()

    for id in tqdm(ID):
        if not '404' in data[id]:
            data[id]['404'] = False

        continue

        if data[id]['outdated']:
            continue

        link = data[id]['link']
        time.sleep(1)

        try:
            response = requests.get(link).text
            d = re.findall("var \$render_data = \[({.*})]\[0]", response, re.DOTALL)[0]
            d = json.loads(d)['status']
        except:
            data[id]['404'] = True

    np.save(cache_path, data)

def data_update_saved(cache_path, del_id = '0', del_keyword = 'error'):
    '''
    Update the info that has been saved.
    del_id: invalidate the id
    del_keyword: invalidate the posts containing the keyword
    '''
    data = np.load(cache_path+".npy", allow_pickle=True)[()]

    ID = data.keys()

    for id in ID:
        if not 'valid' in data[id]:
            data[id]['valid'] = 1

        if data[id]['valid'] == 0:
            continue

        if del_keywords in data[id]['post']:
            print("delete "+data[id]['link']+data[id]['post'])
            data[id]['valid'] = 0

    if del_id in ID:
        data[del_id]['valid'] = 0
        print("delete " + data[del_id]['post'])

    np.save(cache_path, data)


def data_recover_saved(cache_path, recover_id = '0'):
    '''
    Recover an invalid item
    '''
    data = np.load(cache_path+".npy", allow_pickle=True)[()]

    if recover_id in data:
        data[recover_id]['valid'] = 1
        print("recover " + data[recover_id]['post'])

    np.save(cache_path, data)

def data_export(cache_path, output_path):
    '''
    Used to export data for visualization,
    data in json format
    '''
    data = np.load(cache_path+".npy", allow_pickle=True)[()]

    news = []
    ID = data.keys()

    for id in ID:
        v = data[id]
        if 'address' in v and "河南" in v['address'] and v['valid'] == 1:
            news.append({
                "Time":v['time'], 
                "address":v['address'], 
                "location":v['location'], 
                "post":v['post'], 
                "link":v["link"]})

    with open(output_path, "w", encoding="utf-8") as fp:
        json.dump(news, fp, ensure_ascii=False, indent=4)

    print("Export %d info"%len(news))


def data_export_csv(cache_path, to_fname='final.csv'):
    data = np.load(cache_path+".npy", allow_pickle=True)[()]
    df = pd.DataFrame.from_dict(data, orient='index')
    df = df.loc[df['address'].str.contains("河南")].sort_values(by=['time','link'], ascending=False)
    df['outdated'] = df['outdated'].map({True: '是', False: '否'})
    df['urgent'] = df['urgent'].map({True: '有效', False: '无效'})
    locations = pd.DataFrame(df['location'].to_list())
    df['lng'] = list(locations['lng'])
    df['lat'] = list(locations['lat'])
    df.rename(columns={'time': '时间', 'link': '微博链接', 'address':'地址',
                    'post':'微博内容', 'lng':'经度', 'lat':'纬度', 
                    'category':'分类', 'outdated':'已过期或已删除',
                   'urgent':'勿动_机器分类_有效'}, inplace=True)
    df = df[['微博链接', '时间', '地址', '经度', '纬度', '微博内容', '分类', '已过期或已删除', '勿动_机器分类_有效']]
    df.to_csv(to_fname, encoding='utf-8', index=False)
    print('Fetched data exported to %s' % to_fname)
