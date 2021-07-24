# -*- coding: utf-8 -*-
import re
import csv
import json
import time
import jieba
import datetime
import requests
import numpy as np
from tqdm import tqdm

def data_content_filter(cache_path):
    data = np.load(cache_path + ".npy", allow_pickle=True)[()]
    dict_word_calculate = {}

    file = open(r'dict.txt', 'r', encoding='utf-8')

    lines = file.readlines()
    for line in lines:
        name = line.strip().split()[0]
        number = float(line.strip().split()[1])
        dict_word_calculate[name] = number

    ID = data.keys()

    for id in ID:
        if not 'urgent' in data[id]:
            data[id]['urgent'] = True

        word_list = []

        seg_list = jieba.cut(data[id]['post'], cut_all=False, HMM=True)
        split_line = " ".join(seg_list).split()
        # print(split_line)
        for words in split_line:
            for word in words:
                if u'\u4e00' <= word <= u'\u9fff':
                    word_list.append(words)
                    break

        # print(word_list)
        result = 1
        for words in word_list:
            if dict_word_calculate.get(words) != None:
                result = result * dict_word_calculate.get(words)

        if result < 1:
            data[id]['urgent'] = False

    np.save(cache_path, data)

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

def data_export(cache_path):
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

    with open("final.json", "w", encoding="utf-8") as fp:
        json.dump(news, fp, ensure_ascii=False, indent=4)

    print("Export %d info"%len(news))


def data_export_csv(cache_path, to_fname='final.csv'):
    data = np.load(cache_path+".npy", allow_pickle=True)[()]

    Address = dict()
    data_ids = data.keys()

    for data_id in data_ids:
        v = data[data_id]

        if 'address' in v and "河南" in v['address'] and len(v['address']) != 0:
            addr = v['address']
            if not addr in Address:
                Address[addr] = []
            Address[addr].append([v['time'], v['link'], v['post'], v['location'], v['404'], v['outdated'], v['urgent']])

    with open(to_fname, 'w', newline='', encoding='utf-8') as f:
        f_csv = csv.writer(f)
        f_csv.writerow(['微博链接', '时间', '地址', '经度', '纬度', '微博内容', '已过期或已删除', '勿动_机器分类_有效'])
        for addr in Address.keys():
            content = Address[addr]
            content.sort()
            for t, l, p, location, r, o, u in content:
                useless = '否'
                if r or o:
                    useless = '是'
                urgent = '有效'
                if u == False:
                    urgent = '无效'
                print(u, urgent)
                f_csv.writerow([l, t, addr, location['lng'], location['lat'], p, useless, urgent])

