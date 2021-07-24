# -*- coding: utf-8 -*-
import csv
import json
import time
import datetime
import numpy as np

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

    for id in ID:
        if not '404' in data[id]:
            data[id]['404'] = False

        link = 'https://m.weibo.cn/detail/'+data[id]['link']

        try:
            response = requests.get(link).text
            data = re.findall("var \$render_data = \[({.*})]\[0]", response, re.DOTALL)[0]
            data = json.loads(data)['status']
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

        # 这里把筛查河南的信息去掉了，因为有一些可能是地址识别的问题。
        if 'address' in v and len(v['address']) != 0:
            addr = v['address']
            if not addr in Address:
                Address[addr] = []
            Address[addr].append([v['time'], v['link'], v['post'], v['location'], v['404'], v['outdated']])

    with open(to_fname, 'w', newline='', encoding='utf-8') as f:
        f_csv = csv.writer(f)
        f_csv.writerow(['微博链接', '时间', '地址', '经度', '纬度', '微博内容', '已删除/已过期'])
        for addr in Address.keys():
            content = Address[addr]
            content.sort()
            for t, l, p, location, remove, outdated in content:
                useless = ''
                if remove or outdated:
                    useless = '是'
                f_csv.writerow([l, t, addr, location['lng'], location['lat'], p, useless])

