# -*- coding: utf-8 -*-
import csv
import json
import numpy as np

def data_update_saved(del_id = '0', del_keyword = 'error'):
    '''
    Update the info that has been saved.
    del_id: invalidate the id
    del_keyword: invalidate the posts containing the keyword
    '''
    data = np.load("latest_data.npy", allow_pickle=True)[()]

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

    np.save("latest_data", data)


def data_recover_saved(recover_id = '0'):
    '''
    Recover an invalid item
    '''
    data = np.load("latest_data.npy", allow_pickle=True)[()]

    if recover_id in data:
        data[recover_id]['valid'] = 1
        print("recover " + data[recover_id]['post'])

    np.save("latest_data", data)


def data_export():
    '''
    Used to export data for visualization,
    data in json format
    '''
    data = np.load("latest_data.npy", allow_pickle=True)[()]

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


def data_export_csv(to_fname='final.csv'):
    data = np.load("latest_data.npy", allow_pickle=True)[()]

    Address = dict()
    data_ids = data.keys()

    for data_id in data_ids:
        v = data[data_id]

        if 'address' in v and "河南" in v['address'] and v['valid'] == 1:
            addr = v['address']
            if not addr in Address:
                Address[addr] = []
            Address[addr].append([v['time'], v['link'], v['post'], v['location']])

    with open(to_fname, 'w', newline='', encoding='utf-8') as f:
        f_csv = csv.writer(f)
        f_csv.writerow(['地址', '时间', '微博链接', '微博内容', '经度', '纬度'])
        for addr in Address.keys():
            content = Address[addr]
            content.sort()
            for t, l, p, location in content:
                #p = p.encode('gbk', 'ignore').decode('utf-8','ignore')
                f_csv.writerow([addr, t, l, p, location['lng'], location['lat']])

