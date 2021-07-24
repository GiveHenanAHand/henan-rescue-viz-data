# -*- coding: utf-8 -*-
import json
import time
import requests
import pandas as pd
from requests.api import request

from syncWithFeishu import *

class CustomFeishuySyncer(FeishuSyncer):
    def __init__(self, APP_id, APP_secret, baidu_ak):
        FeishuSyncer.__init__(self, APP_id, APP_secret)
        #baidu api key for getting position
        self.baidu_ak = baidu_ak

    def cleanURLCell(self, cell):
        if isinstance(cell, list):
            #this is a text with url, feishu will return [{'type':'url', 'text':"https://blabla"}]
            itemUrl = cell[0]['text']
            return itemUrl
        else:
            return cell

    '''
    百度api, 根据地址获取经纬度
    '''
    def getLocation2D(loc_text):
        if len(loc_text) == 0:
            return {'lng':0, 'lat':0}
        # api-endpoint
        URL = "http://api.map.baidu.com/geocoding/v3"
        # defining a params dict for the parameters to be sent to the API
        PARAMS = {'address':loc_text, "output": "json", "ak": self.baidu_ak}
        # sending get request and saving the response as response object
        r = requests.get(url = URL, params = PARAMS)
        rjson = r.json()
        if rjson['status'] == 0:
            return rjson['result']['location']
        return {'lng':0, 'lat':0}

    '''
    向文档写入数据
    '''
    def updateRange(fromCol, fromRow, toCol, toRow, vals):
        #write the newly scraped data to Feishu
        insertRange = "%s!%s%d:%s%d" % (self.sheetID, fromCol, fromRow, toCol, toRow)
        InsertDataURL = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/%s/values" % self.sheetToken
        params = {
            "valueRange": {
                "range": insertRange,
                "values": vals
            }
        }
        r = requests.put(InsertDataURL, headers=self.HEADER, json=params)

    '''
    用来展示任意带有 地址 经度 纬度 微博内容(或内容) 的文档条目
    '''
    def read_custom_sheet_data(self, Nth_sheet=0):
        self.verify_access()
        if Nth_sheet < 0 or Nth_sheet >= len(self.sheetMetaJson["data"]["sheets"]):
            raise Exception("[CustomFeishuySyncer Error] Sheet Index Wrong")
        self.sheetID = self.sheetMetaJson["data"]["sheets"][Nth_sheet]["sheetId"]

        # use the first line as the default titles
        self.get_title_columns()
        lng_idx, lat_idx, address_idx = self.title_cols.index('经度'), self.title_cols.index('纬度'), self.title_cols.index('地址')
        lng_col, lat_col = chr(ord('A')+lng_idx), chr(ord('A')+lat_idx)
        self.has_link = ('微博链接' in self.title_cols) | ('链接' in self.title_cols)

        # read the rest data
        reading_end = False
        line_per_read = 500
        starting_line = 2
        read_rows = []
        total_rows = starting_line-1
        while not reading_end:
            ranges = "?ranges=%s!%s%d:%s%d" %\
                (self.sheetID, self.startCol, starting_line, self.stopCol, starting_line+line_per_read)
            BatchGetRangeURL = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/%s/values_batch_get" % self.sheetToken + ranges
            r = requests.get(BatchGetRangeURL, headers=self.HEADER)
            rjson = r.json()
            if 'data' not in rjson:
                break
            empty_lines = 0 # how many empty lines this fetch has
            last_non_empty_line = 0
            fetched_rows = r.json()['data']['valueRanges'][0]['values']
            cleaned_rows = []
            for (i, row) in enumerate(fetched_rows):
                if all(v is None for v in row):
                    empty_lines += 1
                    # if more than 5 empty line, we think it's ending
                    continue

                last_non_empty_line = starting_line+i
                cleaned_row = [self.cleanURLCell(itm) for itm in row]
                if cleaned_row[lng_idx] is None and cleaned_row[address_idx] != None:
                    # if the location is empty,
                    # we request for longitude/latitude
                    loc = getLocation2D(cleaned_row[address_idx])
                    # and update the feishu sheet
                    updateRange(lng_col, starting_line+i, lat_col, starting_line+i, [[loc['lng'], loc['lat']]])
                    print('[Feishu Custom Syncer Info] update loc for row %d' % last_non_empty_line, loc)
                    cleaned_row[lng_idx] = loc['lng']
                    cleaned_row[lat_idx] = loc['lat']
                cleaned_rows.append(cleaned_row)
            read_rows += cleaned_rows
            starting_line += line_per_read+1
            # if more than 5 empty line, we think it's ending
            if empty_lines >= 5:
                reading_end = True
                self.feishu_total_rows = last_non_empty_line
        edited_doc = pd.DataFrame(read_rows)
        edited_doc.columns = self.title_cols
        self.read_doc = edited_doc
        print('[Feishu Custom Syncer Info] read feishu with %d rows' % self.feishu_total_rows)


    def export_to_json(self, saveResPath):
        read_doc = self.read_doc.rename(columns=CHN_COL_TO_ENG)
        if 'valid_info' in read_doc:
            read_doc = read_doc.loc[read_doc['valid_info'] != '否']
        if 'outdated_or_deleted' in read_doc:
            read_doc = read_doc.loc[read_doc['outdated_or_deleted'] != '是']
        read_doc = read_doc.where(pd.notnull(read_doc), None)
        export_dict = read_doc.to_dict(orient='records')
        for i,item in enumerate(export_dict):
            item['location']= {'lng':item['lng'], 'lat':item['lat']}
            if not self.has_link:
                # we generate a random
                item['link'] = 'no_link%s_%s_%d' % (self.sheetToken, self.sheetID, i)

        with open(saveResPath, 'w', encoding='utf-8') as file:
            json.dump(export_dict, file, indent=4, ensure_ascii=False)
        print('[Feishu Custom Syncer Info] Export json with %d items saved to %s' % (read_doc.shape[0], saveResPath))

    def startExport(self, export_path, feishu_sheet_token, feishu_sheet_nth=0):
        self.get_sheet_meta_data(feishu_sheet_token)
        self.read_custom_sheet_data(feishu_sheet_nth)
        self.export_to_json(export_path)
