# -*- coding: utf-8 -*-
import json
import requests
import pandas as pd
from requests.api import request

'''
A syncer dealing with Feishu sheet
Trying to sync the edited items with Feishu
And add new items at the same time
'''
CHN_COL_TO_ENG = {
    '微博链接': 'link',
    '时间': 'Time',
    '地址': 'address',
    '经度': 'lng',
    '纬度': 'lat',
    '微博内容': 'post',
    '紧急': 'urgent',
    '分类': 'category',
    '已过期或已删除': 'outdated_or_deleted',
    '是否为有效信息': 'valid_info',
    '审核人': "reviewer",
    '经纬度已人工矫正': "is_corrected_latlong",
    '勿动_机器分类_有效': 'machine_valid_classification',
    '联系人': 'contact_person',
    '联系方式': 'contact_info',
    '信息来源地址': 'source_info',
    '内容': 'post',
    '链接': 'link'
}

APP_ID = 'YOUR_APP_ID'
APP_SECRET = 'YOUR_APP_SECRET'
DEFAULT_SHEET_TOKEN="shtcnh4177SPTo2N8NglZHCirDe"

class FeishuSyncer(object):
    def __init__(self, APP_id=APP_ID, APP_secret=APP_SECRET):
        
        # Get tenant_access_token
        TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
        TOKEN_DATA = {"app_id":APP_id,
                     "app_secret":APP_secret}
        r = requests.post(TOKEN_URL, data=TOKEN_DATA)
        if "tenant_access_token" in r.json():
            self.TENANT_ACCESS_TOKEN = str(r.json()["tenant_access_token"])
        else:
            self.TENANT_ACCESS_TOKEN = None
        self.HEADER = {"content-type":"application/json; charset=utf-8",
                        "Authorization":"Bearer " + self.TENANT_ACCESS_TOKEN}

    def verify_access(self):
        if self.TENANT_ACCESS_TOKEN == None:
            raise Exception("[FeishuSyncer Error] No access token granted")

    # default sheet: https://u9u37118bj.feishu.cn/sheets/shtcnh4177SPTo2N8NglZHCirDe
    def get_sheet_meta_data(self, SpreadSheetToken=DEFAULT_SHEET_TOKEN):
        self.verify_access()
        MetaDataURL = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/%s/metainfo" % SpreadSheetToken
        r = requests.get(MetaDataURL, headers=self.HEADER)
        self.sheetMetaJson = r.json()
        self.sheetToken = SpreadSheetToken

    def get_title_columns(self):
        ranges = "?ranges=%s!%s%d:%s%d" %\
                    (self.sheetID, 'A', 1, 'Z', 1)
        BatchGetRangeURL = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/%s/values_batch_get" % self.sheetToken + ranges
        r = requests.get(BatchGetRangeURL, headers=self.HEADER)
        rjson = r.json()
        if 'data' not in rjson:
            raise Exception("[FeishuSyncer Error] Fail to read the first line")
        title_row = r.json()['data']['valueRanges'][0]['values'][0]
        self.title_cols = []
        for key in title_row:
            if key is not None:
                self.title_cols.append(key)
        self.startCol = 'A'
        self.stopCol = chr(ord('A')+len(self.title_cols)-1)

    def read_sheet_data(self, Nth_sheet=0):
        self.verify_access()
        if Nth_sheet < 0 or Nth_sheet >= len(self.sheetMetaJson["data"]["sheets"]):
            raise Exception("[FeishuSyncer Error] Sheet Index Wrong")
        self.sheetID = self.sheetMetaJson["data"]["sheets"][Nth_sheet]["sheetId"]

        # use the first line as the default titles
        self.get_title_columns()

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
                if isinstance(row[0], list):
                    #this is a text with url, feishu will return [{'type':'url', 'text':"https://blabla"}]
                    itemUrl = row[0][0]['text']
                    last_non_empty_line = starting_line+i
                elif row[0] != None:
                    itemUrl = row[0]
                    last_non_empty_line = starting_line+i
                else:
                    if all(v is None for v in row):
                        empty_lines += 1
                    # if more than 5 empty line, we think it's ending
                    continue
                new_row = [itemUrl]+row[1:]
                cleaned_rows.append(new_row)
            read_rows += cleaned_rows
            starting_line += line_per_read+1
            # if more than 5 empty line, we think it's ending
            if empty_lines >= 5:
                reading_end = True
                self.feishu_total_rows = last_non_empty_line
        edited_doc = pd.DataFrame(read_rows)
        edited_doc.columns = self.title_cols
        self.read_doc = edited_doc
        print('[Feishu Syncer Info] read feishu with %d rows' % self.feishu_total_rows)

    '''
    Load the scraped csv file
    '''
    def read_local_scraped_CSV(self, fname="final.csv"):
        scrapedCSV = pd.read_csv("final.csv")
        for column_name in self.title_cols:
            if column_name not in scrapedCSV:
                scrapedCSV[column_name] = ""
        self.scraped_csv = scrapedCSV

    def compare_Feishu_n_localFile(self, saveResPath='combined.json'):
        #Get newly scraped rows that's not in the Feishu doc
        newly_scraped_rows = pd.concat([self.scraped_csv, self.read_doc, self.read_doc])\
                                .drop_duplicates(subset=['微博链接'], keep=False)
        newly_scraped_rows = newly_scraped_rows[self.title_cols]
        print('[Feishu Syncer Info] Newly scraped %d rows' % newly_scraped_rows.shape[0])

        #Get valid items from the Feishu doc
        valid_rows_after_edit = self.read_doc.loc[(
            self.read_doc['是否为有效信息'] != '否') & (self.read_doc['已过期或已删除'] != '是')]
        #Further validate
        valid_rows_after_edit = valid_rows_after_edit.loc[(
            valid_rows_after_edit['是否为有效信息'] == '是') | (valid_rows_after_edit['勿动_机器分类_有效'] == '是')]
        
        #Combine old and new data for the server to show
        combinedCSV = pd.concat([valid_rows_after_edit, newly_scraped_rows])
        combinedCSV = combinedCSV[self.title_cols]
        #We need to convert the key names 
        #to confirm to the historical json format
        combinedCSV = combinedCSV.rename(columns=CHN_COL_TO_ENG)
        combinedCSV = combinedCSV.where(pd.notnull(combinedCSV), None)
        combined_dict = combinedCSV.to_dict(orient='records')
        for item in combined_dict:
            item['location']= {'lng':item['lng'], 'lat':item['lat']}
        with open(saveResPath, 'w', encoding='utf-8') as file:
            json.dump(combined_dict, file, indent=4, ensure_ascii=False)
        print('[Feishu Syncer Info] Combined json with %d items saved to %s' % (combinedCSV.shape[0], saveResPath))

        #write the newly scraped data to Feishu
        value_to_add = newly_scraped_rows.values.tolist()
        fromRow = self.feishu_total_rows+1
        toRow = fromRow+len(value_to_add)-1
        insertRange = "%s!%s%d:%s%d" % (self.sheetID, self.startCol, fromRow, self.stopCol, toRow)
        InsertDataURL = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/%s/values_append" % self.sheetToken
        params = {
            "valueRange": {
                "range": insertRange,
                "values": value_to_add
            }
        }
        r = requests.post(InsertDataURL, headers=self.HEADER, json=params)
        rjson = r.json()
        print(rjson)

    def startSync(self, local_csv='final.csv', save_local_path='combined.json', 
                    feishu_sheet_token=DEFAULT_SHEET_TOKEN, feishu_sheet_nth=0):
        self.get_sheet_meta_data(feishu_sheet_token)
        self.read_sheet_data(feishu_sheet_nth)
        self.read_local_scraped_CSV(local_csv)
        self.compare_Feishu_n_localFile(save_local_path)


if __name__ == "__main__":
    syncer = FeishuSyncer()
    syncer.get_sheet_meta_data()
    syncer.read_sheet_data()
#     print(syncer.title_cols)

    syncer.read_local_scraped_CSV()
    # print(syncer.scraped_csv.head(3))

    syncer.compare_Feishu_n_localFile()
