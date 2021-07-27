# -*- coding: utf-8 -*-
import boto3
import requests
from syncWithFeishu import *
import time
import argparse

'''
Sync the data from dynamodb (which stores the revised category values from the front-end)
with Feishu
'''

ACCESS_KEY = "YOUR_AWS_KEY"
SECRET_KEY = 'YOUR_AWS_SECRET'

APP_ID = 'FEISHU_ID'
APP_SECRET = 'FEISHU_SECRET'
DEFAULT_SHEET_TOKEN="shtcnh4177SPTo2N8NglZHCirDe"

class DynamoDB(object):
    def __init__(self):
        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name='ap-east-1',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY
        )
        self.viztable = self.dynamodb.Table('Henan-viz')

    def reset_table(self):
        try:
            self.viztable.delete()
            self.viztable.wait_until_not_exists()
        except:
            pass
        print('creating table')
        self.viztable = self.dynamodb.create_table(
            TableName='Henan-viz',
            KeySchema=[
                {
                    'AttributeName': 'itemID',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'itemID',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 4,
                'WriteCapacityUnits': 4
            },
            StreamSpecification={
                'StreamEnabled': False
            }
        )

    def get_all_items(self):
        items = self.viztable.scan()['Items']
        return items

class FeishuUpdate(FeishuSyncer):
    # default sheet: https://u9u37118bj.feishu.cn/sheets/shtcnh4177SPTo2N8NglZHCirDe
    def get_sheet_meta_data(self, SpreadSheetToken=DEFAULT_SHEET_TOKEN):
        super().get_sheet_meta_data()
        self.sheetID = self.sheetMetaJson["data"]["sheets"][0]["sheetId"]
        self.get_title_columns()
        rc_idx = self.title_cols.index('revised_category')
        self.rc_col = chr(ord('A')+rc_idx)

    def find_cell_range(self, content):
        MatchURL = "https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/%s/sheets/%s/find" % (self.sheetToken, self.sheetID)
        params = {
            "find_condition": {
                "range": "%s!A:A" % self.sheetID
            },
            "find": content
        }
        r = requests.post(MatchURL, headers=self.HEADER, json=params)
        rjson = r.json()
        if 'data' not in rjson:
            return None
        cells = rjson['data']['find_result']['matched_cells']
        if len(cells) == 0:
            return None
        return cells[0][1:]

    def update_cell_category(self, row, category):
        changeRange = "%s!%s:%s" % (self.sheetID, self.rc_col+row, self.rc_col+row)
        InsertDataURL = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/%s/values" % self.sheetToken
        params = {
            "valueRange": {
                "range": changeRange,
                "values": [[category]]
            }
        }
        r = requests.put(InsertDataURL, headers=self.HEADER, json=params)
        rjson = r.json()
        if 'msg' not in rjson or rjson['msg'] != 'Success':
            print('[FeishuUpdater Info] write to Feishu sheet failed!')
            return

db = DynamoDB()
fu = FeishuUpdate(APP_ID, APP_SECRET)
fu.get_sheet_meta_data()

def syncFeishuCategory():
    items = db.get_all_items()
    for item in items:
        if 'category' in item:
            row = fu.find_cell_range(item['itemID'])
            if row:
                fu.update_cell_category(row, item['category'])
    print('[FeishuUpdater Info] Updated %d data.' % len(items))
    if len(items) > 50:
        db.reset_table()

if __name__ == "__main__":
    starttime = time.time()
    while True:
        syncFeishuCategory()
        time.sleep(300)

