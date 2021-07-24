from dataFetcher.dataProcessUtils import *
from dataFetcher.baiduApiWrapper import *
from dataFetcher.weiboDataFetcher import *
from dataSyncer.syncWithFeishu import *
import argparse
import shutil
import os

parser = argparse.ArgumentParser(description='Scraper for henan flood-related weibo')
parser.add_argument('--cache', type=str, default='latest_data.npy', help='cache numpy array')
parser.add_argument('--json_output', type=str, default='final.json', help='output json file')
parser.add_argument('--csv_output', type=str, default='final.csv', help='output json file')
parser.add_argument('--api_key', type=str, default=None, help='baidu API key')
parser.add_argument('--api_secret', type=str, default=None, help='baidu API secret')
parser.add_argument('--feishu_app_id', type=str, default=None, help='Feishu API key')
parser.add_argument('--feishu_app_secret', type=str, default=None, help='Feishu API secret')

def backup_if_exist(prefix):
    now = datetime.datetime.now()
    if os.path.exists(prefix+".npy"):
        shutil.copy(prefix+".npy", prefix + "." + now.strftime('%Y%m%d%H%M%S') + ".old.npy")

def fetch_n_export(args):
    Weibo_Fetcher = WeiboDataFetcher()
    Api_Wrapper = BaiduAPIWrapper(args.api_key, args.api_secret)

    for i in tqdm(range(50)):
        Weibo_Fetcher.fetch_weibo_data(args.cache, "河南暴雨互助", page=i, stop_if_repeat=False)
        Api_Wrapper.extract_addresses_from_data(args.cache)

    data_date_valid(args.cache)
    data_link_valid(args.cache)
    data_content_filter(args.cache)
    data_export_csv(args.cache, args.csv_output)
    
    feishuSyncer = FeishuSyncer(args.feishu_app_id, args.feishu_app_secret)
    feishuSyncer.startSync(local_csv=args.csv_output, 
                            save_local_path=args.json_output)

if __name__ == "__main__":
    args = parser.parse_args()

    while True:
        backup_if_exist(args.cache)
        fetch_n_export(args)