from dataFetcher.dataProcessUtils import *
from dataFetcher.baiduApiWrapper import *
from dataFetcher.weiboDataFetcher import *
from dataFetcher.MLContentClassifier import *
from dataSyncer.syncWithFeishu import *
from dataSyncer.customFeishuSync import *
import argparse
import shutil
import os

parser = argparse.ArgumentParser(description='Scraper for henan flood-related weibo')
parser.add_argument('--cache', type=str, default='latest_data', help='cache numpy array')
parser.add_argument('-pages', '--scrape_pages', type=int, default='50', help='the weibo pages for each scrape round')
parser.add_argument('-mlfolder', '--ml_dict_folder', type=str, default='modal_dicts', help='ml modal dict folder')
parser.add_argument('--json_output', type=str, default='final.json', help='output json file')
parser.add_argument('--json_manual_output', type=str, default='manual.json', help='manual data output json file')
parser.add_argument('--csv_output', type=str, default='final.csv', help='output json file')
parser.add_argument('--api_key', type=str, default=None, help='baidu API key')
parser.add_argument('--api_secret', type=str, default=None, help='baidu API secret')
parser.add_argument('--baidu_ak', type=str, default=None, help='baidu maps API key')
parser.add_argument('--feishu_app_id', type=str, default=None, help='Feishu API key')
parser.add_argument('--feishu_app_secret', type=str, default=None, help='Feishu API secret')
parser.add_argument('--backup_count', type=int, default='5', help="the max number of the latest backup cache")

def backup_if_exist(prefix, backup_id):
    if os.path.exists(prefix+".npy"):
        shutil.copy(prefix+".npy", prefix + "." + str(backup_id) + ".old.npy")

def fetch_n_export(args):
    Weibo_Fetcher = WeiboDataFetcher()
    Api_Wrapper = BaiduAPIWrapper(args.api_key, args.api_secret)
    contentClassifier = MLContentClassifier(args.ml_dict_folder)

    for i in tqdm(range(args.scrape_pages)):
        Weibo_Fetcher.fetch_weibo_data(args.cache, "河南暴雨互助", page=i, stop_if_repeat=False)
        Api_Wrapper.extract_addresses_from_data(args.cache)

    data_date_valid(args.cache)
    data_link_valid(args.cache)
    contentClassifier.data_content_filter(args.cache)
    data_export_csv(args.cache, args.csv_output)
    
    feishuSyncer = FeishuSyncer(args.feishu_app_id, args.feishu_app_secret)
    feishuSyncer.startSync(local_csv=args.csv_output, 
                            save_local_path=args.json_output)

    feishuCustomSyncer = CustomFeishuySyncer(args.feishu_app_id, args.feishu_app_secret, args.baidu_ak)
    feishuCustomSyncer.startExport(export_path=args.json_manual_output)

if __name__ == "__main__":
    args = parser.parse_args()

    backup_id = 0
    while True:
        print('start new round of fetching...')
        backup_if_exist(args.cache, backup_id)
        backup_id = (backup_id + 1) % (args.backup_count)
        fetch_n_export(args)