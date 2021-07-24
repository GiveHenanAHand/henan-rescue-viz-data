from dataProcessUtils import *
from baiduApiWrapper import *
from weiboDataFetcher import *
import argparse
import shutil

parser = argparse.ArgumentParser(description='Scraper for henan flood-related weibo')
parser.add_argument('--cache', type=str, default=None, help='cache numpy array')
parser.add_argument('--output', type=str, default=None, help='output json file')
parser.add_argument('--api_key', type=str, default=None, help='baidu API key')
parser.add_argument('--api_secret', type=str, default=None, help='baidu API secret')

def backup_if_exist(path):
    now = datetime.datetime.now()
    if os.path.exists(path+".npy"):
        shutil.copy(path+".npy", path + "." + now.strftime('%Y%m%d%H%M%S') + ".old")

def fetch_n_export(args):
    Weibo_Fetcher = WeiboDataFetcher()
    Api_Wrapper = BaiduAPIWrapper(args.api_key, args.api_secret)

    for i in tqdm(range(1)):
        Weibo_Fetcher.fetch_weibo_data(args.cache, "河南暴雨互助", page=i, stop_if_repeat=False)
        Api_Wrapper.extract_addresses_from_data(args.cache)

    data_date_valid(args.cache)
    data_link_valid(args.cache)
    data_content_filter(args.cache)
    data_export(args.cache)
    data_export_csv(args.cache)

if __name__ == "__main__":
    args = parser.parse_args()

    while True:
        backup_if_exist(args.cache)
        backup_if_exist(args.output)
        fetch_n_export(args)
        break
