from dataProcessUtils import *
from baiduApiWrapper import *
from weiboDataFetcher import *

Weibo_Fetcher = WeiboDataFetcher()
Api_Wrapper = BaiduAPIWrapper()

def fetch_n_export():
    Weibo_Fetcher.fetch_weibo_data("河南暴雨互助", page=10, stop_if_repeat=False)
    Api_Wrapper.extract_addresses_from_data()
    data_export()
    data_export_csv()

if __name__ == "__main__":
    data_export_csv()
    # for i in range(100):
    #     fetch_n_export()
    #     time.sleep(10)
