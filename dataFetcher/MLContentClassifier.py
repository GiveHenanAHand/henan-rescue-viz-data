import jieba
import numpy as np

VALID_CLASSIFIER_DICT_PATH = 'dict.txt'
CATEGORY_NEEDHELP_DICT_PATH = 'need_help_dict.txt'
CATEGORY_OFFERHELP_DICT_PATH = 'offer_help_dict.txt'
CATEGORY_OTHER_DICT_PATH = 'other_dict.txt'

class MLContentClassifier(object):
    def __init__(self, dict_folder_path=''):
        self.ml_dict_folder_prefix = ''
        if dict_folder_path != '':
            self.ml_dict_folder_prefix = dict_folder_path+'/'

    def load_dict(self, fname):
        return_dict = {}
        with open(self.ml_dict_folder_prefix+fname, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            for line in lines:
                name = line.strip().split()[0]
                number = float(line.strip().split()[1])
                return_dict[name] = number
        return return_dict

    def get_word_list(self, text):
        word_list = []
        seg_list = jieba.cut(text, cut_all=False, HMM=True)
        split_line = " ".join(seg_list).split()
        for words in split_line:
            for word in words:
                if u'\u4e00' <= word <= u'\u9fff':
                    word_list.append(words)
                    break
        return word_list

    # A ML model to classify & categorize the data
    def data_content_filter(self, cache_path, 
                            valid_dict_path=VALID_CLASSIFIER_DICT_PATH,
                            needhelp_dict_path=CATEGORY_NEEDHELP_DICT_PATH,
                            offerhelp_dict_path=CATEGORY_OFFERHELP_DICT_PATH,
                            other_dict_path=CATEGORY_OTHER_DICT_PATH):
        self.data = np.load(cache_path + ".npy", allow_pickle=True)[()]
        self.dict_valid = self.load_dict(valid_dict_path)
        self.dict_need_help = self.load_dict(needhelp_dict_path)
        self.dict_offer_help = self.load_dict(offerhelp_dict_path)
        self.dict_other = self.load_dict(other_dict_path)

        IDs = self.data.keys()
        for data_id in IDs:
            valid = self.classify_validity(data_id)
            if valid == 'valid':
                cateogry = self.classify_category(data_id)

        np.save(cache_path, self.data)

    # classify the validity of an item
    def classify_validity(self, dataid):
        # urgent means validity...
        if 'urgent' in self.data[dataid]:
            return self.data[dataid]['urgent']
        
        self.data[dataid]['urgent'] = True
        word_list = self.get_word_list(self.data[dataid]['post'])

        result = 1
        for words in word_list:
            if self.dict_valid.get(words) != None:
                result = result * self.dict_valid.get(words)

        if result < 1:
            self.data[dataid]['urgent'] = False
            return 'not_valid'
        return 'valid'

    # classify the category of an item
    def classify_category(self, dataid):
        def get_category_score(cat_dict, words, current_score, default_score):
            if cat_dict.get(words) != None:
                return current_score*cat_dict.get(words)
            return current_score*default_score

        if 'category' in self.data[dataid]:
            return self.data[dataid]['category']

        word_list = self.get_word_list(self.data[dataid]['post'])
        need_help_score, offer_help_score, other_score = 1.0, 1.0, 1.0
        for words in word_list:
            need_help_score = get_category_score(
                self.dict_need_help, words, need_help_score, 0.07)
            offer_help_score = get_category_score(
                self.dict_offer_help, words, offer_help_score, 0.2)
            other_score = get_category_score(
                self.dict_other, words, other_score, 0.2)

        if need_help_score > offer_help_score:
            if need_help_score > other_score:
                result = "求救"
            else:
                result = "其他"
        else:
            if offer_help_score > other_score:
                result = "帮助"
            else:
                result = "其他"
        self.data[dataid]['category'] = result
        return result