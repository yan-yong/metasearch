# -*- encoding: utf-8 -*-
import re, time, datetime, sys

class SourceExtractor:
    __site_keyword_lst = ['网', '频道', '新闻', '博客', '论坛', '通讯社', '在线', '微博', '资讯', '报', '视窗', '站', '百科', '电视台']
    __big_name_lst = ['网易', '新浪', '搜狐', '腾讯', '凤凰', '百度', '知乎', '优酷', '土豆', '贴吧', '搜狗', '和讯', '中新','人民网']
    __type_lst = ['科技', '教育', '军事', '育儿', '房产', '金融', '财经', 'IT']
    __seperate_flag = [',', ' ', '\t', '\r', '\n', '_', '-', '——', ';', '；', ',', '，', ':']
    __deny_keyword_lst = ['快照', '站内搜索', '在线播放', '相同新闻', '最新相关资讯', '举报']
    __max_cmp_len = 30
    def __find_lst(self, text, keyword_lst):
        beg_pos = len(text)
        for keyword in keyword_lst:
            cur_pos = text.find(keyword)
            if cur_pos < 0:
                continue
            if cur_pos < beg_pos:
                beg_pos = cur_pos
        if beg_pos == len(text): 
            return -1
        return beg_pos
    def __head_seperate(self, text, beg_pos):
        if beg_pos == 0:
            return True
        if beg_pos < 0 or beg_pos > len(text):
            return False
        for flag in self.__seperate_flag:
            if beg_pos >= len(flag) and text[beg_pos - len(flag):beg_pos] == flag:
                return True
        return False
    def __after_seperate(self, text, end_pos):
        if end_pos == len(text):
            return True
        if end_pos < 0 or end_pos > len(text):
            return False
        for flag in self.__seperate_flag:
            if end_pos + len(flag) <= len(text) and text[end_pos:end_pos+len(flag)] == flag:
                return True
        return False
    def __extract(self, text):
        accuracy = 0
        cmp_pos = -1
        if self.__find_lst(text, self.__deny_keyword_lst) >= 0:
            return (0, (0, 0))
        cur_pos = self.__find_lst(text, self.__site_keyword_lst)
        if cur_pos >= 0:
            cmp_pos = cur_pos
            accuracy += 10
        cur_pos = self.__find_lst(text, self.__big_name_lst)
        if cur_pos >= 0:
            cmp_pos = cur_pos
            accuracy += 20
        cur_pos = self.__find_lst(text, self.__type_lst)
        if cur_pos >= 0:
            cmp_pos = cur_pos
            accuracy += 10
        beg_pos = cmp_pos
        str_len = 0
        while beg_pos >= 0 and not self.__head_seperate(text, beg_pos) and str_len < self.__max_cmp_len:
            beg_pos -= 1
            str_len += 1
        #beg_pos += 1
        #str_len -= 1
        end_pos = cmp_pos
        while end_pos < len(text) and not self.__after_seperate(text, end_pos) and str_len < self.__max_cmp_len:
            end_pos += 1
            str_len += 1
        return accuracy, (beg_pos, end_pos)
    def extract(self, text):
        #text = text_val.strip(' ').strip('\t').strip('\n').strip('\r').strip(' ').strip('\t')
        accuracy = 0
        pos = (0, 0)
        source  = None
        if len(text) <= self.__max_cmp_len:
            accuracy, pos = self.__extract(text)
        else:
            prefix_text = text[0:self.__max_cmp_len]
            prefix_accuracy, prefix_tuple = self.__extract(prefix_text)
            '''防止被截断'''
            if prefix_accuracy > 0 and prefix_tuple[1] == self.__max_cmp_len:
                prefix_accuracy = 0
            after_text  = text[-self.__max_cmp_len:]
            after_accuracy, after_tuple = self.__extract(after_text)
            if after_accuracy > 0 and after_tuple[0] == 0:
                after_accuracy = 0
            if prefix_accuracy > after_accuracy or (prefix_accuracy == after_accuracy \
                and prefix_tuple[1] - prefix_tuple[0] > after_tuple[1] - after_tuple[0]):
                accuracy = prefix_accuracy
                pos = prefix_tuple
            else:
                accuracy = after_accuracy
                after_beg_pos = len(text) - self.__max_cmp_len
                pos = (after_tuple[0]+after_beg_pos, after_tuple[1]+after_beg_pos)
        if accuracy > 0:
            source = text[pos[0]:pos[1]]
        return accuracy, source, pos
    def extract_lst(self, text_lst):
        accuracy = 0
        source = ''
        node = None
        pos_span = 0
        for item in text_lst:
            cur_node = item[0]
            cur_text = item[1]
            cur_accuracy, cur_source, pos_span = self.extract(cur_text)
            if cur_accuracy > accuracy:
                accuracy = cur_accuracy
                source = cur_source
                node = cur_node
        return accuracy, source, node, pos_span
        
if __name__ == "__main__":
    source_extractor = SourceExtractor()
    print source_extractor.extract('同花顺网  2014-12-19 19:00')[1]
    print source_extractor.extract('同花顺网  2014-12-19 19:00')[1]
    print source_extractor.extract('凤凰网  11月27日')[1]

        
