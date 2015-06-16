# -*-: encoding: utf-8 -*-
from common import *
import search_page_parser, ConfigParser, json, hashlib, shelve, socket
import os, sys, re, urllib2, copy, threading, Queue
import rmw_page_parser,news_page_parser
import traceback, json

class MetaSite:
    m_type = '新闻'
    m_url_pattern = '' 
    m_search_in_site = ''
    m_dup_method = 'LINK'
    m_encode_type = 'utf-8'
    m_page_start = 1
    m_page_incs = 1
    m_total_page_cnt = 20
    m_schedule_interval_sec = 600
    m_fetch_thread_num = 5
    m_stop_turn_page = True
    m_thread_lst = []
    def __init__(self, template_dict, dup, stastic_queue, result_queue):
        self.m_type = template_dict.get('TASK_TYPE')
        self.m_url_pattern = template_dict.get('REQUEST_URL')
        self.m_search_in_site = template_dict.get('SEARCH_IN_SITE')
        self.m_dup_method = template_dict.get('DUP_METHOD')
        self.m_encode_type = template_dict.get('ENCODE_TYPE')
        self.m_page_start = template_dict.get('PAGE_START')
        self.m_page_incs = template_dict.get('PAGE_INCREACE_CNT')
        self.m_total_page_cnt = template_dict.get('PAGE_TOTAL_CNT')
        self.m_stop_turn_page = template_dict.get('STOP_TURN_PAGE')
        self.m_schedule_interval_sec = template_dict.get('FETCH_INTERVAL_SEC')
        self.m_fetch_thread_num = template_dict.get('FETCH_THREAD_NUM')
        self.m_ruest_headers = template_dict.get('REQUEST_HEADERS')
        self.m_stastic_queue = stastic_queue
        post_data = template_dict.get('POST_DATA')
        if post_data is None:
            self.m_post_data = ''
        else:
            self.m_post_data = post_data
        timeout_sec = template_dict.get('TIMEOUT_SEC')
        if timeout_sec is None or timeout_sec == 0:
            self.m_timeout_sec = 40
        else:
            self.m_timeout_sec = timeout_sec
        self.m_dup = dup
        self.m_fetch_keyword_set  = set([])
        self.m_cancel_keyword_set = set([])
        self.m_lock = threading.Lock()
        self.m_result_queue = result_queue
        self.m_continious_empty_page_count = 0
    def get_host(self, url = ''):
        if url == '':
            host_url = self.m_url_pattern
        else:
            host_url = url
        protocol, other = urllib.splittype(host_url)
        host, path = urllib.splithost(other)
        return host
    def get_thread_lst(self):
        return self.m_thread_lst
    def encode_keyword(self, keyword):
        if self.m_search_in_site != '':
            keyword = '%s %s' % (self.m_search_in_site, keyword)
        if self.m_encode_type is not 'utf8':
            keyword = keyword.decode('utf8').encode(self.m_encode_type)
        keyword = my_urlencode(keyword)
        return keyword
    def decode_keyword(self, keyword):
        return urllib.unquote_plus(keyword)
    def set_keyword(self, keyword_lst):
        self.m_fetch_keyword_set.clear()
        cur_fetch_keyword_set  = set(keyword_lst)
        self.m_lock.acquire()
        self.m_cancel_keyword_set.clear()
        self.m_lock.release()
        for i in range(len(cur_fetch_keyword_set)):
            cur_fetch_keyword = self.encode_keyword(cur_fetch_keyword_set.pop())
            self.m_fetch_keyword_set.add(cur_fetch_keyword)
    ''''''
    def request_generator(self, context = ''):
        referer_url = ''
        post_data = ''
        for page_idx in range(0, self.m_total_page_cnt):
            self.m_lock.acquire()
            for cancel_keyword in self.m_cancel_keyword_set:
                if cancel_keyword in self.m_fetch_keyword_set:
                    self.m_fetch_keyword_set.remove(cancel_keyword)
                    log_info('keyword %s canceled' % cancel_keyword)
            self.m_cancel_keyword_set.clear()
            self.m_lock.release()
            if self.m_ruest_headers.get('Referer') is not None and referer_url != '':
                self.m_ruest_headers['Referer'] = referer_url
            for keyword in self.m_fetch_keyword_set:
                cur_page_cnt = self.m_page_start + page_idx * self.m_page_incs
                if self.m_post_data != 'None':
                    post_data = self.m_post_data.replace('{1}', str(cur_page_cnt))
                    post_data = post_data.replace('{0}', keyword)
                cur_request_url = self.m_url_pattern.replace('{0}', keyword).replace('{1}', str(cur_page_cnt))
                #log_info('make url sucess: %s/%s' % (cur_request_url,post_data))
                if post_data == '':
                    req = urllib2.Request(cur_request_url, headers=self.m_ruest_headers)
                else:
                    req = urllib2.Request(cur_request_url, data=post_data,headers=self.m_ruest_headers)
                req.m_keyword = keyword
                req.m_timeout_sec = self.m_timeout_sec
                referer_url = cur_request_url
                yield req
    ''''''
    def get_schedule_interval(self):
        return self.m_schedule_interval_sec
    ''''''
    def get_parser(self):
        return search_page_parser.SearchPageParser()
    ''''''
    def get_site_type(self):
        return self.m_type
    '''解析结果, 抽链接： 页面的任意一个链接变化了，就返回True， 表示接着下载；注意插滤重器'''
    def format_result(self, link, tsak_type, date='', title = '',maxDepth = -1):
        result_dic = {}
        result_dic["url"] = '%s' % link
        result_dic["pubdate"] = '%s' % date
        result_dic["title"] = '%s' % title
        result_dic["siteName"] = ''
        result_dic["channelName"] = ''
        result_dic["siteType"] = '%s' % tsak_type
        result_dic["maxDepth"] = '%d' % maxDepth
        result_dic["gfwed"] =  '%d' % 1
        #json_result = json.dumps(result_dic)
        return result_dic
    def handle_result(self, result_json_lst, request):
        keyword = request.m_keyword
        #parse json
        json_result_lst = []
        dup_result_cnt = 0
        new_result_cnt = 0
        ret = True
        dup_item_dict = {} 
        for index, json_str in enumerate(result_json_lst):
            cur_result_dict = json.loads(json_str)
            result_link = cur_result_dict.get('link')
            result_title = my_strip(cur_result_dict.get('title'))
            if result_link.find('&query=')>0 or result_link.find('/ns?word=')>0 or result_link.find('news.haosou.com/ns?j=0')>0:
                continue
            result_host = self.get_host(result_link)
            if result_host == 'www.xici.net' or result_host.find('dedeadmin.com') >= 0 or result_host == "news.baidu.com":
                continue
            same_news_link = cur_result_dict.get('same_news_link')
            if self.m_dup_method == 'TITLE':
                result_summary = my_strip(cur_result_dict.get('summary'))
                dup_key = '%s%s' % (result_title, result_summary[0:20])
            else:
                dup_key = result_link
            dup_item_dict[dup_key] = cur_result_dict
        new_dup_key_lst = self.m_dup.obtain_new_lst(dup_item_dict.keys())
        new_result_cnt  = len(new_dup_key_lst)
        dup_result_cnt  = len(result_json_lst) - new_result_cnt
        for dup_key in new_dup_key_lst:
            result_link = cur_result_dict.get('link')
            cur_result_dict = dup_item_dict[dup_key]
            result_date = cur_result_dict.get('date')
            if result_date is None:
                result_date=''
            json_result_lst.append(self.format_result(result_link, self.m_type, title = result_title, date = result_date))
            '''加入新闻相同链接'''
            #same_news_link = cur_result_dict.get('same_news_link')
            #if same_news_link is not None:
            #    json_result_lst.append(self.format_result(same_news_link, self.m_type, maxDepth = 0))
        if new_result_cnt + dup_result_cnt == 0:
            self.m_continious_empty_page_count += 1
        else:
            self.m_continious_empty_page_count = 0
        '''统计信息'''
        while True:
            try:
                self.m_stastic_queue.put((self.get_host(), self.decode_keyword(keyword), new_result_cnt, dup_result_cnt))
                log_info('add stat %s %s new:%d dup:%d' % (self.get_host(), self.decode_keyword(keyword), new_result_cnt, dup_result_cnt))
                break
            except Exception, err:
               log_error("put stastic error: %s, %s" % (err, traceback.format_exc()))
               time.sleep(1)
        '''cancel条件： ①这一页有结果，但是没有新的链接; ②连续的空白页'''
        if (new_result_cnt == 0 and dup_result_cnt > 0 and self.m_stop_turn_page) or self.m_continious_empty_page_count >= 5:
            self.m_lock.acquire()
            self.m_cancel_keyword_set.add(keyword)
            self.m_lock.release()
            return
        '''发送结果'''
        json_result_dict = {}
        log_info('json_result_lst len : %d' % len(json_result_lst))
        if len(json_result_lst) >0:
            json_result_dict["tasks"] = json_result_lst
            post_data = json.dumps(json_result_dict)
            while True:
                if self.m_result_queue.qsize()>10000:
                    time.sleep(2)
                    continue
                self.m_result_queue.put(post_data)
                log_info('put result success: 1/%d.' % self.m_result_queue.qsize())
                break

class NewsMetaSite(MetaSite):
    def __init__(self, template_dict, dup, stastic_queue, result_queue):
        MetaSite.__init__(self, template_dict, dup, stastic_queue, result_queue)
    def get_parser(self):
        return news_page_parser.NewsPageParser()

class BbsMetaSite(MetaSite):
    def __init__(self, template_dict, dup, stastic_queue, result_queue):
        MetaSite.__init__(self, template_dict, dup, stastic_queue, result_queue)

class RmwMetaSite(MetaSite):
    def __init__(self, template_dict, dup, stastic_queue, result_queue):
        MetaSite.__init__(self, template_dict, dup, stastic_queue, result_queue)
    def get_parser(self):
        return rmw_page_parser.RmwPageParser()

class BlogMetaSite(MetaSite):
    def __init__(self, template_dict, dup, stastic_queue, result_queue):
        MetaSite.__init__(self, template_dict, dup, stastic_queue, result_queue)
        
class WebchatMetaSite(MetaSite):
    def __init__(self, template_dict, dup, stastic_queue, result_queue):
        MetaSite.__init__(self, template_dict, dup, stastic_queue, result_queue)

class DupDetector:
    def __init__(self, dup_service_url):
        self.m_dup_service_url = dup_service_url
    def obtain_new_lst(self, key_lst):
        item_lst = []
        for key in key_lst:
            item = {}
            item['lk'] = key
            item['op'] = 'i'
            item_lst.append(item)
        if len(key_lst) == 0:
            return [] 
        post_data = json.dumps(item_lst)
        res_lst   = []
        while True:
            try:
                res_lst = json.loads(urllib2.urlopen(self.m_dup_service_url, data = post_data).read())
                break
            except Exception, err:
                log_error('request DupDetector %s error: %s' % (self.m_dup_service_url, err));
                time.sleep(1)
        if res_lst is None or len(res_lst) == 0:
            log_error('recv empty item list from dup service')
            return []
        new_key_lst = []
        for res in res_lst:
            if res.get('lk') is None or res.get('st') is None:
                log_error('invalid dup return result: %s' % res)
                continue
            if res['st'] == '0':
                new_key_lst.append(res['lk']) 
        return new_key_lst;

class MetaSiteFactory:
    def __init__(self, dup_service_url, stastic_queue, result_queue):
        self.m_dup = DupDetector(dup_service_url)
        self.m_host_set = set()
        self.m_stastic_queue = stastic_queue
        self.m_result_queue  = result_queue 
    def __read_config(self, template_file):
        cf = ConfigParser.ConfigParser()
        cf.read(template_file)
        template_dict = {}
        if cf.has_option('TEMPLATE', 'TASK_TYPE'):
            template_dict['TASK_TYPE'] = cf.get('TEMPLATE', 'TASK_TYPE')
        else:
            template_dict['TASK_TYPE'] = '新闻'
        if cf.has_option('TEMPLATE', 'REQUEST_URL'):
            template_dict['REQUEST_URL'] = cf.get('TEMPLATE', 'REQUEST_URL')
        intra_site_name = ''
        if cf.has_option('TEMPLATE', 'SEARCH_IN_SITE'):
            intra_site_name = cf.get('TEMPLATE', 'SEARCH_IN_SITE')
            if intra_site_name is None or intra_site_name == 'None':
                intra_site_name = ''
        template_dict['SEARCH_IN_SITE'] = intra_site_name
        if cf.has_option('TEMPLATE', 'ENCODE_TYPE'):
            template_dict['ENCODE_TYPE'] = cf.get('TEMPLATE', 'ENCODE_TYPE')
        else:
            template_dict['ENCODE_TYPE'] = 'utf-8'
        if cf.has_option('TEMPLATE', 'DUP_METHOD'):
            template_dict['DUP_METHOD'] = cf.get('TEMPLATE', 'DUP_METHOD')
        else:
            template_dict['DUP_METHOD'] = 'LINK'
        if cf.has_option('TEMPLATE', 'PAGE_START'):
            template_dict['PAGE_START'] = cf.getint('TEMPLATE', 'PAGE_START')
        else:
            template_dict['PAGE_START'] = 1
        if cf.has_option('TEMPLATE', 'PAGE_INCREACE_CNT'):
            template_dict['PAGE_INCREACE_CNT'] = cf.getint('TEMPLATE', 'PAGE_INCREACE_CNT')
        else:
            template_dict['PAGE_INCREACE_CNT'] = 1
        if cf.has_option('TEMPLATE', 'PAGE_TOTAL_CNT'):
            template_dict['PAGE_TOTAL_CNT'] = cf.getint('TEMPLATE', 'PAGE_TOTAL_CNT')
        else:
            template_dict['PAGE_TOTAL_CNT'] = 50
        if cf.has_option('TEMPLATE', 'STOP_TURN_PAGE'):
            template_dict['STOP_TURN_PAGE'] = cf.getboolean('TEMPLATE', 'STOP_TURN_PAGE')
        else:
            template_dict['STOP_TURN_PAGE'] = True
        if cf.has_option('TEMPLATE', 'TIMEOUT_SEC'):
            template_dict['TIMEOUT_SEC'] = cf.getint('TEMPLATE', 'TIMEOUT_SEC')
        else:
            template_dict['TIMEOUT_SEC'] = 60
        if cf.has_option('TEMPLATE', 'FETCH_INTERVAL_SEC'):
            template_dict['FETCH_INTERVAL_SEC'] = cf.getint('TEMPLATE', 'FETCH_INTERVAL_SEC')
        else:
            template_dict['FETCH_INTERVAL_SEC'] = 600
        if cf.has_option('TEMPLATE', 'FETCH_THREAD_NUM'):
            template_dict['FETCH_THREAD_NUM'] = cf.getint('TEMPLATE', 'FETCH_THREAD_NUM')
        else:
            template_dict['FETCH_THREAD_NUM'] = 5
        if cf.has_option('TEMPLATE', 'POST_DATA'):
            template_dict['POST_DATA'] = cf.get('TEMPLATE', 'POST_DATA')
        else:
            template_dict['POST_DATA'] = None
        if cf.has_option('TEMPLATE', 'REQUEST_HEADERS'):
            header = cf.get('TEMPLATE', 'REQUEST_HEADERS')
            template_dict['REQUEST_HEADERS'] = eval(header)
        else:
            template_dict['REQUEST_HEADERS'] = { \
                                                'User-Agent':"Mozilla/5.0 (Windows NT 5.1; rv:32.0) Gecko/20100101 Firefox/32.0", \
                                                'Accept':"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",\
                                                'Accept-Encoding':"gzip, deflate"\
                                                }
        return template_dict
    def __get_host(self, url):
        protocol, other = urllib.splittype(url)
        host, path = urllib.splithost(other)
        return host
    '''返回metasite'''
    def create_site(self, template_file):
        log_info('MetaSiteFactory create_site start')
        template_dict = self.__read_config(template_file)
        request_url = template_dict.get('REQUEST_URL')
        if request_url is None or len(request_url)<10:
            log_error('the template %s has no item named REQUEST_URL' % template_file)
            return None
        task_type = template_dict.get('TASK_TYPE')
        if task_type is None:
            task_type = '新闻'
            #return None
        meta_site = None
        '''TODO: add site modify here'''
        host_name = self.__get_host(template_dict.get('REQUEST_URL'))
        if host_name in self.m_host_set:
            log_error('dumplicate site task: %s' % host_name)
        else:
            self.m_host_set.add(host_name)
        if task_type == "新闻":
            if host_name == 'search.people.com.cn':
                meta_site = RmwMetaSite(template_dict, self.m_dup, self.m_stastic_queue, self.m_result_queue)
            else:
                meta_site = NewsMetaSite(template_dict, self.m_dup, self.m_stastic_queue, self.m_result_queue)
        elif task_type == '博客':
            meta_site = BlogMetaSite(template_dict, self.m_dup, self.m_stastic_queue, self.m_result_queue)
        elif task_type == '论坛':
            meta_site = BbsMetaSite(template_dict, self.m_dup, self.m_stastic_queue, self.m_result_queue)
        elif task_type == '微信':
            meta_site = WebchatMetaSite(template_dict, self.m_dup, self.m_stastic_queue, self.m_result_queue)
        else:
            log_error('the template %s has invalid TASKTYPE: %s' % (template_file, task_type))
            return None
        log_info('load template %s success.' % template_file)
        return meta_site
