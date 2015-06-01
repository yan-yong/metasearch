# -*-: encoding: utf-8 -*-
from common import *
import search_page_parser, ConfigParser, json, hashlib, shelve, socket
import os, sys, re, urllib2, copy, threading, Queue
import rmw_page_parser

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
    def __init__(self, template_dict, dup, result_action, stastic_queue):
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
        self.m_result_action = result_action
        self.m_fetch_keyword_set  = set([])
        self.m_cancel_keyword_set = set([])
        self.m_lock = threading.Lock()
        self.m_result_queue = Queue.Queue(10000)
        self.m_send_result_tread = threading.Thread(target=self.send_result, args=())
        self.m_send_result_tread.start()
        self.m_thread_lst.append(self.m_send_result_tread)
        self.m_continious_empty_page_count = 0
    def get_host(self):
        protocol, other = urllib.splittype(self.m_url_pattern)
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
    def format_result(self, link, tsak_type):
        result_dic = {}
        result_dic["url"] = '%s' % link
        result_dic["siteName"] = ''
        result_dic["channelName"] = ''
        result_dic["siteType"] = '%s' % tsak_type
        result_dic["maxDepth"] = '%d' % 2
        result_dic["gfwed"] =  '%d' % 1
        #json_result = json.dumps(result_dic)
        return result_dic
    def handle_result(self, result_json_lst, request):
        keyword = request.m_keyword
        #parse json
        json_result_lst = []
        #keyword_need_cancel = False
        dup_result_cnt = 0
        new_result_cnt = 0
        for index, json_str in enumerate(result_json_lst):
            cur_result_dict = json.loads(json_str)
            #获取结果
            result_link = cur_result_dict.get('link')
            if self.m_dup_method == 'TITLE':
                result_title = my_strip(cur_result_dict.get('title'))
                result_summary = my_strip(cur_result_dict.get('summary'))
                dup_key = '%s%s' % (result_title, result_summary[0:20])
            else:
                dup_key = result_link
            if not self.m_dup.check(dup_key):
                #keyword_need_cancel = False
                new_result_cnt += 1
                self.m_dup.insert(dup_key)
                json_result_lst.append(self.format_result(result_link, self.m_type))
            else:
                dup_result_cnt += 1
                log_info('skip the dump one:%s'% result_link)
        if new_result_cnt + dup_result_cnt == 0:
            self.m_continious_empty_page_count += 1
        else:
            self.m_continious_empty_page_count = 0
        '''cancel条件： ①这一页有结果，但是没有新的链接; ②连续的空白页'''
        if (new_result_cnt == 0 and dup_result_cnt > 0 and self.m_stop_turn_page) or self.m_continious_empty_page_count >= 5:
            #keyword_need_cancel = True
            self.m_lock.acquire()
            self.m_cancel_keyword_set.add(keyword)
            self.m_lock.release()
            return
        '''统计信息'''
        self.m_stastic_queue.put((self.get_host(), self.decode_keyword(keyword), new_result_cnt, dup_result_cnt))
        '''发送结果'''
        json_result_dict = {}
        json_result_dict["tasks"] = json_result_lst
        post_data = json.dumps(json_result_dict)
        while True:
            if self.m_result_queue.qsize()>10000:
                time.sleep(2)
                continue
            self.m_result_queue.put(post_data)
            log_info('put result success: 1/%d.' % self.m_result_queue.qsize())
            break
    def send_result(self):
        #log_info('$$$$$$thread send_result start$$$$$$')
        while True:
            post_data = None
            if self.m_result_queue.qsize()==0:
                time.sleep(2)
                continue
            try:
                post_data = self.m_result_queue.get()
                urllib2.urlopen(self.m_result_action, data = post_data )
                log_info('send result action success: 1/%d. %s' % (self.m_result_queue.qsize(),self.m_result_action))
            except Exception, err:
                log_error('send result action %s error: %s' % (self.m_result_action, err))
                if not post_data:
                    self.m_result_queue.put(post_data)
                time.sleep(1)

class NewsMetaSite(MetaSite):
    def __init__(self, template_dict, dup, result_action, stastic_queue):
        MetaSite.__init__(self, template_dict, dup, result_action, stastic_queue)
class BbsMetaSite(MetaSite):
    def __init__(self, template_dict, dup, result_action, stastic_queue):
        MetaSite.__init__(self, template_dict, dup, result_action, stastic_queue)
class RmwMetaSite(MetaSite):
    def __init__(self, template_dict, dup, result_action, stastic_queue):
        MetaSite.__init__(self, template_dict, dup, result_action, stastic_queue)
    def get_parser(self):
        return rmw_page_parser.RmwPageParser()

class BlogMetaSite(MetaSite):
    def __init__(self, template_dict, dup, result_action, stastic_queue):
        MetaSite.__init__(self, template_dict, dup, result_action, stastic_queue)
        
class WebchatMetaSite(MetaSite):
    def __init__(self, template_dict, dup, result_action, stastic_queue):
        MetaSite.__init__(self, template_dict, dup, result_action, stastic_queue)

class DupDetector:
    def __init__(self, dup_file_name, sync_interval_sec = 10):
        self.m_last_sync_time = time.time()
        self.m_sync_interval_sec = sync_interval_sec
        self.m_lock = threading.Lock()
        try:
            self.m_storage = shelve.open(dup_file_name)
        except Exception, err:
            log_error('DupDetector open file %s error: %s' % (dup_file_name, err))
            sys.exit(1)
    def sync(self):
        if self.m_last_sync_time + self.m_sync_interval_sec < time.time():
            self.m_storage.sync()
            self.m_last_sync_time = time.time()
    def check(self, url):
        self.sync()
        key = hashlib.md5(url).hexdigest()
        self.m_lock.acquire()
        if self.m_storage.get(key) is not None:
            self.m_lock.release()
            return True
        self.m_lock.release()
        return False
    def insert(self, url):
        log_info('insert into filter: %s'% url)
        key = hashlib.md5(url).hexdigest()
        self.m_lock.acquire()
        self.m_storage[key] = 1
        self.m_lock.release()
        self.sync()
    def pop(self, url):
        key = hashlib.md5(url).hexdigest()
        self.m_lock.acquire()
        self.m_storage.pop(key)
        self.m_lock.release()
    def close(self):
        self.m_lock.acquire()
        self.m_storage.sync()
        self.m_lock.release()

class MetaSiteFactory:
    def __init__(self, dup_file_dir, result_action, stastic_queue):
        self.m_dup_file_dir = dup_file_dir
        self.m_dup_file_map = {}
        self.m_result_action = result_action
        self.m_stastic_queue = stastic_queue
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
    def close(self):
        for host, dup in self.m_dup_file_map.items():
            dup.close()
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
        if self.m_dup_file_map.get(host_name):
            log_info('dumplicate fetch host %s' % host_name)
            return meta_site
        site_dup_detector  = DupDetector('%s/%s.dat' % (self.m_dup_file_dir.rstrip('/'), host_name))
        if task_type == "新闻":
            if host_name == 'search.people.com.cn':
                meta_site = RmwMetaSite(template_dict, site_dup_detector, self.m_result_action, self.m_stastic_queue)
            else:
                meta_site = NewsMetaSite(template_dict, site_dup_detector, self.m_result_action, self.m_stastic_queue)
        elif task_type == '博客':
            meta_site = BlogMetaSite(template_dict, site_dup_detector, self.m_result_action, self.m_stastic_queue)
        elif task_type == '论坛':
            meta_site = BbsMetaSite(template_dict, site_dup_detector, self.m_result_action, self.m_stastic_queue)
        elif task_type == '微信':
            meta_site = WebchatMetaSite(template_dict, site_dup_detector, self.m_result_action, self.m_stastic_queue)
        else:
            log_error('the template %s has invalid TASKTYPE: %s' % (template_file, task_type))
            return None
        log_info('load template %s success.' % template_file)
        return meta_site