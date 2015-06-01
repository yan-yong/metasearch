# -*- encoding:utf-8 -*-
from common import *
from meta_site import *
import proxy_spider
import ConfigParser, threading
import multiprocessing, stat_server

class MetaSearch:
    def __init__(self):
        self.m_proxy_server_url = 'http://59.108.122.184:9090/'
        self.m_site_dir = 'site'
        self.m_keyword_file = 'keyword.dat'
        self.m_dup_dir = 'dup_dir'
        self.m_schedule_interval_sec = 3600
        self.m_result_action = 'http://127.0.0.1:10000/'
        self.m_site_task_lst = []
        self.m_keyword_lst = []
        self.m_meta_task_factory = None
        self.m_thd_lst = []
        self.m_exit = False
        self.m_each_fetch_count = 50
        self.m_work_process_num = 1
        self.m_stastic_queue = multiprocessing.Queue()
        self.m_stat_server = stat_server.StatHttpServer()
    '''抓一轮'''
    def sync_fetch_result_generator(self, proxy_fetcher, req_lst, timeout_sec, should_exit):
        for res_html, res_header, fetch_req, fetch_proxy in proxy_fetcher.sync_generator( req_lst, False, timeout_sec):
            if should_exit or res_html is None:
                log_error('fetch_one_round stop, should_exit: %s, res_html:%s' % (should_exit, type(res_html)))
                return
            '''res_header是否gzip压缩'''
            content_type = res_header.get('Content-Encoding')
            if content_type is not None and content_type.find('gzip') >= 0:
                try:
                    res_html = gzip_decompress(res_html)
                except Exception, err:
                    log_error('gzip decompress error: %s' % fetch_req.get_full_url())
                    continue
            '''if len(res_html) < 1000:
                log_error('the web page content is too short: %d %s' % (len(res_html), fetch_req.get_full_url()))
                continue'''
            req_url = fetch_req.get_full_url()
            if fetch_req.data is not None:
                req_url += '&%s' % str(fetch_req.data)
            yield fetch_req, req_url, res_html, res_header
    def fetch_one_round(self, proxy_fetcher, task_site, should_exit):
        #log_info('fetch_one_round start')
        task_site.set_keyword(self.m_keyword_lst)
        req_lst = []
        fetch_parser = task_site.get_parser()
        if should_exit:
            return
        for req in task_site.request_generator(self.m_keyword_lst):
            if len(req_lst) < self.m_each_fetch_count:
                req_lst.append(req)
                continue
            if should_exit:
                return
            timeout_sec = req.m_timeout_sec
            for fetch_req, req_url, res_html, res_header in self.sync_fetch_result_generator(proxy_fetcher, req_lst, timeout_sec, should_exit):
                json_result_lst = fetch_parser.get_parse_result(req_url, res_html, res_header)
                if json_result_lst is None or len(json_result_lst) == 0:
                    continue
                task_site.handle_result(json_result_lst, fetch_req)
            del req_lst[:]
        '''处理剩余结果'''
        timeout_sec = req.m_timeout_sec
        for fetch_req, req_url, res_html, res_header in self.sync_fetch_result_generator(proxy_fetcher, req_lst, timeout_sec, should_exit):
            json_result_lst = fetch_parser.get_parse_result(fetch_req.get_full_url(), res_html, res_header)
            if json_result_lst is None or len(json_result_lst) == 0:
                continue
            task_site.handle_result(json_result_lst, fetch_req)
    def load_config(self, config_file):
        try:
            cf = ConfigParser.ConfigParser()
            cf.read(config_file)
            if cf.has_option('PROXY', 'proxy_server'):
                self.m_proxy_server_url = cf.get('PROXY', 'proxy_server')
            if cf.has_option('PROXY', 'each_fetch_count'):
                self.m_each_fetch_count = cf.getint('PROXY', 'each_fetch_count')
            if self.m_each_fetch_count <= 0:
                log_error('invalid each fetch count: %d' % self.m_each_fetch_count)
                sys.exit(1)
            if cf.has_option('METASEARCH', 'site_dir'):
                self.m_site_dir = cf.get('METASEARCH', 'site_dir')
            if cf.has_option('METASEARCH', 'keyword_file'):
                self.m_keyword_file = cf.get('METASEARCH', 'keyword_file')
            if cf.has_option('METASEARCH', 'dup_detector_dir'):
                self.m_dup_dir = cf.get('METASEARCH', 'dup_detector_dir')
            if cf.has_option('METASEARCH', 'schedule_interval_sec'):
                self.m_schedule_interval_sec = cf.getint('METASEARCH', 'schedule_interval_sec')
            if cf.has_option('METASEARCH', 'worker_process_num'):
                self.m_work_process_num = cf.getint('METASEARCH', 'worker_process_num')
            if cf.has_option('AUTOFETCHER', 'result_action'):
                self.m_result_action = cf.get('AUTOFETCHER', 'result_action')
            if cf.has_option('STATSERVER', 'stat_http_server_ip'):
                self.m_stat_http_server_ip = cf.get('STATSERVER', 'stat_http_server_ip')
            if cf.has_option('STATSERVER', 'stat_http_server_port'):
                self.m_stat_http_server_port = cf.getint('STATSERVER', 'stat_http_server_port')
        except Exception, err:
            log_error('MetaSearch::load_config %s exception: %s' % (config_file, err))
            sys.exit(1)
    def worker_process(self, site_file_lst):
        self.m_meta_task_factory = MetaSiteFactory(self.m_dup_dir, self.m_result_action, self.m_stastic_queue)
        for site_file in site_file_lst:
            site_task = self.m_meta_task_factory.create_site(site_file)
            if site_task is None:
                log_error('skip dumplicate site: %s' % site_file)
                continue
            self.m_site_task_lst.append(site_task)
        for index, site_task in enumerate(self.m_site_task_lst):
            thd = threading.Thread(target=self.fetch_runtine, args=(site_task, index+1,))
            self.m_thd_lst.append(thd)
        for thd in self.m_thd_lst:
            thd.start()
        log_info('spawn %d task thread success.' % len(self.m_thd_lst))
        for thd in self.m_thd_lst:
            thd.join()
    def http_stat_thread(self):
        self.m_stat_server.start((self.m_stat_http_server_ip, self.m_stat_http_server_port))        
    '''runtine start'''  
    def start(self):
        '''load keywords'''
        keyword_fid = open(self.m_keyword_file, 'r')
        if keyword_fid is None:
            log_error('keyword file %s is not exited.' % self.m_keyword_file)
            sys.exit(1)
        for line in keyword_fid.readlines():
            line = my_strip(line)
            if line is None or len(line) == 0:
                continue 
            self.m_keyword_lst.append(line)
        if len(self.m_keyword_lst) == 0:
            log_error('found 0 keyword in %s' % self.m_keyword_file)
            sys.exit(1)
        '''load site file'''
        site_file_lst = get_file_lst(".dat", self.m_site_dir, False)
        if not os.path.exists(self.m_site_dir):
            os.makedirs(self.m_site_dir)
        if len(site_file_lst) == 0:
            log_error('find no site file in %s' % self.m_site_dir)
            sys.exit(1)
        if self.m_work_process_num <= 0:
            log_error('invalid worker_process_num: %d' % self.m_work_process_num)
            sys.exit(1)
        '''spawn multi processes'''
        self.m_stastic_queue = multiprocessing.Queue()
        worker_proc_lst = []
        site_sum_len = len(site_file_lst)
        each_proc_file_num = site_sum_len / self.m_work_process_num
        if each_proc_file_num == 0:
            each_proc_file_num = 1
        proc_beg_idx = 0
        while proc_beg_idx < site_sum_len:
            proc_end_idx = proc_beg_idx + each_proc_file_num
            if proc_end_idx > site_sum_len:
                proc_end_idx = site_sum_len
            worker_file_lst = site_file_lst[proc_beg_idx:proc_end_idx]
            worker_proc = multiprocessing.Process(target=self.worker_process, args=(worker_file_lst, ))
            worker_proc_lst.append(worker_proc)
            worker_proc.start()
            proc_beg_idx = proc_end_idx
        log_info('spawn %d worker proccess' % len(worker_proc_lst))
        '''http listen thread'''
        http_listen_thd = threading.Thread(target=self.http_stat_thread)
        http_listen_thd.start()
        while not self.m_exit:
            while self.m_stastic_queue.qsize() == 0:
                time.sleep(2)
                continue
            stat_data = self.m_stastic_queue.get(True, 1)
            if len(stat_data) != 4:
                log_error('skip invalid stat data: %s' % stat_data)
            self.m_stat_server.add_stat(stat_data[0], stat_data[1], stat_data[2], stat_data[3])
        self.m_stat_server.close()
        '''handle close'''
        http_listen_thd.join()
        for worker_proc in worker_proc_lst:
            worker_proc.join()
    '''close process'''
    def close(self):
        self.m_exit = True
        log_info('MetaSearch begin close ... ')
        for thd in self.m_thd_lst:
            thd.join()
        log_info('MetaSearch close success. ')
    '''抓取线程'''
    def fetch_runtine(self, site_task, thd_id):
        fetcher = proxy_spider.NewProxySpider(proxy_server_url = self.m_proxy_server_url, \
                                              internal_thread_num = site_task.m_fetch_thread_num, \
                                              foreign_thread_num = 0, socket_timeout = 10)
        #log_info('fetch_runtine thread %d start!' % thd_id)
        fetcher.start()
        last_schedule_time = 0
        schedule_interval_sec = site_task.get_schedule_interval()
        while self.m_exit is False:
            cur_time = time.time()
            if last_schedule_time + schedule_interval_sec > cur_time:
                time.sleep(1)
                continue
            last_schedule_time = time.time()
            log_info('fetch_runtine thread %d fetch_one_round!' % thd_id)
            self.fetch_one_round(fetcher, site_task, self.m_exit)
            #抓取间隔为-1则只抓一次
            if schedule_interval_sec < 0:
                break
        thd_lst = site_task.get_thread_lst()
        for thd in thd_lst:
            thd.join()
        log_info('fetch_runtine thread %d stop!' % thd_id)
        
def main():
    config_file = 'metasearch.cfg'
    if len(sys.argv) < 2:
        log_info('use default config: %s' % config_file)
    else:
        config_file = sys.argv[1]
    log_info('main process start!')
    metasearch = MetaSearch()
    metasearch.load_config(config_file)
    metasearch.start()

if __name__ == "__main__":
    main()