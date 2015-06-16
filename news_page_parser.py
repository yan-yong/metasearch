from common import *
import json,time
import lxml, lxml.html, BeautifulSoup
import os, sys, re
from search_page_parser import *

class NewsPageParser(SearchPageParser):
    def __init__(self):
        self.m_xpath_dic = {}
        self.m_xpath_dic['news.chinaso.com'] = '//p[@class="snapshot"]/a/@href'
        self.m_xpath_dic['news.baidu.com'] = '//span[@class="c-info"]/a[@class="c-more_link"]/@href'
        self.m_xpath_dic['news.haosou.com'] = '//p[@class="newsinfo"]/a[@class="same"]/@href'
        
    def get_xpath_pattern(self):
        host = self.get_host()
        same_news_xpath = None
        if self.m_xpath_dic.has_key(host):
            same_news_xpath = self.m_xpath_dic.get(host)
        return same_news_xpath
    def get_host(self):
        protocol, other = urllib.splittype(self.m_url)
        host, path = urllib.splithost(other)
        return host
    def proc_one(self, item_html, xpath_val):
        if xpath_val is None or len(xpath_val)<3:
            return ''
        result = item_html.xpath(xpath_val);
        res_str = ''
        for i in result:
            res_str += i 
        res_str = self.safe_decode(res_str)
        return res_str
    def safe_decode(self, str):
        if str!= None:
            str = str.replace('\t', '')
            str = str.replace('\r', '')
            str = str.replace('\n', '')
            return str
        else:
            return ''
    def get_parse_result(self, url, html_str, html_header=None):
        result_lst = []
        html_str = uniform_web_content(url, html_str)
        html_str = uniform_charset(html_str)
        self.start(url, html_str, html_header)
        #open('3_%d_html_str.html'%int(time.time()),'w').write(html_str)
        for result_dict_str, child_str in self.parse_search_page():
            if result_dict_str is None:
                continue
            same_news_xpath = self.get_xpath_pattern()
            if same_news_xpath is not None and child_str is not None:
                html_str = "<html><head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" /></head><body>" + child_str + "</body></html>"
                html  = lxml.html.fromstring(html_str)
                same_news_link = self.proc_one(html, same_news_xpath)
                #same_news_link = uniform_link(same_news_link, self.m_url)
                #log_info('######get same news link: %s' % same_news_link)
                if len(same_news_link) > 5:
                    same_news_link = uniform_link(same_news_link, self.m_url)
                    result_dic = json.loads(result_dict_str)
                    result_dic['same_news_link'] = same_news_link
                    result_dict_str = json.dumps(result_dic)
            result_lst.append(result_dict_str)
        log_info('get %d parser result %s' % (len(result_lst), url))
        return result_lst
