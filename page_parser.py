# -*- coding: utf-8 -*-
import urllib2, sys, BeautifulSoup, time
import HTMLParser, re, copy
from parser_common import *
import spider, hashlib
from uniform_webpage import *

'''basic class TextUnit'''
class TextUnit:
    '''config arguments'''
    __text_seperate__ = ' '
    __link_decide_level = 2
    '''config end'''    
    def __init__(self, node):
        self.m_text_nodes = []
        self.m_text_len  = 0
        self.m_ordinary_text_nodes = []
        self.m_ordinary_text_len = 0
        self.m_link_nodes = []
        self.m_node = node
        self.m_xpath= None
        self.m_is_link = self.__is_link_node(self.m_node)
    def __unescape(self, text):
        return HTMLParser.HTMLParser().unescape(text)
    def __is_link_node(self, node):
        if type(node) != BeautifulSoup.Tag:
            node = node.parent
        for i in range(self.__link_decide_level):
            if not node:
                break
            if is_link_tag(node):
                return True
            node = node.parent
        return False        
    def is_link_unit(self):
        return self.m_is_link
    def link_rate(self):
        if self.m_is_link or not self.m_text_len:
            return 1.0
        return (self.m_text_len - self.m_ordinary_text_len)*1.0 / self.m_text_len
    def add_text_node(self, node):
        cur_text_len = len(get_node_text(node))
        self.m_text_nodes.append(node)
        self.m_text_len += cur_text_len
        if not self.m_is_link and not self.__is_link_node(node):
            self.m_ordinary_text_nodes.append(node)
            self.m_ordinary_text_len += cur_text_len
        else:
            self.m_link_nodes.append(node)
    def get_text(self, text_type = 0):
        nodes = None
        if text_type == 0:
            nodes = self.m_text_nodes
        elif text_type == 1:
            nodes = self.m_ordinary_text_nodes
        else:
            nodes = self.m_link_nodes
        cur_text = ''
        for node in nodes:
            if len(cur_text) > 0:
                cur_text += self.__text_seperate__
            cur_text += get_node_text(node)
        return cur_text
    def get_text_list(self, text_type = 0):
        nodes = None
        text_lst = []
        if text_type == 0:
            nodes = self.m_text_nodes
        elif text_type == 1:
            nodes = self.m_ordinary_text_nodes
        else:
            nodes = self.m_link_nodes
        cur_text = ''
        for node in nodes:
            cur_tuple = ()
            cur_text = get_node_text(node)
            cur_tuple = (node,cur_text)
            text_lst.append(cur_tuple)
        return text_lst
    '''text_type: 0 --> all text; 1 --> ordinary text; 2 --> link text'''
    def get_text_len(self, text_type = 0):
        if text_type == 0:
            return self.m_text_len;
        elif text_type == 1:
            return self.m_ordinary_text_len
        else:
            return self.m_text_len - self.m_ordinary_text_len
    def get_ordinary_text_len(self):
        return self.m_ordinary_text_len 
    def get_link_text_len(self):
        return self.m_text_len - self.m_ordinary_text_len
    def get_ordinary_text(self):
        cur_text = ''
        for node in self.m_ordinary_text_nodes:
            if len(cur_text) > 0:
                cur_text += self.__text_seperate__            
            cur_text += get_node_text(node)
        return cur_text
    def get_xpath(self, allow_id = True, parent_node = None):
        if self.m_xpath:
            return self.m_xpath
        xpath_list = []
        cur_node = self.m_node
        if type(self.m_node) == BeautifulSoup.NavigableString:
            xpath_list.append('text()')
            cur_node = cur_node.parent
        while cur_node and parent_node != cur_node and type(cur_node) != BeautifulSoup.BeautifulSoup:
            find_id = False
            if allow_id:
                for attri in cur_node.attrs:
                    if attri[0] == 'id':
                        xpath_list.append("id('%s')" % attri[1])
                        find_id = True
                        break
            if find_id:
                break
            tag_cnt = 1
            for sibling in cur_node.fetchPreviousSiblings():
                if sibling.name.lower() == cur_node.name.lower():
                    tag_cnt += 1
            if cur_node.name !='html':
                xpath_list.append('%s[%d]' % (cur_node.name, tag_cnt))
            cur_node = cur_node.parent
            #open('test.html', 'w').write(str(cur_node.parent))
        xpath_list.reverse()
        if len(xpath_list) and xpath_list[0].lower() == 'html[1]':
            xpath_list[0] = '/' + xpath_list[0]
        self.m_xpath = '/'.join(xpath_list)
        return self.m_xpath

class QueryParam:
    def __init__(self, param_str):
        arr_lst = param_str.split('=')
        self.m_key = ''
        self.m_val = ''
        if len(arr_lst) > 0:
            self.m_key = arr_lst[0]
        if len(arr_lst) > 1:
            self.m_val = arr_lst[1]
    def __cmp__(self, other):
        if self.m_key < other.m_key:
            return -1  
        if self.m_key > other.m_key:
            return 1
        if self.m_val.isdigit() and other.m_val.isdigit():
            return int(self.m_val) - int(other.m_val)
        if self.m_val < other.m_val:
            return -1  
        if self.m_val > other.m_val:
            return 1    
        return 0 
    def __str__(self):
        return '%s=%s' % (self.m_key, self.m_val)
    def is_digit(self):
        return self.m_val.isdigit()
    
class NextPageLink:
    def __init__(self, url):
        self.m_url = url
        self.m_path = ''
        self.m_query_params = []
        self.m_path, query = urllib.splitquery(url)
        if query is not None:
            query_params = my_split(query, '&')
            if len(query_params) == 0:
                return
            self.m_query_params = map(QueryParam, query_params)
            self.m_query_params.sort()
    def __str__(self):
        return '%s?%s' % (self.m_path, '&'.join(map(str, self.m_query_params)))
    def __lt__(self, other):
        if self.m_path != other.m_path:
            return self.m_path < other.m_path
        if len(self.m_query_params) != len(other.m_query_params):
            return str(self.m_query_params) < str(other.m_query_params)
        for i in range(0, len(self.m_query_params)):
            if self.m_query_params[i] != other.m_query_params[i]:
                return self.m_query_params[i] < other.m_query_params[i]
        return False
    def prefix_to(self, other):
        for cur_param in self.m_query_params:
            found = False
            for other_param in other.m_query_params:
                if cur_param.m_key == other_param.m_key and \
                    (cur_param.is_digit() or cur_param.m_val == other_param.m_val):
                    found = True
                    break
            if not found:
                return False
        i = 0
        while i < len(other.m_query_params):
            found = False
            other_param = other.m_query_params[i]
            for cur_param in self.m_query_params:
                if cur_param.m_key == other_param.m_key:
                    found = True
                    break
            if not found and not other_param.is_digit():
                del(other.m_query_params[i])
                continue
            i += 1
        return True

class PageParser:
    '''config arguments'''
    __image_tag_set__ = set(['img'])
    __title_tag_pattern__ = r'h\d'
    __title_max_len__ = 40*3
    __page_title_min_len__ = 4*3
    __title_max_levenshtein_rate__ = 0.5
    # content link rate max limit
    __content_max_link_rate__ = 0.5
    # content main component rate
    __main_text_node_rate__ = 0.7
    '''config end'''
    def __init__(self, url, html):
        self.m_soup = None
        self.m_text_units = {}
        self.m_content_nodes   = []
        self.m_page_title_node = None
        self.m_title_node   = None
        self.m_url = url
        self.m_img_nodes = []
        self.m_parsed_unit = False
        #open('aa_%d.html'%int(time.time()), 'w').write(html)
        if type(html) != BeautifulSoup.BeautifulSoup: 
            self.m_soup = BeautifulSoup.BeautifulSoup(str(html))
        #open('m_soup_%d.html'%int(time.time()), 'w').write(str(self.m_soup))
        self.__title_tag_pattern__ = re.compile(self.__title_tag_pattern__)
        html_lst = self.m_soup.findAll('html')
        if len(html_lst)==0:
            log_info('find no html node')
            self.m_html = None
        else:
            self.m_html = max(html_lst, key = lambda node: len(node.text))
        if not self.m_html:
            spider.log_error('%s have no html tag.' % self.m_url)
            return  
    def __is_head_tag(self, node):
        return re.match(self.__title_tag_pattern__, node.name.lower())
    def get_text_unit(self, node, insert = False):
        if node is None:
            return None
        obj_id = id(node)
        unit = self.m_text_units.get(obj_id)
        if not unit and insert:
            unit = TextUnit(node)
            self.m_text_units[obj_id] = unit
        return unit
    def __filter_node(self, node):
        if type(node) != BeautifulSoup.NavigableString and type(node) != BeautifulSoup.Tag:
            return False
        unit = self.get_text_unit(node)
        return unit and unit.link_rate() < self.__content_max_link_rate__ 
    def __get_oridinary_text_len(self, node):
        unit = self.get_text_unit(node)
        if not unit:
            return 0
        return unit.get_ordinary_text_len()
    def __is_text_node(self, node):
        if type(node) != BeautifulSoup.NavigableString:
            return False
        if spider.my_strip(str(node)) == '':
            return False
        tree_node = node.parent     
        while tree_node and type(tree_node) != BeautifulSoup.BeautifulSoup:
            if type(tree_node) != BeautifulSoup.Tag:
                return False
            if is_no_use_tag(tree_node):
                return False
            tree_node = tree_node.parent
        return True
    def __add_tree_text_node(self, node, filter_callback):
        if not self.__is_text_node(node):
            return False
        try:
            if filter_callback is not None and filter_callback(node):
                return False
        except:
            pass 
        tree_node = node
        while tree_node and type(tree_node) != BeautifulSoup.BeautifulSoup:
            self.get_text_unit(tree_node, True).add_text_node(node)
            tree_node = tree_node.parent
        return True
    def __extract_text_units(self, filter_callback):
        if len(self.m_text_units) > 0 or self.m_parsed_unit:
            return
        self.m_parsed_unit = True
        for node in self.m_soup.recursiveChildGenerator():
            if type(node) == BeautifulSoup.NavigableString:
                self.__add_tree_text_node(node, filter_callback)
            if type(node) == BeautifulSoup.Tag:
                tag_name = node.name.lower()
                if tag_name == 'title':
                    self.m_page_title_node = node
                if tag_name in self.__image_tag_set__:
                    self.m_img_nodes.append(node)
    def get_img(self):
        assert(self.m_html)
        self.__extract_text_units(None)
        if len(self.m_img_nodes) == 0:
            for node in self.m_soup.recursiveChildGenerator():
                if type(node) != BeautifulSoup.Tag:
                    continue
                tag_name = node.name.lower()
                if tag_name in self.__image_tag_set__:
                    self.m_img_nodes.append(node)            
        image_url_lst = []
        for img in self.m_img_nodes:
            '''for attri in img.attrs:
                if attri.get('src'):'''
            src_attr = get_tag_attr(img, 'src')
            if src_attr is not None:
                image_url_lst.append(my_strip(src_attr))
            src_attr = get_tag_attr(img, 'data-original')
            if src_attr is not None:
                image_url_lst.append(my_strip(src_attr))
            src_attr = get_tag_attr(img, 'src2')
            if src_attr is not None:
                image_url_lst.append(my_strip(src_attr))
        return image_url_lst            
    def text_node_generator(self):
        assert(self.m_html)
        self.__extract_text_units(None)
        if len(self.m_text_units) > 0:
            for node in self.get_text_unit(self.m_html).m_text_nodes:
                yield node
        else:
            for node in self.m_html.recursiveChildGenerator():
                if type(node) == BeautifulSoup.NavigableString and len(node) > 0 and \
                self.__is_text_node(node) and len(str(node)) > 0:
                    yield node
    def parse_text_units(self, filter_callback = None):
        assert(self.m_html)
        self.__extract_text_units(filter_callback)
        return self.m_text_units
    def parse_title(self):
        assert(self.m_html)
        self.__extract_text_units(None)
        if self.m_title_node:
            return (HTMLParser.HTMLParser().unescape(self.m_title_node), \
                HTMLParser.HTMLParser().unescape(self.m_page_title_node))            
        first_h_node = None
        min_diff_len = self.__title_max_len__
        for node in self.text_node_generator():
            tagname  = node.parent.name.lower()
            cur_text = str(node)
            if tagname == "title":
                self.m_page_title_node = cur_text
                continue
            if len(cur_text) > self.__title_max_len__ or not self.m_page_title_node:
                continue
            page_title_len = len(str(self.m_page_title_node))
            diff_threshhold_len = max(len(cur_text), page_title_len) * self.__title_max_levenshtein_rate__
            diff_len = levenshtein(str(self.m_page_title_node), cur_text)
            if not first_h_node and re.match(self.__title_tag_pattern__, tagname):
                first_h_node = node
                if diff_len < diff_threshhold_len:
                    self.m_title = node
                    break
            if diff_len < diff_threshhold_len and diff_len < min_diff_len:
                min_diff_len = diff_len
                self.m_title_node = node
        if not self.m_title_node and first_h_node:
            self.m_title_node = first_h_node
        ret_title_node = None
        if self.m_title_node is not None:
            ret_title_node = HTMLParser.HTMLParser().unescape(self.m_title_node)
        ret_page_title_node = None
        if self.m_page_title_node is not None:
            ret_page_title_node = HTMLParser.HTMLParser().unescape(self.m_page_title_node)
        return (ret_title_node, ret_page_title_node)
    def get_next_page(self):
        assert(self.m_html)
        self.__extract_text_units(None)
        link_nodes = self.m_soup.findAll('a')
        if len(link_nodes) == 0:
            spider.log_error('%s have no link url.' % self.m_url)
            return None
        cur_link = NextPageLink(self.m_url)
        link_str_array = []
        protocol, other = urllib.splittype(self.m_url)
        host, path = urllib.splithost(other)
        for link in link_nodes:
            cur_url = get_tag_attr(link, 'href')
            if cur_url is not None:
                if not cur_url.startswith('http') and not cur_url.startswith('?') and not cur_url.startswith('/'):
                    cur_url = '/%s'%cur_url
                #含有“下一页”、“下页”的，直接用下一页的链接
                link_node_str = str(link)+str(link.contents)
                link_node_str = link_node_str.replace('\r','').replace('\n','').replace(' ','')
                if link_node_str.count('下一页')>0 or link_node_str.count('下页')>0 or link_node_str.count('u\'>>\''):
                    if cur_url[0]=='&':
                        regex_patten = re.sub('\d+','\\d+',cur_url)
                        cur_url = re.sub(regex_patten,cur_url,self.m_url)
                    if cur_url.count('pageIndex')==1:
                        pag_num_regex_pat = r'pageIndex=(\d+)'
                        regex = re.compile(pag_num_regex_pat)
                        m = regex.search(cur_url)
                        page_num = m.group(1)
                        page_num_pre = str(int(page_num)-1)
                        cur_url = re.sub('&p=%s'%page_num_pre,'&p=%s'%page_num,self.m_url)
                    #print cur_url
                    cur_url = uniform_link(cur_url, self.m_url)
                    next_page_other, next_page_query = urllib.splitquery(cur_url)
                    cur_page_other, cur_page_query = urllib.splitquery(self.m_url)
                    if cur_url.find('?')>0 and next_page_other!= cur_page_other:
                        cur_url = '%s?%s'%(cur_page_other,next_page_query)
                    return cur_url
                cur_url = uniform_link(cur_url, self.m_url)
                if not cur_url.find(host)>0:
                    continue
                link_str_array.append(cur_url)
                #print cur_url
        next_links = []
        for link_str in link_str_array:
            next_link = NextPageLink(link_str)
            if cur_link.prefix_to(next_link):
                next_links.append(next_link)
        next_links.append(cur_link)
        next_links.sort()
        #for next_link in next_links:
            #print '#####', next_link
        #print '1####', str(cur_link)
        for next_link in next_links:
            if cur_link < next_link:
                next_page_link = str(next_link)
                next_page_other, next_page_query = urllib.splitquery(next_page_link)
                cur_page_other, cur_page_query = urllib.splitquery(self.m_url)
                if next_page_other!= cur_page_other and cur_page_other!=self.m_url:
                    next_page_link = '%s%s'%(cur_page_other,next_page_query)
                return next_page_link
        spider.log_error('%s cannot find next url.' % self.m_url)
        return None
    def parse_content(self, filter_callback = None):
        assert(self.m_html)
        time1 = time.time()
        self.__extract_text_units(filter_callback)
        time2 = time.time()
        #print "sss %s" % (time2 - time1)
        main_node = self.m_html
        while main_node:
            text_unit = self.get_text_unit(main_node)
            if text_unit == None:
                return ''
            if type(main_node) == BeautifulSoup.NavigableString or len(main_node.contents) == 0:
                return text_unit.get_ordinary_text()
            ordinary_text_len = self.__get_oridinary_text_len(main_node)
            #print "1* %d %s" % (ordinary_text_len, text_unit.get_ordinary_text())
            if not ordinary_text_len:
                return ''
            node_lst = filter(self.__filter_node, main_node.contents)
            if len(node_lst) == 0:
                return ''
            if len(node_lst) == 1:
                main_node = node_lst[0]
                continue
            node_lst.sort(lambda x, y: self.__get_oridinary_text_len(y) - self.__get_oridinary_text_len(x))
            if self.__get_oridinary_text_len(node_lst[0]) > ordinary_text_len*self.__main_text_node_rate__:
                main_node = node_lst[0]
                #print "2* %f > %f" % (self.__get_oridinary_text_len(node_lst[0]), ordinary_text_len*self.__main_text_node_rate__)
                continue
            time3 = time.time()
            #print "1 cost %f, 2 cost %f" % (time2 - time1, time3 - time2)
            return text_unit.get_ordinary_text()                
        return ''
    
'''get a BeautifulSoup node text length'''
def get_node_text_len(parser, node, text_type = 0):
    text_len = 0
    unit = parser.get_text_unit(node)
    if unit:
        text_len = unit.get_text_len(text_type)
    return text_len

'''compare text/ordinary_text/link_text length of BeautifulSoup nodes '''
'''text_type: 0 --> all text; 1 --> ordinary text; 2 --> link text'''
def cmp_node_contents_text(parser, item1, item2, text_type = 0):
    return get_node_text_len(parser, item2) - get_node_text_len(parser, item1)

'''obtain the main node based on text length'''
def get_main_node(node, parser, main_child_text_rate, text_type = 0):
    cur_node = node
    while type(cur_node) == BeautifulSoup.Tag and len(cur_node.contents):
        unit = parser.get_text_unit(cur_node)
        if not unit:
            break
        if len(cur_node.contents) == 1:
            cur_node = cur_node.contents[0]
            continue
        sum_len = unit.get_text_len(text_type)
        #sorted_lst = sorted(cur_node.contents, cmp=lambda x, y: cmp_node_contents_text(parser, x, y, text_type))
        max_child_node = max(cur_node.contents, key=lambda x: get_node_text_len(parser, x))
        max_child_unit = parser.get_text_unit(max_child_node)
        if not max_child_unit:
            break
        if max_child_unit.get_text_len() > main_child_text_rate*sum_len:
            cur_node = max_child_node
            continue
        return cur_node
    return None
         
if __name__ == '__main__':
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    '''url最好要去除一些无关参数，最好带第一页的页码'''
    #url = 'http://www.baidu.com/s?wd=python&pn=0'
    #url = 'http://wenku.baidu.com/search?word=python&lm=0&od=0&pn=0'
    #url = 'http://search.dangdang.com/?key=python&page_index=1'
    url = 'http://www.evsou.com/search_%E5%85%AC%E5%8A%A1%E5%91%98%E8%80%83%E8%AF%95_1.html'
    http_headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36 SE 2.X MetaSr 1.0"}
    next_url = None
    for i in range(5):
        cur_url = url
        if next_url is not None:
            cur_url = next_url
        request = urllib2.Request(cur_url, headers=http_headers)
        html_str = urllib2.urlopen(request).read()
        if html_str.find('charset=GBK')>0 or html_str.find('charset=gbk')>0:
            html_str = html_str.decode('gbk','ignore').encode('utf8')
        elif html_str.find('charset=GB2312')>0 or html_str.find('charset=gb2312')>0:
            html_str = html_str.decode('gb2312','ignore').encode('utf8')
        html_str = "<html><head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" /></head><body>" + html_str + "</body></html>"
        print "\n\nStart Parse ", cur_url
        open('test.html','w').write(html_str)
        beg_time = time.time()
        soup = PageParser(cur_url, html_str)
        next_url = soup.get_next_page()
        print "next_page: %s" % next_url
        #real_title, page_title = soup.parse_title()
        #print "title: %s" % real_title
        #print "page title: %s" % page_title
        #content = soup.parse_content()
        #print "content: %s" % content
        #print "cost: %f\n" % (time.time()- beg_time)
