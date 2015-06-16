# -*-: encoding: utf-8 -*-

import BeautifulSoup, lxml, lxml.html, lxml.html.soupparser
import urllib2, copy, re, json
from parser_common import *
from page_parser import *
from common import *
from datetime_extractor import *
from source_extractor import *
from parser_extractor import *
from uniform_webpage import *

global_xpath_dic = {}

def uniform_charset(html_str):
    sub_str = html_str[0:500]
    other_str = html_str[500:]
    return re.sub(r'<\s*meta\s*charset="?(\S+)"?\s*>', \
        r'<meta http-equiv="Content-Type" content="text/html; charset=\1/>', sub_str) + other_str
class SearchPageParser(parser_extractor):
    __main_child_text_rate = 0.6
    __max_find_repeat_child_level = 3
    __max_tag_tree_struct_level = 2
    __min_repeat_child_num = 3
    __max_tag_diff_rate = 0.4
    __main_search_item_text_rate = 0.99
    __title_min_len = 12
    __title_max_len_rate = 0.5
    __unit_ordinary_rate = 0.3
    __item_parse_rate = 0.8
    __summary_min_len_rate = 0.4
    __summary_ordinary_len_rate = 0.5
    __datetime_node_max_text = 30
    __source_node_max_text  = 30
    __item_common_xpath_cnt = 2
    __max_node_text_len = 400
    m_parser = None
    '''def __init__(self, url, html_str, html_header = None,has_summary = True):
        self.m_url = url
        self.m_html_str = html_str
        self.m_html_header = html_header
        self.m_has_summary = has_summary'''
    def start(self, url, html_str, html_header = None,has_summary = True):
        self.m_url = url
        self.m_html_str = html_str
        self.m_html_header = html_header
        self.m_has_summary = has_summary
        self.m_parser = PageParser(self.m_url, self.m_html_str )
        self.m_parser.parse_text_units(self.__filter_text_node)
        self.m_datetime = DateTimeExtractor()
        self.m_source = SourceExtractor()
        if not self.m_has_summary:
            self.__title_max_len_rate = 0.8
    def __filter_text_node(self, node):
        return len(str(node)) > self.__max_node_text_len
    def __get_tag_name_lst(self, tree):
        res = []
        last_level_nodes = []
        last_level_nodes.extend(tree.contents)
        cur_level = 1
        for level in xrange(cur_level+1, self.__max_tag_tree_struct_level+1):
            cur_level_nodes = []
            for node in last_level_nodes:
                if type(node) == BeautifulSoup.Tag and not is_no_use_tag(node):
                    res.append(node.name)
                    cur_level_nodes.extend(node.contents)
            cur_level += 1
            last_level_nodes = cur_level_nodes
        return res    
    def __find_max_repeat_child_tree(self, node):
        if type(node) != BeautifulSoup.Tag:
            return []
        count_tag_dict = {}
        max_tag_len  = 0
        max_tag_name = ''
        '''先判断node下同一个标签名的结点数目是否过少，提前减枝'''
        for child in node.contents:
            #print '### %s' % str(child)
            if type(child) != BeautifulSoup.Tag or is_no_use_tag(child):
                continue
            cur_lst = count_tag_dict.get(child.name)
            if not cur_lst:
                cur_lst = []
                count_tag_dict[child.name] = cur_lst
            cur_lst.append(child)
            if len(cur_lst) > max_tag_len:
                max_tag_len  = len(cur_lst)
                max_tag_name = child.name
        if max_tag_len < self.__min_repeat_child_num:
            return []
        child_lst = count_tag_dict[max_tag_name]
        return child_lst
        '''
        max_repeat_lst = []
        cur_repeat_lst = [child_lst[0]]
        #利用编辑距离，计算子节点的标签结构相似性
        for i in xrange(1, len(child_lst)):
            lst1 = self.__get_tag_name_lst(child_lst[i-1])
            lst2 = self.__get_tag_name_lst(child_lst[i])
            diff_val = levenshtein(lst1, lst2)
            if diff_val > self.__max_tag_diff_rate*max(len(lst1), len(lst2)):
                if len(cur_repeat_lst) > len(max_repeat_lst):
                    max_repeat_lst = cur_repeat_lst
                cur_repeat_lst = []
            cur_repeat_lst.append(child_lst[i])
        if len(cur_repeat_lst) > len(max_repeat_lst):
            max_repeat_lst = cur_repeat_lst
        if len(max_repeat_lst) < self.__min_repeat_child_num:
            return []
        return max_repeat_lst
        '''
    '''用来去除item下区域的xpath前缀，这个前缀在items遍历时无意义'''
    def __remove_xpath_prefix(self, item_xpath):
        idx = item_xpath.index('/')
        return item_xpath[idx+1:]
    def __exclude_tag_from_text(self, xpath, tag_name_lst):
        res_lst = []
        mid_val = '%s//*'%xpath
        for tag_name in tag_name_lst:
            mid_val += '[local-name()!="%s"]' % tag_name
            #res_lst.append('%s/text()' % mid_val)
        mid_val += '/text()'
        return mid_val
    def __host(self):
        protocol, other = urllib.splittype(self.m_url)
        host, path = urllib.splithost(other)
        return host
    def __extract_summary_xpath(self, node, date_node, parent_node):
        units = self.m_parser.get_text_unit(node)
        if units is None:
            return None
        ordinary_text_len = units.get_ordinary_text_len()
        summary_xpath = units.get_xpath(False, parent_node)
        summary_xpath = self.__remove_xpath_prefix(summary_xpath)
        #return summary_xpath + '//text()'
        '''return summary_xpath + '/text()'+'|' + self.__exclude_tag_from_text(summary_xpath, 'a', 3) +'|' \
             +self.__exclude_tag_from_text(summary_xpath, 'style', 5) + '|' \
             +self.__exclude_tag_from_text(summary_xpath, 'script', 3)'''
        tag_name_lst = ['a','style','script']
        return summary_xpath + '/text()'+'|' + self.__exclude_tag_from_text(summary_xpath, tag_name_lst)
        '''
        summary_xpath = '%s/*[local-name()!="script"][local-name()!="style"][local-name()!="link"]' % summary_xpath 
        remove_class = None
        #由于日期结点可能位于这个摘要结点之中，先把日期结点的class属性求出来，在xpath中排除掉
        if date_node:
            for item in units.m_ordinary_text_nodes:
                if id(date_node) == id(item) and item.parent.attrs.get('class') and len(item.parent.attrs['class']) > 0:
                    remove_class = item.parent.attrs['class'][0]
        #一般来说，summary区域是不包含链接的，但是有的站点summary就是链接，这里先预判一下是否是非链接型的summary， 如果是就只取非链接区域
        if units.link_rate() < 1 - self.__summary_ordinary_len_rate:
            summary_xpath = '%s[local-name()!="a"]' % summary_xpath
        if remove_class is not None:
            summary_xpath = '%s[@class != "%s"]' % (summary_xpath, remove_class)
        summary_xpath += '//text()'
        summary_xpath = self.__remove_xpath_prefix(summary_xpath)
        return summary_xpath
        '''
    def __extract_title_xpath(self, node, parent_node):
        units = self.m_parser.get_text_unit(node)
        if units is None:
            return None
        link_xpath = None
        ordinary_text_len = units.get_ordinary_text_len()
        title_xpath = units.get_xpath(False, parent_node)
        title_xpath = self.__remove_xpath_prefix(title_xpath)
        link_xpath = '%s/@href'%title_xpath
        tag_name_lst = ['a','style','script']
        title_xpath =  title_xpath+'/text()'+'|' +self.__exclude_tag_from_text(title_xpath, tag_name_lst)
        return title_xpath, link_xpath
    
    def __extract_date_xpath(self, node, parent_node):
        units = self.m_parser.get_text_unit(node)
        if units is None:
            return ''
        ordinary_text_len = units.get_ordinary_text_len()
        date_xpath = units.get_xpath(False, parent_node)
        date_xpath = self.__remove_xpath_prefix(date_xpath)
        date_xpath = '%s'%date_xpath
        if date_xpath is None:
            return ''
        if date_xpath.find('/text()')>0:
            return date_xpath
        return date_xpath + '/text()'
    def __extract_item_xpath(self, node, child_node):
        units = self.m_parser.get_text_unit(node)
        if units is None:
            return ''
        ordinary_text_len = units.get_ordinary_text_len()
        item_xpath = units.get_xpath(True)
        item_xpath = item_xpath.replace('[1]','')
        item_xpath = '%s/%s' % (item_xpath, child_node.name)
        if item_xpath[0:4] == 'body':
            item_xpath = 'html/%s' % item_xpath
        if item_xpath is None:
            return ''
        return item_xpath
    def __save_xpath(self, xpath_dic):
        global global_xpath_dic
        url_host = self.__host()
        if global_xpath_dic.has_key(url_host):
            saved_xpath_dic = global_xpath_dic.get(url_host)
            for key in xpath_dic:
                save_xpath_str = saved_xpath_dic.get(key)
                cur_xpath_str = xpath_dic[key]
                if save_xpath_str is not None and cur_xpath_str is not None and save_xpath_str.find(cur_xpath_str)<0 :
                    xpath_str = '%s|%s'%(save_xpath_str, cur_xpath_str)
                    xpath_str = xpath_str.lstrip('|').rstrip('|')
                    saved_xpath_dic[key] = xpath_str
            global_xpath_dic[url_host] = saved_xpath_dic
        else:
            global_xpath_dic[url_host] = xpath_dic
        
    '''获得xpath的搜索结果列表'''
    def __obtain_search_items(self):
        search_node = self.m_parser.m_html
        repeat_child_lst = []
        '''先依层次向下找到一个主结点：直接剪出一些文本较小的分支，如果两个兄弟结点文本长度差不多，就会停留在其父节点'''
        search_node = get_main_node(search_node, self.m_parser, self.__main_child_text_rate) 
        if not search_node:
            log_error('cannot find search page main node: %s' % self.m_url)
            return search_node, repeat_child_lst
        node = search_node
        if search_node.name == 'body' or search_node.name == 'html':
            node = max(node.contents, key=lambda x: get_node_text_len(self.m_parser, x))
        cal_level = 0
        while node is not None and type(node) == BeautifulSoup.Tag:
            cal_level += 1
            unit = self.m_parser.get_text_unit(node)
            if not unit or unit.get_text_len() == 0:
                log_error('search page text length 0: %s' % self.m_url)
                break
            '''计算该结点的最大重复子树'''
            repeat_child_lst = self.__find_max_repeat_child_tree(node)
            if len(repeat_child_lst) == 0:
                node = max(node.contents, key=lambda x: get_node_text_len(self.m_parser, x))
                continue
            link_cnt = 0
            for index,child in enumerate(repeat_child_lst):
                if is_no_use_tag(child):
                    del repeat_child_lst[index]
                    continue
                if child.find('a'):
                    link_cnt += 1
                else:
                    del repeat_child_lst[index]
            #如果当前重复子树中最大子树长度所占比例大于__main_child_text_rate，则继续查找重复子树
            sum_len = get_node_text_len(self.m_parser,node)
            max_child = max(node.contents, key=lambda x: get_node_text_len(self.m_parser, x))
            max_child_len = get_node_text_len(self.m_parser,max_child)
            if max_child_len > sum_len*self.__main_child_text_rate:
                node = max_child
                continue
            if link_cnt >= self.__min_repeat_child_num:
                search_node = node
                break
            repeat_child_lst = []
            if len(node.contents) == 0 or cal_level > self.__max_find_repeat_child_level:
                break
            '''如果当前结点下找不到重复子树，则在它文本最多的孩子结点下找，但最多只找几层'''
            node = max(node.contents, key=lambda x: get_node_text_len(self.m_parser, x))
        return search_node, repeat_child_lst 
    '''parse each result item'''
    def __parse_result_item(self, child_node, parent_node):
        title = ''
        link = ''
        summary =''
        source = ''
        date =''
        if not child_node or type(child_node) != BeautifulSoup.Tag:
            log_error('crazy error %s' % self.m_url)
            return (title, link, summary, source, date)
        node = get_main_node(child_node, self.m_parser, self.__main_search_item_text_rate)
        #open('main.html','w').write(str(node))
        unit = self.m_parser.get_text_unit(node)
        if unit is None:
            return (title, link, summary, source, date)
        text_type = 1
        if not self.m_has_summary or unit.link_rate() > 1 - self.__unit_ordinary_rate:
            text_type = 0
        sum_len      = unit.get_text_len(text_type)
        title_node = None
        summary_node = None
        date_node    = None
        source_node  = None
        date_accuracy = 0
        source_accuracy = 0
        image_url_lst = []
        '''遍历，找到title、summary、日期、source结点'''
        i=0
        while True:
            '''遍历，找到title、summary、日期、source结点'''
            max_len  = 0
            max_node = None
            for child in node.contents:
                child_unit = self.m_parser.get_text_unit(child)
                if type(child) != BeautifulSoup.Tag or child_unit is None:
                    if child_unit is None and type(child)!=BeautifulSoup.Comment:
                        node_text = get_node_text(child)
                        text_len = len(node_text)
                        if  text_len >= self.__summary_min_len_rate*sum_len:
                            summary_node = child
                    continue
                ''' @date '''
                cur_accuracy, cur_date, cur_date_node, _ = self.m_datetime.extract_lst(child_unit.get_text_list())
                if cur_accuracy > date_accuracy:
                    date = cur_date
                    if type(cur_date_node) == BeautifulSoup.NavigableString:
                        cur_date_node = cur_date_node.parent
                    date_node = cur_date_node
                    date_accuracy = cur_accuracy
                ''' @source '''
                cur_accuracy, cur_source, cur_source_node, _  = self.m_source.extract_lst(child_unit.get_text_list())
                source_unit = self.m_parser.get_text_unit(cur_source_node)
                if source_unit is not None:
                    cur_accuracy += 1
                    if source_unit.is_link_unit():
                        cur_accuracy -= 2
                if cur_accuracy > source_accuracy:
                    source = cur_source
                    if type(cur_source_node) == BeautifulSoup.NavigableString:
                        cur_source_node = cur_source_node.parent
                    source_node = cur_source_node
                    source_accuracy = cur_accuracy
                '''find title node and summary node'''                
                link_text_len = child_unit.get_link_text_len()
                text_len = child_unit.get_text_len(text_type)
                if text_len > max_len:
                    max_len  = text_len
                    max_node = child
                if title == '' and link_text_len >= self.__title_min_len and link_text_len <= sum_len*self.__title_max_len_rate:
                    links = child.findAll(lambda x: self.m_parser.get_text_unit(x) \
                                and self.m_parser.get_text_unit(x).is_link_unit())
                    if not links or not len(links):
                        if self.m_parser.get_text_unit(child) and self.m_parser.get_text_unit(child).is_link_unit():
                            links.append(child)
                        else:
                            continue
                    links.sort(lambda x, y: cmp_node_contents_text(self.m_parser, x, y))
                    title_node = links[0]
                    ''' @link '''
                    link = my_strip(get_tag_attr(title_node, 'href'))
                    if link is None:
                        link = ''
                    if link.count('javascript:')>0:
                        title_node = None
                        link = ''
                        continue
                    if link.count('more.php?id=')>0:
                        title_node = None
                        link = ''
                        continue
                    if not link.startswith('http') and not link.startswith('?') and not link.startswith('/'):
                        link = '/%s'%link
                    link = uniform_link(link, self.m_url)
                    ''' @title '''
                    title_unit = self.m_parser.get_text_unit(title_node)
                    if title_unit is not None:
                        title = title_unit.get_text()
                    continue
                if  text_len >= self.__summary_min_len_rate*sum_len:
                    summary_node = child
            '''说明主节点找错了'''
            if (title_node is None or self.m_has_summary and summary_node is None) and max_len > self.__summary_min_len_rate*sum_len:
                node = max_node
                title_node = None
                title = ''
                summary_node = None
            else:
                break
        '''遍历完成, 下面开始搞summary结点'''
        if self.m_has_summary and summary_node is not None:
            summary_unit = self.m_parser.get_text_unit(summary_node)
            '''考虑到很少数的summary也是链接，对于大部分的网站，summary只用取非文本就行'''
            text_lst = []
            if summary_unit is None:
                summary = get_node_text(summary_node)
                text_lst.append(summary)
            else:
                summary = summary_unit.get_text(text_type)
                text_list = summary_unit.get_text_list()
            '''没有找到日期单元，在summary里找'''
            if date is None or len(date) == 0:
                cur_accuracy, date, cur_date_node, pos = self.m_datetime.extract_lst(text_lst)
                if cur_accuracy:
                    summary = '%s %s' % (summary[0:pos[0]], summary[pos[1]:])
            '''没有找到来源单元，在summary里找'''
            if source is None or len(source) == 0:
                cur_accuracy, source, cur_source_node, pos = self.m_source.extract_lst(text_lst)
                if cur_accuracy:
                    summary = '%s %s' % (summary[0:pos[0]], summary[pos[1]:])
            flag_beg = summary.rfind('...')
            if flag_beg != -1 and flag_beg > 0.5*len(summary):
                summary = summary[0:flag_beg]
            summary = my_strip(summary)
            summary = summary.replace('\n', ' ')
            ''' @summary '''
        #get the xpath for each item
        xpath_dic = {}
        #items xpath
        items_xpath = ''
        items_xpath = self.__extract_item_xpath(parent_node,child_node)
        xpath_dic['ITEMS'] = items_xpath
        title_xpath = ''
        link_xpath = ''
        if title_node is not None:
            title_xpath, link_xpath = self.__extract_title_xpath(title_node, parent_node)
            xpath_dic['TITLE'] = title_xpath
            xpath_dic['LINK'] = link_xpath
        if date_node is not None and date_node != summary_node and date_node.parent != summary_node:
            pubdate_xpath = self.__extract_date_xpath(date_node, parent_node)
            xpath_dic['PUBDATE'] = pubdate_xpath
        source_xpath = ''
        if source_node is not None and source_node != summary_node and source_node.parent != summary_node and source_node != title_node and source_node.parent !=title_node:
            source_xpath = self.__extract_date_xpath(source_node, parent_node)
            xpath_dic['SOURCE'] = source_xpath
        summary_xpath = ''
        if summary_node is not None:
            summary_xpath = self.__extract_summary_xpath(summary_node, date_node, parent_node)
            xpath_dic['SUMMARY'] = summary_xpath
        self.__save_xpath(xpath_dic)
        return (title, link, summary, source, date)
    '''考虑到误差, 忽略只出现一两次的xpath，这些求出来的xpath可能不准，其它的用 或 连接起来'''
    def __etract_common_xpath(self, xpath_lst):
        xpath_lst.sort()
        last_xpath = ''
        cnt = 1
        res_lst = []
        for item in xpath_lst:
            if len(item) == 0:
                continue
            if item != last_xpath:
                if cnt > self.__item_common_xpath_cnt:
                    res_lst.append(last_xpath)
                last_xpath = item
                cnt = 1
            else:
                cnt += 1
        if cnt >= self.__item_common_xpath_cnt:
            res_lst.append(last_xpath)
        return '|'.join(res_lst)
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
    def __proc_date_source(self, text):
        source = ''
        date =''
        date_regex_str = r'(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)'
        if text == '':
            return date, source
        try:
            date_pat = re.compile(date_regex_str)
            text = text.replace('\r\n','').replace('\n','')
            res = re.search(date_pat, text)
            date = '%s-%s-%s %s:%s:%s'%(res.group(1), res.group(2), res.group(3), res.group(4), res.group(5), res.group(6))
            source = text.replace(date, '').strip(' ')
        except Exception,err:
            log_error('__proc_date_source error: %s' % err)
        return date, source
    def parse_search_page_by_xpath(self, html_str=None):
        title = ''
        link = ''
        summary =''
        source = ''
        date =''
        global global_xpath_dic
        cur_xpath_dic = None
        if global_xpath_dic.has_key(self.__host()):
            cur_xpath_dic = global_xpath_dic.get(self.__host())
        if cur_xpath_dic is None:
            yield (title, link, summary, source, date)
        items_lst = []
        if html_str is None:
            items_xpath = cur_xpath_dic.get('ITEMS')
            #print 'items_xpath:%s' % items_xpath
            html_str = uniform_charset(self.m_html_str)
            html = lxml.html.soupparser.fromstring(html_str)
            items_lst = html.xpath(items_xpath)
        else:
            items_xpath = '//body'
            html = lxml.html.document_fromstring(html_str)
            items_lst = html.xpath(items_xpath)
            #items_lst.append(items_ls)
        if len(items_lst)==0:
            log_error('items xpath faild: %s\n%s'%(self.m_url, items_xpath))
            yield (title, link, summary, source, date)
        log_info('get %d items by xpath in %s'%(len(items_lst), self.m_url))
        for item in items_lst:
            try:
                if cur_xpath_dic.has_key('TITLE'):
                    title_xpath = cur_xpath_dic.get('TITLE')
                    title = self.proc_one(item, title_xpath)
                    #print 'title_xpath:%s %s' % (title_xpath, self.m_url)
                if cur_xpath_dic.has_key('LINK'):
                    link_xpath = cur_xpath_dic.get('LINK')
                    link = self.proc_one(item, link_xpath)
                    link = uniform_link(link, self.m_url)
                    #print 'link_xpath:%s %s' % (link_xpath, self.m_url)
                if cur_xpath_dic.has_key('PUBDATE'):
                    date_xpath = cur_xpath_dic.get('PUBDATE')
                    date = self.proc_one(item, date_xpath)
                    #print 'date_xpath:%s' %date_xpath 
                if cur_xpath_dic.has_key('SOURCE'):
                    source_xpath = cur_xpath_dic.get('SOURCE')
                    source = self.proc_one(item, source_xpath)
                    #print 'source_xpath:%s' % source_xpath
                if source == date:
                    source = re.sub('[\d]', '', source)
                    source = re.sub('[:-]', '', source)
                    source = re.sub('[年月日]', '', source)
                elif len(source)>len(title):
                    cur_accuracy, source, _  = self.m_source.extract(source)
                if source is None:
                    source = ''
                cur_accuracy, date, _ = self.m_datetime.extract(date)
                if date is None:
                    date = ''
                if cur_xpath_dic.has_key('SUMMARY'):
                    summary_xpath = cur_xpath_dic.get('SUMMARY')
                    summary = self.proc_one(item, summary_xpath)
                    summary = summary.replace('\r\n','').replace('\r','').replace('\n','').replace(' ','')
            except Exception, err:
                #log_error('parse_search_page_by_xpath err %s' % err)
                break
            yield (title, link, summary, source, date)
    def final_result(self, title, link, summary, source, date ):
        result_dic = {}
        result_dic['title'] = title
        result_dic['link'] = link
        result_dic['summary'] = summary
        result_dic['source'] = source
        result_dic['date'] = date
        json_result = json.dumps(result_dic)
        return json_result
    def parse_search_page(self):
        global global_xpath_dic
        auto_parse = True
        result_lst = []
        if auto_parse :
            #faild child list
            faild_child_lst = []
            search_node, repeat_child_lst = self.__obtain_search_items()
            log_info('%s parsed %d result items.' % (self.m_url, len(repeat_child_lst)))
            if len(repeat_child_lst) < self.__min_repeat_child_num:
                #log_error('cannot parse enough repeat search items: %s.' % self.m_url)
                return
            for child in repeat_child_lst:
                attrs_lst = ['sub-nav','bk-sub-sort','bk-filter']
                if get_tag_attr(child,'class') in attrs_lst:
                    continue
                title, link, summary, source, date = self.__parse_result_item(child, search_node)
                if len(link) == 0 or (len(summary)==0 and self.m_has_summary):
                    faild_child_lst.append(child)
                    '''log_error( 'find no link :%s len(link):%d len(summary):%d %s'%\
                               (self.m_url, len(link),len(summary), str(self.m_has_summary)))'''
                    continue
                if not self.m_has_summary:
                    sum_unit = self.m_parser.get_text_unit(child)
                    sum_text = sum_unit.get_text(1)
                    summary = sum_text.replace(' ','').replace('\r','')#.replace(title.replace(' ',''),'')
                json_result = self.final_result(title, link, summary, source, date )
                yield (json_result, str(child) )
            #用xpath解析失败的child
            if len(faild_child_lst)>0:
                log_info('parse autoparse faild items by xpath %s '%self.m_url)
                for child_node in faild_child_lst:
                    child_lst = []
                    for child in child_node:
                        if type(child) != BeautifulSoup.Tag:
                            continue
                        child_lst.append(str(child))
                    html_str = ''.join(child_lst)
                    html_str = "<html><head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" /></head><body>" + html_str + "</body></html>"
                    for title, link, summary, source, date in self.parse_search_page_by_xpath( html_str = html_str ):
                        if not self.m_has_summary:
                            sum_unit = self.m_parser.get_text_unit(child_node)
                            sum_text = sum_unit.get_text(1)
                            summary = sum_text.replace(' ','').replace('\r','')#.replace(title.replace(' ',''),'')
                        if len(link) == 0 or (len(summary)==0 and self.m_has_summary):
                            '''log_error( 'find no link :%s len(link):%d len(summary):%d %s'%\
                                       (self.m_url, len(link),len(summary), str(self.m_has_summary)))'''
                            continue
                        json_result = self.final_result(title, link, summary, source, date )
                        yield (json_result, str(child) )
        log_info('process %s end'%self.m_url)
    def parse_item_generator(self, url, html, header=None):
        #重写此函数解析web页
        self.start(url, html, header)
        for result_dic,child_str in self.parse_search_page():
            yield result_dic
    def get_parse_result(self, url, html_str, html_header=None):
        result_lst = []
        html_str = uniform_web_content(url, html_str)
        html_str = uniform_charset(html_str)
        self.start(url, html_str, html_header)
        #    open('3_%d_html_str.html'%int(time.time()),'w').write(html_str)
        for result_dict, child_str in self.parse_search_page():
            if result_dict is None:
                continue
            result_lst.append(result_dict)
        log_info('get %d parser result %s' % (len(result_lst), url))
        return result_lst
                
def test(html_str, item_xpath, title_xpath, link_xpath, summary_xpath, source_xpath, date_xpath): 
    html  = lxml.html.fromstring(html_str)
    item_lst = []
    if len(item_xpath):
        item_lst = html.xpath(item_xpath)
        for item in item_lst:
            if len(title_xpath):
                print 'title:', ''.join(item.xpath(title_xpath)).replace('\n', ' ')
            if len(link_xpath):
                print 'link:', ''.join(item.xpath(link_xpath)).replace('\n', ' ')
            if len(summary_xpath):
                print 'summary:', ''.join(item.xpath(summary_xpath)).replace('\n', ' ')
            if len(source_xpath):
                print 'source:', ''.join(item.xpath(source_xpath)).replace('\n', ' ')
            if len(date_xpath):
                print 'date:', ''.join(item.xpath(date_xpath)).replace('\n', ' ')
            print '\n\n'

if __name__ == "__main__":
    stderr = sys.stderr
    stdout = sys.stdout
    reload(sys)
    sys.stderr = stderr
    sys.stdout = stdout
    sys.setdefaultencoding('utf-8')
    
    '''http_headers= { \
    'User-Agent':"Mozilla/5.0 (Windows NT 5.1; rv:32.0) Gecko/20100101 Firefox/32.0", \
    'Accept':"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", \
    #'Accept-Encoding':'gzip, deflate',\
    'Referer':"http://s.weibo.com/", \
    'Cookie':'UOR=www.wyzxwk.com,widget.weibo.com,login.sina.com.cn; SINAGLOBAL=6391197826290.152.1421390027406; ULV=1422585128074:5:5:2:8561728398602.245.1422585128068:1422434210211; un=shanshanshen@126.com; myuid=1960968341; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5DcFlXDjf6JwFTYjM2YQpY5JpX5K2t; SUHB=0rrKfCR6RXGTpe; SUB=_2A255zoA6DeTxGedH7VIY9ibPzz2IHXVavfbyrDV8PUNbuNBeLUj1kW9ivtiFEduaji47rFT3U-lF4icqUA..; YF-V5-G0=447063a9cae10ef9825e823f864999b0; _s_tentry=login.sina.com.cn; Apache=8561728398602.245.1422585128068; YF-Page-G0=bf52586d49155798180a63302f873b5e; YF-Ugrow-G0=69bfe9ce876ec10bd6de7dcebfb6883e; login_sid_t=bc39453fedfc00d635a87d5d003e13d2; SUS=SID-1960968341-1422585962-GZ-fadzl-f724175a04f60b49d82668d0a2d51285; SUE=es%3D425c20b7055776db4c52cb99a4a7818d%26ev%3Dv1%26es2%3Da8f9034676ae0679bc257fb345053a3f%26rs0%3DUJYMyVUr5XFhXfhFCBrvzZDf1rbYYgFGUxH3DXDA2nEI0g%252BAe%252Frq5mdtP4fBeVXBn9jm1xQtGTdEJbx2QZ9TLJzSBq6128JFrIA0v%252FK41WAhiVay8fEP74BWHlDd3erpk7KuTnXGyxYvBBhgM6O6jhm3yMx7P7O%252FbskbHOyfYtE%253D%26rv%3D0; SUP=cv%3D1%26bt%3D1422585962%26et%3D1422672362%26d%3Dc909%26i%3D1285%26us%3D1%26vf%3D0%26vt%3D0%26ac%3D0%26st%3D0%26uid%3D1960968341%26name%3Dshanshanshen%2540126.com%26nick%3Dshanshanshen%26fmp%3D%26lcp%3D2014-07-20%252022%253A21%253A12; ALF=1454121960; SSOLoginState=1422585962'
    }
    url = 'http://s.weibo.com/weibo/%25E4%25B9%25A0%25E8%25BF%2591%25E5%25B9%25B3&Refer=index'
    request = urllib2.Request(url, data = '', headers = http_headers)
    html_str = urllib2.urlopen(request).read()'''
    #url = 'http://weixin.sogou.com/weixin?type=2&query=习近平&ie=utf8'
    #url = 'http://news.so.com/ns?q=习近平&src=tab_www'
    #url = 'http://www.so.com/s?ie=utf-8&q=%E4%B9%A0%E8%BF%91%E5%B9%B3'
    #url = 'http://cn.bing.com/search?q=%E4%B9%A0%E8%BF%91%E5%B9%B3'
    #url = 'http://news.baidu.com/ns?tn=news&word=%E4%B9%A0%E8%BF%91%E5%B9%B3&ie=utf-8'  
    #url = 'http://www.baidu.com/s?word=%E5%BC%80%E5%9D%A6%E5%85%8B&rn=50'
    #url = 'http://www.sogou.com/sogou?query=%E5%BC%80%E5%9D%A6%E5%85%8B'
    #url = 'http://news.baidu.com/ns?ct=1&rn=20&ie=utf-8&bs=%E5%AD%94%E5%AD%90%E5%AD%A6%E9%99%A2+site%3A%28sina.com%29&rsv_bp=1&sr=0&cl=2&f=8&prevct=no&tn=news&word=%E7%91%9E%E5%85%B8+%E5%AD%94%E5%AD%90%E5%AD%A6%E9%99%A2+site%3A%28news.163.com%29&rsv_sug3=2&rsv_sug4=24&rsv_n=2&inputT=1314'
    #url = 'http://www.sogou.com/web?query=%BF%D7%D7%D3%D1%A7%D4%BA&_asf=www.sogou.com&_ast=1422602560&w=01019900&interation=196647&chuidq=39&p=40040100&sut=10255&sst0=1422602560112&lkt=1%2C1422602552491%2C1422602552491'
    #url = 'http://www.sogou.com/web?query=%BF%D7%D7%D3%D1%A7%D4%BA&_asf=www.sogou.com&_ast=1422602795&w=01019900&interation=196648&chuidq=40&p=40040100&sut=8492&sst0=1422602794935&lkt=0%2C0%2C0'
    input_file_name = 'input1.txt'
    f_in = open(input_file_name,'r')
    lines = f_in.readlines()
    for line in lines:
        colums = line.strip('\r').strip('\n').strip(' ').split('\t')
        url = colums[1]
        html_str = urllib2.urlopen(url).read().decode('utf-8','ignore')
        open('aa.html', 'w').write(html_str)
        log_info('html str len: %d' % len(html_str))

        parser = SearchPageParser()
        output_file_name = 'output1.txt'
        f = open(output_file_name,'w')
        for result in parser.run_page_parser(url, html_str):
            try:
                result_json = json.loads(result)
                output_str = '%s\t'%result_json.get('date')
                output_str += '%s\t'%result_json.get('source')
                output_str += '%s\t'%result_json.get('title')
                output_str += '%s\t'%result_json.get('summary')
                output_str += '%s\t'%result_json.get('link')
                #print '***********************************************'
                #print output_str
                f.write('%s\n'%output_str)
                f.flush()
            except Exception,err:
                print '********%s'%err
        f.close()
    log_info('process end')
    
    '''
    items_xpath, title_xpath, link_xpath, summary_xpath, source_xpath, date_xpath = \
        parser.parse_search_page()
    log_info('*** item_xpath %s' % items_xpath)
    log_info('*** title_xpath %s' % title_xpath)
    log_info('*** link_xpath %s' % link_xpath)
    log_info('*** summary_xpath %s' % summary_xpath)
    log_info('*** source_xpath %s' % source_xpath)
    log_info('*** date_xpath %s' % date_xpath)
    test(html_str, items_xpath, title_xpath, link_xpath, summary_xpath, source_xpath, date_xpath)
    '''
