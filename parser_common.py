# -*- coding: utf-8 -*-

import HTMLParser, BeautifulSoup, spider
from common import *

'''useless tags'''
def is_no_use_tag(node, tag_lst = ['script', 'style', 'link', 'textarea'],attr_lst=['display: none;','display:none','cat-menu']):
    tagname = ''
    is_no_use_tag = False
    if type(node) == BeautifulSoup.Tag:
        tagname = node.name.lower()
    is_no_use_tag = tagname in tag_lst
    if not is_no_use_tag:
        if get_tag_attr(node,'style') in attr_lst or get_tag_attr(node,'style') in attr_lst or get_tag_attr(node,'id') in attr_lst:
            is_no_use_tag = True
    return is_no_use_tag

def is_link_tag(node, tag_lst = ['a']):
    tagname = ''
    if type(node) == BeautifulSoup.Tag:
        tagname = node.name.lower()
    return tagname in tag_lst  
def get_node_text(node):
    reload(sys)
    sys.setdefaultencoding('utf-8')
    '''无效节点返回空'''
    if type(node) is not BeautifulSoup.NavigableString and is_no_use_tag(node):
        return ''
    cur_text = ''
    if type(node) == BeautifulSoup.Tag:
        cur_text = my_strip(node.text)
    else:
        cur_text = my_strip(str(node))
    return cur_text.encode('utf-8','ignore')

def get_tag_attr(node, attr_name):
    if node.attrs is None:
        return None
    if type(node.attrs) == list:
        for attr in node.attrs:
            if attr[0] == attr_name:
                return attr[1]
        return None
    if type(node.attrs) == dict:
        if node.attrs.get(attr_name):
            return node.attrs[attr_name]
        return None
    return None 

def get_use_tag_lst(node):
    res = []
    if type(node) != BeautifulSoup.Tag:
        return res
    return node.findAll(lambda x: type(x) == BeautifulSoup.Tag and not is_no_use_tag(x))

def levenshtein(first,second):
    if len(first) > len(second):
        first,second = second,first
    if len(first) == 0:
        return len(second)
    if len(second) == 0:
        return len(first)
    first_length = len(first) + 1 
    second_length = len(second) + 1 
    distance_matrix = [range(second_length) for x in range(first_length)] 
    #print distance_matrix
    for i in range(1,first_length):
        for j in range(1,second_length):
            deletion = distance_matrix[i-1][j] + 1
            insertion = distance_matrix[i][j-1] + 1
            substitution = distance_matrix[i-1][j-1]
            if first[i-1] != second[j-1]:
                substitution += 1
            distance_matrix[i][j] = min(insertion,deletion,substitution)
    return distance_matrix[first_length-1][second_length-1]  

def uniform_link(link, page_url):
    link = my_strip(link)
    if link.startswith('/') :
        protocol, other = urllib.splittype(page_url)
        host, path = urllib.splithost(other)
        link = '%s://%s%s' % (protocol, host, link)  
    elif link.startswith('?'):
        other, query = urllib.splitquery(page_url)
        link = '%s%s' % (other, link)
    return link

def uniform_charset(html_str):
    sub_str = html_str[0:500]
    other_str = html_str[500:]
    pattern = '<\s+meta\s+charset="?(w+)"?\s+>'
    return re.sub(r'<\s*meta\s*charset="?(\S+)"?\s*>', \
        r'<meta http-equiv="Content-Type" content="text/html; charset=\1/>', sub_str) + other_str
