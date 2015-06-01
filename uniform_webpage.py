# -*- coding: utf-8 -*-
import urllib, urllib2, time, base64, sys, os, re
import json, common
import HTMLParser

google_site_cnt = 0

def find_max_len_item(item_list):
    max_item = ''
    for item in item_list:
        if len(max_item) < len(item):
            max_item = item
    return max_item
def find_result_item(item_list):
    str_result_tag = '\\"i\\":\\"search\\"'
    result_item = ''
    for item in item_list:
        if item.find(str_result_tag) != -1:
            result_item = item
    return result_item

def parse_google_json_result(html):
    json_list = html.split('/*""*/')
    if len(json_list) < 2:
        print 'parse_google_json_result split json num less than 2'
        return None
    try:
        max_json_item = find_result_item(json_list[2:])
        #max_json_item = json_list[4]
        #open('input1.txt', 'w').write(max_json_item)
        js_item = json.loads(max_json_item)['d']
        #open('input2.txt', 'w').write(js_item)
        data_js_list = js_item.split('<script>')
        if len(data_js_list) < 2:
            print 'parse_google_json_result split script num less than 2'
            return None
        data_js_item = find_max_len_item(data_js_list)
        search_page = re.findall(r'{.*}', data_js_item)[0]
        ##remove attributes which has no quote 
        search_page = re.sub(r',?"\w+?":\w+', '', search_page)
        search_dict = eval(search_page)
        real_result = search_dict['h'].decode('unicode_escape', 'ignore')
    except Exception, err:
        print 'parse_google_json_result excepion: %s' % err
        return None
    #real_result = "<html><head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf8\" /></head><body>" + real_result + "</body></html>"
    global google_site_cnt
    google_site_cnt += 1
    #open('%d.html' % google_site_cnt, 'w').write(result_str)
    return real_result

def parse_weibo_json_result(html):
    regex = re.compile(r'<script>STK && STK.pageletM && STK.pageletM.view\(({"pid":"pl_weibo_direct"[\s\S]*?)\)</script>')
    try:
        m = regex.search(html)
        result_str = m.group(1)
        json_Dict = json.loads(result_str)
        if json_Dict.has_key('html'):
            html_value = json_Dict.get('html')
        #result_str = "<html><head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" /></head><body>" + html_value + "</body></html>"        
        #open('result_str.htm','w').write(result_str)
        return result_str
    except Exception, err:
        print 'get the regex result error:%s' % err
        return None

def uniform_web_content(url, html):
    reload(sys)
    sys.setdefaultencoding('utf-8')
    colums = url.split('/')
    site_name =  colums[2]
    html_str = html
    if html_str.find('charset=GBK')>0 or html_str.find('charset=gbk')>0:
        html_str = html.decode('gbk','ignore').encode('utf8')
    elif html_str.find('charset=GB2312')>0 or html_str.find('charset=gb2312')>0:
        html_str = html.decode('gb2312','ignore').encode('utf8')
    if site_name == 'www.google.com.hk':
        html_str = parse_google_json_result(html)
    elif site_name == 's.weibo.com':
        html_str = parse_weibo_json_result(html)
    html_parser = HTMLParser.HTMLParser()
    html_str = html_parser.unescape(html_str.decode('utf8','ignore'))
    html_str = "<html><head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" /></head><body>" + html_str + "</body></html>"
    return html_str.encode('utf-8','ignore')

#if __name__ == "__main__":
#    request = urllib2.Request('https://www.google.com.hk/search?q=facebook+search+api&safe=strict&start=20&bav=on.2,or.&fp=1&bvm=pv.xjs.s.zh_CN.HkgQet7ehvY.O&tch=1&ech=1&psi=MwsJVM7-JMrj8AWx9ILYDg.1409878660249.3')
#    request.add_header('Cookie', 'PREF=ID=6103bc19478dfe09:FF=1:LD=zh-CN:TM=1409879573:LM=1409879573:S=mhDFYW5G5aJJNa-G; NID=67=ItALmtNscOF3oTZxJUXsh-KSC8cDaDWfhc2vhw9AhyPHzbVK8Z9WNjCYtIDdHeUbYF_3viy7VBo_zWcBEvmcA_9cG-hNO0wHk_ro8ZcpnIG2-0piyxAYjXRhjVEP1szM')
#    request.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 6.1; rv:31.0) Gecko/20100101 Firefox/31.0')
#    html = urllib2.urlopen(request).read()
#    open('test1.html', 'w').write(parse_google_json_result(html))
