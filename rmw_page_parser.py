# -*-: encoding: utf-8 -*-
from common import *
import json,time
import os, sys, re
from xml.etree import ElementTree

class RmwPageParser():
    '''def start(self, url, page_content):
        self.m_url = url
        self.m_page_content = page_content'''
    def get_parse_result(self,url, page_content, res_header = None):
        reload(sys)
        sys.setdefaultencoding('utf-8')
        #open('2_web_%d.html'%int(time.time()),'w').write(page_content)
        result_lst = []
        try:
            root = ElementTree.fromstring(page_content)
            result_lst = root.findall('RESULT')
        except Exception,err:
            pass
        if result_lst is None or len(result_lst) == 0:
            log_error('RmwPageParser::parser no result: %s ' % url)
            return None
        parser_result_lst = []
        for result in result_lst:
            result_dict = {}
            title = ''
            link = ''
            summary =''
            source = ''
            date =''
            #title
            try:
                title_node = result.find('TITLE')
                title = title_node.text
                title = re.sub('<[\s\S]*?>','',title)
            except Exception,err:
                print 'TITLE', err
                pass
            #print '$$$%s$$$' % title
            result_dict['title'] = title
            '''#content
            try:
                summary_node = result.find('CONTENT')
                summary = my_strip(summary_node.text)
                summary = re.sub('<[\s\S]*?>','', summary)
            except Exception,err:
                pass
            print '$$$%s$$$' % summary
            result_dict['summary'] = summary'''
            #date
            date_str = ''
            try:
                pubdate_node = result.find('PUBLISHTIME')
                date = pubdate_node.text
                date = date.replace('年','-').replace('月','-').replace('日',' ').replace('时',':').replace('分',':').replace('秒','')
                time_int = time.mktime(time.strptime(date,'%Y-%m-%d %H:%M:%S'))
                date_str = time.strftime('%Y-%m-%d %H:%M',time.localtime(time_int))
            except Exception,err:
                #print err
                pass
            #print 'date_str',date_str
            result_dict['date'] = date_str
            #source
            try:
                source_node = result.find('SOURCE')
                source = source_node.text
            except Exception,err:
                pass
            result_dict['source'] = source
            #chinnel
            try:
                channel_node = result.find('CHANNEL')
                channel = channel_node.text
            except Exception,err:
                pass
            result_dict['channel'] = channel
            #url
            try:
                link_node = result.find('DOCURL')
                link = link_node.text
            except Exception,err:
                pass
            result_dict['link'] = link
            #print '$$$%s$$$' % link
            if len(link)<4:
                #log_error('find no link: %s' % url)
                continue
            parser_result_lst.append(json.dumps(result_dict))
        log_info('RmwPageParser::parseed %d result in %s ' % (len(parser_result_lst), url))
        return parser_result_lst
if __name__ == "__main__":
    s = '<?xml version="1.0" encoding="UTF-8" ?>\
        <RMW>\
            <RESULT>\
                <TITLE><![CDATA[四川阿坝新发现野生大熊猫活动痕迹]]></TITLE>\
                <AUTHOR><![CDATA[]]></AUTHOR>\
                <CONTENT><![CDATA[&nbsp;&nbsp;&nbsp;...强对此区域的重点巡护监测力度，]]></CONTENT>\
                <PUBLISHTIME><![CDATA[2015年5月17日0时11分32秒]]></PUBLISHTIME>\
                <SOURCE><![CDATA[光明网]]></SOURCE>\
                <DOCURL><![CDATA[http://hb.people.com.cn/n/2015/0517/c192237-24887439.html]]></DOCURL>\
                <CHANNEL><![CDATA[湖北频道]]></CHANNEL>\
                <DOCSIZE><![CDATA[2]]></DOCSIZE>\
                <RELEVANCE><![CDATA[0.0]]></RELEVANCE>\
            </RESULT>\
            <RESULT>\
                <TITLE><![CDATA[四川景区5.19中国旅游日优惠活动最全名单出炉]]></TITLE>\
                <AUTHOR><![CDATA[]]></AUTHOR>\
                <CONTENT><![CDATA[&nbsp;&nbsp;&nbsp;...川特别旅游区映秀爱立方景点半价开放；映秀地震，]]></CONTENT>\
                <PUBLISHTIME><![CDATA[2015年5月15日9时4分3秒]]></PUBLISHTIME>\
                <SOURCE><![CDATA[中国网]]></SOURCE>\
                <DOCURL><![CDATA[http://sc.people.com.cn/n/2015/0515/c345167-24869251.html]]></DOCURL>\
                <CHANNEL><![CDATA[人民网四川频道]]></CHANNEL>\
                <DOCSIZE><![CDATA[7]]></DOCSIZE>\
                <RELEVANCE><![CDATA[0.0]]></RELEVANCE>\
            </RESULT>\
            <CURPAGE><![CDATA[1]]></CURPAGE>\
            <PAGES><![CDATA[108]]></PAGES>\
            <PAGECOUNT><![CDATA[20]]></PAGECOUNT>\
            <TOTALCOUNT><![CDATA[2146]]></TOTALCOUNT>\
            <SEARCHTIME><![CDATA[523]]></SEARCHTIME>\
        </RMW>'
    parser = RmwPageParser()
    url = 'http://search.people.com.cn/rmw/GB/rmwsearch/gj_search_pd.jsp#'
    result_lst = parser.get_parse_result(url, s)
