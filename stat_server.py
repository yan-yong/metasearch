#encoding: utf-8
from common import *
import os, sys, platform
import posixpath
import BaseHTTPServer
import threading
import urllib
import cgi
import shutil
import mimetypes
import re
import time

class HostStat:
    def __init__(self):
        self.m_data= {}
        self.m_cur_hour = None
        self.m_cur_day  = None
    def add_stat(self, host, keyword, new_link_count, dup_link_count):
        host_stat = self.m_data.get(host)
        if host_stat is None:
            host_stat = {}
            self.m_data[host] = host_stat
        keyword_stat = host_stat.get(keyword)
        if keyword_stat is None:
            '''(hour_hub_page_count, hour_new_link_count, hour_dup_link_count,
                day_hub_page_count, day_new_link_count, day_dup_link_count )'''
            keyword_stat = [0, 0, 0, 0, 0, 0]
            host_stat[keyword] = keyword_stat
        cur_hour = time.strftime('%H',time.localtime(time.time()))
        cur_day  = time.strftime('%d',time.localtime(time.time()))
        if cur_hour != self.m_cur_hour:
            keyword_stat[0] = keyword_stat[1] = keyword_stat[2] = 0
            self.m_cur_hour = cur_hour
        if cur_day != self.m_cur_day:
            keyword_stat[3] = keyword_stat[4] = keyword_stat[5] = 0
            self.m_cur_day = cur_day
        keyword_stat[0] += 1
        keyword_stat[1] += new_link_count
        keyword_stat[2] += dup_link_count
        keyword_stat[3] += 1
        keyword_stat[4] += new_link_count
        keyword_stat[5] += dup_link_count
    def host_html_format(self, host):
        host_stat = self.m_data.get(host)
        if host_stat is None:
            log_error('StatHttpHandler cannot find host: %s' % host)
            return '<html><body>StatHttpHandler cannot find host: %s</body></html>' % host
        cont = '<html><meta http-equiv="content-type" content="text/html;charset=utf-8"><body><h1>%s</h1><table border="2" cellpadding="1" cellspacing="1" bordercolor="#000000"> \
                <tr><td>关键词</td><td>小时列表页下载量</td><td>小时新链接发现数目</td><td>小时重复链接发现数目</td> \
                <td>当天列表页下载量</td><td>当天新链接发现数目</td><td>当天重复链接发现数目</td></tr>' % host
        for keyword, stat_data in self.m_data[host].items():
            cont += '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>\n'  % \
                (keyword, stat_data[0], stat_data[1], stat_data[2], stat_data[3], stat_data[4], stat_data[5])
        cont += '</table></body></html>'
        return cont
    def html_format(self):
        cont = '<html><meta http-equiv="content-type" content="text/html;charset=utf-8"><body><table border="2" cellpadding="1" cellspacing="1" bordercolor="#000000"> \
                <tr><td>站点名称</td><td>小时列表页下载量</td><td>小时新链接发现数目</td> <td>小时重复链接发现数目</td>\
                <td>当天列表页下载量</td><td>当天新链接发现数目</td><td>当天重复链接发现数目</td><td>详情</td></tr>'
        print '1111111111111111'
        for host, keyword_dict in self.m_data.items():
            sum_count = [0,0,0,0,0,0]
            for keyword, stat_data in self.m_data[host].items():
                for i in range(len(sum_count)):
                    sum_count[i] += stat_data[i]
            quote_host = urllib.quote_plus(host)
            cont += '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td><a href="/host?q=%s">详情</a></td></tr>\n' % \
                (host, sum_count[0], sum_count[1], sum_count[2], sum_count[3], sum_count[4], sum_count[5], quote_host)        
        cont += '</table></body></html>'
        return cont

host_stat = HostStat()

class StatHttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def response_client(self, content):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)        
    def do_GET(self):
        global host_stat
        prefix_path, query = urllib.splitquery(self.path)
        content = ''
        if prefix_path == '/host':
            cols = query.split('=')
            if len(cols) == 2 and cols[0] == 'q':
                host = urllib.quote_plus(cols[1])
                content = host_stat.host_html_format(host)
                self.response_client(content)
                return 
        content = host_stat.html_format()
        self.response_client(content)

class StatHttpServer:
    def __init__(self):
        self.m_serv = None
        self.m_serv_addr = None
    def start(self, server_addr):
        self.m_serv_addr = server_addr
        self.m_serv = BaseHTTPServer.HTTPServer(server_addr, StatHttpHandler)
        log_info('StatHttpServer start at %s:%d' % (self.m_serv_addr[0], self.m_serv_addr[1]))
        self.m_serv.serve_forever()
    def close(self):
        self.m_serv.server_close()
        log_info('StatHttpServer %s closed.' % self.m_serv_addr)
    def add_stat(self, host, keyword, new_link_count, dup_link_count):
        global host_stat
        host_stat.add_stat(host, keyword, new_link_count, dup_link_count)
        
def main():
    serv = StatHttpServer()
    serv.add_stat('www.baidu.com', 'aaaa', 2, 2)
    serv.add_stat('www.baidu.com', 'aaaa', 10, 10)
    serv.add_stat('www.baidu.com', 'bbbb', 11, 11)
    serv.add_stat('www.sina.com', 'cccc', 15, 15)
    serv.start(("0.0.0.0", 9999))
    
if __name__ == "__main__":
    main()