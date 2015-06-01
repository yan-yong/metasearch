# -*-: encoding: utf-8 -*-

import BeautifulSoup, lxml, lxml.html
import urllib2, copy, re
import traceback
from parser_common import *
from page_parser import *
from common import *
from datetime_extractor import *
from source_extractor import *
from search_page_parser import *

def get_header(info, header):
    lines = info.split('\r\n')
    for line in lines:
        collums = line.split(':')
        if len(collums) != 2 or collums[0].strip(' ') != header.strip(' '):
            continue
        return collums[1].strip(' ')
    return None

class TestPageParser(SearchPageParser):

    def run_page_parser(self, url, html, header=None,has_summary = False):
        #重写此函数解析web页
        self.start(url, html, header,has_summary)
        for json_result,child_str in self.parse_search_page():
            yield json_result 

if __name__ == "__main__":
    stderr = sys.stderr
    stdout = sys.stdout
    reload(sys)
    sys.stderr = stderr
    sys.stdout = stdout
    sys.setdefaultencoding('utf-8')

    input_file_name = 'input1.txt'
    output_file_name = 'output1.txt'
    if len(sys.argv) != 3:
        log_info("need 2 argument: input file name and output file name,use default")
    else:
        input_file_name = sys.argv[1]
        output_file_name = sys.argv[2]
        
    http_headers= { \
    'User-Agent':"Mozilla/5.0 (Windows NT 5.1; rv:32.0) Gecko/20100101 Firefox/32.0", \
    'Accept':"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }
    print sys.path[0]
    f_in = open(input_file_name,'r')
    lines = f_in.readlines()
    f = open(output_file_name,'w')
    for line in lines:
        log_info('start process %s '% line)
        colums = line.strip('\r').strip('\n').strip(' ').split('\t')
        line_id = colums[0]
        url = colums[1]
        print url
        url_host = url.split('/')[2]
        if url_host == 's.weibo.com':
            http_headers['Referer']="http://s.weibo.com/"
            http_headers['Cookie']='YF-V5-G0=00db9c3a9570b0baeaa15e7d99208a6a; SUB=_2A254Dl7kDeTxGeNN6VQX8izIzDyIHXVbejcsrDV8PUNbuNBeLXLwkW9Q-LzoWfAU8tjvwRJ1kX_sd_BCxQ..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5pXkpuIgKb8zwsEfF-AKxF5JpX5K2t; login_sid_t=5ed9ec4255d29cc178cc34b10619b915; YF-Ugrow-G0=ea90f703b7694b74b62d38420b5273df; _s_tentry=-; Apache=4456779861136.972.1426730608609; SINAGLOBAL=4456779861136.972.1426730608609; ULV=1426730608628:1:1:1:4456779861136.972.1426730608609:; YF-Page-G0=b35da6f93109faa87e8c89e98abf1260; SUS=SID-5326622470-1426730676-GZ-2kf9z-c203223d8dae3bf58afc489e83f21285; SUE=es%3D0d403280c9f18cdea709b0b36af70ee3%26ev%3Dv1%26es2%3D1366596c397d5aa12b6d0d6751e7bfe6%26rs0%3DN5sPm%252FWVQTyubJhi24XD2VUVnxk7CsJud%252BRqIOu17XwzibeXBFKqsPl4Ny4sXHqlGSyve1dIQ%252F4d5mtCP8jFKdsAwL%252FX5TceVaWXn1sX1T7r23K3qhjSuEm5tulGArR07tas4PHXxljsoH5fQiCuB7tz4gyg2hsG9iidN0VGa0o%253D%26rv%3D0; SUP=cv%3D1%26bt%3D1426730676%26et%3D1426817076%26d%3Dc909%26i%3D1285%26us%3D1%26vf%3D0%26vt%3D0%26ac%3D0%26st%3D0%26uid%3D5326622470%26name%3D1004650403%2540qq.com%26nick%3D%25E8%2593%259D%25E8%2593%259D%25E8%2593%259D017%26fmp%3D%26lcp%3D2015-01-06%252014%253A34%253A28; SUHB=0ft5nTk3iS88uj; ALF=1458266660; SSOLoginState=1426730676; un=1004650403@qq.com'
            request = urllib2.Request(url, data = '', headers = http_headers)
        elif url_host == 'www.google.com.hk':
            http_headers['User-Agent']="Mozilla/5.0 (Windows NT 6.1; rv:31.0) Gecko/20100101 Firefox/31.0"
            http_headers['Accept-Encoding']="gzip"
            request = urllib2.Request(url, headers = http_headers)
        else:
            request = urllib2.Request(url)
        #html_str = urllib2.urlopen(request).read().decode('utf-8','ignore')
        try:
            response = urllib2.urlopen(request)
            info_str = str(response.info())
            html_str = response.read()
            if html_str and get_header(info_str,'Content-Encoding') == 'gzip':
                html_str = gzip_decompress(html_str)
        except Exception,err:
            log_error('html request error: %s' % err)
            print traceback.format_exc()
        
        log_info('html str len: %d' % len(html_str))

        parser = TestPageParser()
        line_cnt = 0
        #process special html str:weibo,google
        html = webpage_extractor(url, html_str)
        open('aa_%s.html'%line_id, 'w').write(html)
        for result in parser.run_page_parser(url, html):
            try:
                if line_cnt == 0:
                    output_str = '序号\t时间\t来源\t标题\t正文\tURL\tLink'
                    f.write('%s\r'%output_str)
                result_json = json.loads(result)
                if result_json.get('link')=='':
                    continue
                output_str = '%d\t'%line_cnt
                output_str += '%s\t'%result_json.get('date')
                output_str += '%s\t'%result_json.get('source')
                output_str += '%s\t'%result_json.get('title')
                output_str += '%s\t'%result_json.get('summary')
                output_str += '%s\t'%result_json.get('link')
                output_str += '%s'%url
                #print output_str
                f.write('%s\n'%output_str)
                f.flush()
                line_cnt +=1
            except Exception,err:
                print "********%s"%err
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
