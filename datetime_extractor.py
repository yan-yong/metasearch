# -*- encoding: utf-8 -*-
import re, time, datetime, sys
from common import log_error

class DateTimeExtractor:
    abosolute_regex_str = r'(?:.*?(?:(\d{2,4})(?: |-|/|年))? ?(\d{1,2})(?: |-|/|月) ?(\d{1,2})(?: |-|/|日|号)?)?(?:.*?([0-9]{1,2})(?::|时|点) ?(?:([0-9]{1,2}) ?(?:|:|分|分钟))? ?(?:([0-9]{1,2})(?:|秒钟|秒)?)?)?'
    relative_regex_str  = r'(?:(\d{1,2})(?: ?年| ?years) *?(?:之前|前|ago))|(?:(\d{1,2})(?: ?天| ?日| ?days| ?day) *?(?:之前|前|ago))|(?:(\d{1,2})(?: ?小时| ?时| ?hours) *?(?:之前|前|ago))|(?:(\d{1,2})(?: ?分| ?分钟| minutes) *?(?:之前|前|ago))|(?:(\d{1,2})(?: ?秒| ?秒钟| ?seconds) *(?:之前|前|ago))|(?:昨天 ?(\d+:\d+))'
    timestamp_regex_str = r'\D*?([12]\d{9})\D*?'
    __month_lst =  ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    __seperate_flag = ['  ', ' ', ',', ' ', '\t', '\r', '\n', ' ', '-', '—', ';', '；', ',', '，',':', '：']
    full_cmp_max_text_length = 100
    def __init__(self):
        self.m_abosolute_pat = re.compile(self.abosolute_regex_str)
        self.m_relative_pat = re.compile(self.relative_regex_str, re.IGNORECASE)
        self.m_timestamp_pat = re.compile(self.timestamp_regex_str)
    def __format(self, time_stamp, tm_format):
        time_str = ''
        if tm_format is None or len(tm_format) == 0:
            return time_stamp
        try:
            time_str = time.strftime(tm_format, time.localtime(time_stamp))
        except Exception, err:
            log_error('DateTimeExtractor::__format err %s %s' % (err,str(time_stamp)))
        return time_str
    def __head_seperate(self, text, beg_pos):
        if beg_pos == 0:
            return True
        if beg_pos < 0 or beg_pos > len(text):
            return False
        for flag in self.__seperate_flag:
            if beg_pos >= len(flag) and text[beg_pos - len(flag):beg_pos] == flag:
                return True
        return False
    def __after_seperate(self, text, end_pos):
        if end_pos == len(text):
            return True
        if end_pos < 0 or end_pos > len(text):
            return False
        for flag in self.__seperate_flag:
            if end_pos + len(flag) <= len(text) and text[end_pos:end_pos+len(flag)] == flag:
                return True
        return False
    '''判断前后是否有分隔符'''
    def __invalid_date(self, text, beg_pos, end_pos):
        return self.__head_seperate(text, beg_pos) and self.__after_seperate(text, end_pos)
    def __extract_first(self, text, tm_format, need_seperate):
        cur_time = time.time()
        cur_tm = time.localtime()
        text = text.replace('\r','').replace('\n','').replace('\r\n','')
        #text1 = text.encode('utf-8')
        #res1 = re.search(self.m_abosolute_pat, text1)
        if text.find('.cn/')> 0 or text.find('.com/')> 0:
            if text.count('/')>2:
                text = text.replace('/','*')
        res = re.search(self.m_abosolute_pat, text)
        accuracy = 0
        '''抽绝对时间'''
        if res is not None:
            beg_pos = len(text)
            end_pos = 0
            tm_lst = [cur_tm.tm_year, cur_tm.tm_mon, cur_tm.tm_mday, \
                  cur_tm.tm_hour, cur_tm.tm_min, cur_tm.tm_sec]     
            extract_field_cnt = 0
            for idx in range(len(tm_lst)):
                try:
                    tm_lst[idx] = int(res.group(idx + 1))
                    accuracy += 10
                    extract_field_cnt += 1
                    if res.span(idx + 1)[0] < beg_pos:
                        beg_pos = res.span(idx + 1)[0]
                except:
                    pass
            end_pos = res.span(0)[1]
            if extract_field_cnt > 1 and (not need_seperate or self.__invalid_date(text, beg_pos, end_pos)):
                if tm_lst[0] < 100 and tm_lst[0] >= 70:
                    tm_lst[0] += 1000
                elif tm_lst[0] < 70:
                    tm_lst[0] += 2000
                try:
                    if text.find('昨天')>0 and res.group(1)is None:
                        tm_lst[2]= tm_lst[2]-1
                        accuracy += 30
                    timestamp = time.mktime(datetime.datetime(tm_lst[0], tm_lst[1], tm_lst[2], tm_lst[3], tm_lst[4], tm_lst[5]).timetuple())
                    if timestamp <= cur_time:
                        return accuracy, self.__format(timestamp, tm_format), (beg_pos, end_pos)
                except:
                    pass
        '''抽相对时间'''
        res = re.search(self.m_relative_pat, text)
        timestamp = cur_time
        if res is not None and (not need_seperate or self.__invalid_date(text, res.span()[0], res.span()[1])):
            accuracy = 30          
            if res.group(2) is not None and len(res.group(2)) and res.group(2).isdigit() > 0:
                timestamp -= int(res.group(2))*86400
            if res.group(3) is not None and len(res.group(3)) > 0 and res.group(3).isdigit() > 0:
                timestamp -= int(res.group(3))*3600
                accuracy += 10  
            if res.group(4) is not None and len(res.group(4)) > 0 and res.group(4).isdigit() > 0:
                timestamp -= int(res.group(4))*60
                accuracy += 20  
            if res.group(5) is not None and len(res.group(5)) and res.group(5).isdigit() > 0:
                timestamp -= int(res.group(5))
                accuracy += 30  
            if res.group(6) is not None and len(res.group(6)):
                str_time = res.group(6)
                hour = int(str_time.split(':')[0])
                mint = int(str_time.split(':')[1])
                tm_lst = [cur_tm.tm_year, cur_tm.tm_mon, cur_tm.tm_mday-1,hour,mint,0]
                timestamp = time.mktime(datetime.datetime(tm_lst[0], tm_lst[1], tm_lst[2], tm_lst[3], tm_lst[4], tm_lst[5]).timetuple())
                accuracy += 20  
            if res.group(1) is not None and len(res.group(1)) and res.group(1).isdigit() > 0:
                accuracy += 10
                timestamp -= int(res.group(1))*86400*365
            if timestamp <= cur_time:
                return accuracy, self.__format(timestamp, tm_format), res.span()
        '''抽时间戳'''             
        res = re.search(self.m_timestamp_pat, text)
        if res is not None and res.group(0).isdigit():
            accuracy = 10
            timestamp = int(res.group(0))
            if timestamp <= cur_time:
                return accuracy, self.__format(timestamp, tm_format), res.span()
        '''英文时间'''
        tm_lst = [cur_tm.tm_year, cur_tm.tm_mon, cur_tm.tm_mday, \
                  cur_tm.tm_hour, cur_tm.tm_min, cur_tm.tm_sec]
        for index, month in enumerate(self.__month_lst):
            cur_pos = text.find(month)
            if cur_pos < 0:
                continue
            cur_text = text[cur_pos:]
            cur_month = int(index)+1
            #如果解析月份大于当前月份,则年减一
            if cur_month > tm_lst[1]:
                tm_lst[1] -= 1
            tm_lst[1] = cur_month
            accuracy += 10
            date_regex_str = r'.*? ?(\d{1,2})(?:.*? ?(\d{2,4}))?'
            date_regex_pat = re.compile(date_regex_str)
            res = re.search(date_regex_pat, cur_text)
            try:
                 if res.group(2) is not None and len(res.group(2)) and res.group(2).isdigit() > 0:
                    tm_lst[0] = int(res.group(2))
                    accuracy += 10
                 if res.group(1) is not None and len(res.group(1)) and res.group(1).isdigit() > 0:
                     tm_lst[2] = int(res.group(1))
                     accuracy += 10
            except:
                pass
            try:
                timestamp = time.mktime(datetime.datetime(tm_lst[0], tm_lst[1], tm_lst[2], tm_lst[3], tm_lst[4], tm_lst[5]).timetuple())
                if timestamp <= cur_time:
                    return accuracy, self.__format(timestamp, tm_format), res.span()
            except:
                pass
        return 0, None, (0, len(text))
    '''full_extract表示提取文本text里的所有时间，考虑到当文本比较大时，正则匹配会比较耗时'''
    '''full_extract为False时，只会匹配text的头部和尾部，这两部分出现时间的概率较大'''
    '''need_seperate为True时，时间段前后必须有分隔符才行'''
    def extract(self, text, tm_format='%Y-%m-%d %H:%M:%S', full_extract = False, need_seperate = False):
        #text = text_val.strip(' ').strip('\t').strip('\n').strip('\r').strip(' ').strip('\t')
        if full_extract or len(text) < self.full_cmp_max_text_length:
            return self.__extract_first(text, tm_format, need_seperate)
        prefix_text = text[0:self.full_cmp_max_text_length]
        prefix_accuracy, prefix_val, prefix_span = self.__extract_first(prefix_text, tm_format, need_seperate)
        '''防止被截断'''
        if prefix_accuracy > 0 and prefix_span[1] == self.full_cmp_max_text_length:
            prefix_accuracy = 0
        after_text = text[-self.full_cmp_max_text_length:]
        after_accuracy, after_val, after_span = self.__extract_first(after_text, tm_format, need_seperate)
        if prefix_accuracy < after_accuracy:
            base_pos = len(text) - self.full_cmp_max_text_length
            beg_pos = base_pos + after_span[0]
            end_pos = base_pos + after_span[1]
            return after_accuracy, after_val, (beg_pos, end_pos)
        return prefix_accuracy, prefix_val, prefix_span
    def extract_lst(self, text_lst):
        final_accuracy = 0
        final_date = ''
        node = None
        pos_span = 0
        for item in text_lst:
            cur_node = item[0]
            cur_text = item[1]
            cur_accuracy, cur_date, pos_span = self.extract(cur_text)
            if cur_accuracy > final_accuracy:
                final_accuracy = cur_accuracy
                final_date = cur_date
                node = cur_node
        #print final_accuracy, final_date
        return final_accuracy, final_date, node, pos_span
if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    date_time = DateTimeExtractor()
    print date_time.extract('Feb 04, 2015 · 台湾复兴航空“2·4”空难已造成31人罹难：')
    print date_time.extract('10条回复 - 发帖时间: 2010年8月17日  12:34:20')
    print date_time.extract('10条回复 - 发帖时间: 2010年8月18日 ')
    print date_time.extract('10条回复 - 发帖时间: 12:34:20')
    print date_time.extract('10条回复 - 发帖时间: 2010-8-17  12:34:20')
    print date_time.extract('10条回复 - 发帖时间 2010年8月17号 11点34分10秒')
    print date_time.extract('10条回复 - 发帖时间  13年08月12 11点24分10')
    print date_time.extract('10条回复 - 发帖时间 :5秒钟之前 的帖子')
    print date_time.extract('10条回复 - 发帖时间: 25分钟前')
    print date_time.extract('10条回复 - 发帖时间: 2天前')
    print date_time.extract('10条回复 - 发帖时间: 2年前')
    print date_time.extract('10条回复 - 发帖时间: 3 hours ago')
    print date_time.extract('2天前我过一个帖子 发帖时间: 2010年8月17日  12:34:20')
    print date_time.extract('问：为何search只匹配到一项1419338605，而不是所有匹配的项？ 答：因为search本身的功能就是: 从左到右，去计算是否匹配，如果有匹配，就返回。\
                     即只要找到匹配，就返回了。 所以，最多只会匹配一个， 而不会匹配多个。 想要匹配多个，请去使用re.findall 2010/8/17 12:34:20')
    print date_time.extract('<script>div.write("1419338605")</script>')
    print date_time.extract('10条回复 - 发帖5秒钟之前 的帖子')
