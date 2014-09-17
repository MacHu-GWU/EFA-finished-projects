##coding=utf8

from LinearSpider.crawler import Crawler, Taskplanner
from LinearSpider.jt import *
from bs4 import BeautifulSoup as BS4
import re
from collections import deque


spider = Crawler()

def extract(store_url):
    def process_hour_string(text):
        '''
        M-F 07:00 AM - 10:00 PM
        Sat 07:00 AM - 10:00 PM
        Sun 07:00 AM - 10:00 PM
        '''
        res = list()
        for line in text.split('\n'):
            if line.strip() != '':
                res.append( line.strip() )
        result = list()
        for piece in ' '.join(res).split():
            if piece.strip() != '':
                result.append( piece.strip() )
        return ' '.join(result)
    
    html = spider.html(store_url)
    if html:
        soup = BS4(html)
        ## 解析 hours
        HOURS = dict()
        for div in soup.find_all('div', attrs = {'class': 'hours'}):
            ## 检测有多少种不同的hour (store, pharmacy, clinic)
            hour_type = list()
            for h4 in div.find_all('h4'):
                hour_type.append( h4.text.strip() )
            hour_type = deque(hour_type)
            ## 处理每种类型的hour
            for ul in div.findAll('ul'):
                hour_subtype = hour_type.popleft()
                HOURS.setdefault(hour_subtype, list() )
                hour_time = list()
                for li in ul.findAll('li'):
                    raw_text = li.text.strip()
                    HOURS[hour_subtype].append( process_hour_string(raw_text) )
                    
        # 解析 service
        service = list()
        for p in soup.findAll('p', attrs = {'class': 'nomargin'}):
            service.append(p.text.strip())
        
        return HOURS, service
    
def main():
    task_fname = 'CVS_task.json'
    task = load_jt(task_fname)
    CVS = load_jt('CVS_data.json')
    for state_url, v in ignore_iteritems(task):
        for city_url, v1 in ignore_iteritems(v):
            for store_url, v2 in ignore_iteritems(v1):
                address, phone, store_id = v2['data']['address'], v2['data']['phone'], v2['data']['store_id']
                if store_id not in CVS:
                    print 'CRAWLING: %s' % store_url
                    try:
                        HOURS, service = extract(store_url)
                        CVS.setdefault(store_id, {'address': address,
                                                  'phone': phone,
                                                  'hours': HOURS,
                                                  'service': service})
                        print '\t SUCCESS!'
                    except:
                        print '\t FAILED!'
        dump_jt(CVS, 'CVS_data.json', replace = True)
                
                
main()