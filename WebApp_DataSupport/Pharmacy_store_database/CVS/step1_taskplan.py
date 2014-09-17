##coding=utf8

from LinearSpider.crawler import Crawler, Taskplanner
from LinearSpider.jt import *
from bs4 import BeautifulSoup as BS4
import re


base_url = 'http://www.cvs.com'
spider = Crawler()
task_fname = 'CVS_task.json'

def s1_state_layer():
    '''Crawl all state_url
    '''
    task = load_jt(task_fname)
    entrance_url = 'http://www.cvs.com/stores/cvs-pharmacy-locations'
    html = spider.html(entrance_url, timeout = 10)
    if html:
        soup = BS4(html)
        for a in soup.find_all('a', href = re.compile(r'/stores/cvs-pharmacy-locations[\s\S]*')):
            state, state_url = a.text.strip(), base_url + a['href']
            task[state_url] = {'data': {'state_name': state,
                                        'href_pattern': a['href']} }
    dump_jt(task, task_fname,replace = True)
    
# s1_state_layer()

def s2_city_layer():
    '''Crawl all city_url
    '''
    task = load_jt(task_fname)
    for state_url in ignore_iterkeys(task):
        pattern = task[state_url]['data']['href_pattern']
        html = spider.html(state_url)
        if html:
            soup = BS4(html)
            for a in soup.find_all('a', href = re.compile(r'%s[\s\S]*' % pattern)):
                
                task[state_url].setdefault(base_url + a['href'],
                                           {'data': {'number_of_store': int(re.findall(r'(?<=\()\d*(?=\))', a.text.strip())[0] ) } } ) # city下有几间店
            dump_jt(task, task_fname, replace = True)
        else:
            print '\t失败！ %s' % state_url

# s2_city_layer()

def s3_store_layer():
    '''Crawl all store_url under city
    '''
    task = load_jt(task_fname)
    for state_url in ignore_iterkeys(task):
        state = task[state_url]['data']['state_name']
        for city_url in ignore_iterkeys(task[state_url]):
            if len( task[state_url][city_url] ) == 1:
                print city_url
                html = spider.html(city_url)
                if html:
                    soup = BS4(html)
                    for tr in soup.find_all('tr', _class = ""):
                        if len( tr.find_all('td') ) == 4: 
                            td1, td2, td3, _ = tr.find_all('td')
                            store_url, store_id, address, phone = base_url + td1.a['href'], td1.text.strip(), td2.text.strip(), td3.text.strip()
                            task[state_url][city_url].setdefault(store_url,
                                                                 {'data': {'store_id': store_id,
                                                                           'address': address,
                                                                           'phone': phone} } )
                else:
                    print '\t失败！ %s' % city_url
                    
        dump_jt(task, task_fname, replace = True)
        print '%s COMPLETE! %s' % (state_url, state)
    
# s3_store_layer()