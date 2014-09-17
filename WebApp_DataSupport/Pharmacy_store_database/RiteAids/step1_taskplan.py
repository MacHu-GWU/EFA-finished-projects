##coding=utf8

''' THIS IS THE SCRIPT TO CRAWL RITEAID STORE LOCATION AND DETAIL INFORMATION
'''
from LinearSpider.crawler import Crawler, Taskplanner
from LinearSpider.jt import *
import bs4
import re, pprint
import jsontree
import itertools
'''
第一级，入口页面，内容是所有的rite aid的商店的url
    https://www.riteaid.com/store-site-map
    
第二季，rite aid商店，内容是具体信息
    https://www.riteaid.com/store-details?storeNumber=01140
'''
    
def step1_taskplan():
    '''设定函数内常量'''
    spider = Crawler()
    TP = Taskplanner()
    base_url = 'https://www.riteaid.com'
    entrance_url = 'https://www.riteaid.com/store-site-map'
    
    TP.todo.setdefault(entrance_url, {'data': None} ) # 给下一步预设空间的行为发生在当前页面爬完的情况下
    
    html = spider.html(entrance_url) # 开始爬
    if html:
        soup = bs4.BeautifulSoup(html)
        for a in soup.findAll(href = re.compile(r'https://www.riteaid.com/store-details\?storeNumber=\d*')):
            TP.todo[entrance_url].setdefault( a['href'],
                                              {'data': a.text} )

    TP._dump_todo('riteaid-task.json', replace = True)


def validate(phone, hours, additional_info, detail): # 下
    if len(hours) == 4: # phone 必须有14位长，例如(202)-001-1234；hour 必须有 Mon-Thur, Fri, Sat, Sun 四项，
#     if (len(phone) == 14) & (len(hours) == 4):
        
        return True
    else:
        return False
    
def step2_download():
    spider = Crawler()
    TP = Taskplanner()
    TP._load_todo('riteaid_task.json')
    base_url = 'https://www.riteaid.com'
    entrance_url = 'https://www.riteaid.com/store-site-map'
    

    riteaid = load_jt('riteaid_data.json')
        
    counter = itertools.count(0)
    for store_url in ignore_iterkeys(TP.todo[entrance_url] ):
        ## 首先处理随着url一块传入的reference data
        text = TP.todo[entrance_url][store_url]['data']
        storeID, address = text.split(',', 1)
        storeID, address = storeID.strip(), address.strip()
        ## 然后处理每个url页面
        if storeID not in riteaid: # 如果没有爬过
            html = spider.html(store_url)
            if html:
                try:
                    soup = bs4.BeautifulSoup(html)
                    
                    ''' phone number '''
                    phone = ''
                    for p in soup.findAll('p', attrs = {'class', 'padding-phone'}):
                        phone = p.text.replace(p.strong.text, '').strip().replace(' ', '-') # process Phone
                
                    ''' hour '''
                    hours = list()
                    for ul in soup.findAll('ul', attrs = {'class', 'days'}):
                        hours.append( ul.text.split() ) # process Office Hour
                    
                    ''' additional information '''
                    additional_info = list()
                    for div in soup.findAll('div', attrs = {'id': 'eventListId'}):
                        for li in div.findAll('li'):
                            additional_info.append( li.text ) # process Additional Information
                    
                    ''' store detail '''
                    detail = {}
                    for div in soup.findAll('div', attrs = {'class': 'storeDetailsAttributeCategory'}):
                        storeDetailsAttributeCategory = div.strong.text.strip()
                        detail.setdefault(storeDetailsAttributeCategory, list())
                        for subdiv in div.findAll('div', attrs = {'class': 'storeDetailsAttribute'}):
                            detail[storeDetailsAttributeCategory].append(subdiv.text.strip()) # process Store Detail
                    
                    ## validate the information I crawled
                    if validate(phone, hours, additional_info, detail): # <=== validate, sometime error
                        print "CORRECT"
                        riteaid.setdefault(storeID, 
                                           {'address': address,
                                            'phone': phone,
                                            'hours': hours,
                                            'additional_info': additional_info,
                                            'detail': detail} )
                        dump_jt(riteaid, 'riteaid_data.json', replace = True)
                        print storeID, counter.next() ## 只统计正确的
                    else:
                        print "ERROR!", (phone, hours, additional_info, detail)
                        print "\t%s" % store_url
                        print '%s.html' % (store_url[-5:],)
                        with open('%s.html' % store_url[-5:], 'wb') as f:
                            f.write(html)
                except:
                    pass
                
def unit_test():
    pass
            
if __name__ == '__main__':
#     step1_taskplan() # 先执行taskplan
    step2_download()
#     unit_test()

