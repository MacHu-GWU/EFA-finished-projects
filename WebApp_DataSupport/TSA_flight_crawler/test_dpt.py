##encoding=utf8
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.select import Select

from HSH.LinearSpider.logger import Log
from HSH.Data.jt import *
from FlightCrawler.task_optimizer import Task_optimizer, dt_interval_generator

import time
import sys
import datetime
import os

reload(sys); # change the system default encoding = utf-8
eval('sys.setdefaultencoding("utf-8")')

def sleep(n):
    time.sleep(n)
    
def process_time_interval(text):
    '''
    INPUT = "12:00AM - 4:00PM"
    OUTPUT = "00-16"
    '''
    st, et = text.split('-')
    st, et = st.strip(), et.strip()
    st, et = datetime.datetime.strptime(st, '%I:00%p').hour, datetime.datetime.strptime(et, '%I:00%p').hour
    return '%s-%s' % (str(st).zfill(2), str(et).zfill(2))

def crawl_by_airport_date(airport, date):
    
    path = r'departure' 
    
    driver = webdriver.Firefox()
    driver.set_page_load_timeout(30)
    while 1:
        print 'Trying to crawl %s in %s' % (airport, date) #
        try:
            driver.get('http://www.flightstats.com/go/FlightStatus/flightStatusByAirport.do?airportQueryType=2') # departures
            sleep(1)
            ## fill the form
            element = driver.find_element_by_id('fsByAirportDepartureDateTextField') # enter date
            element.clear()
            element.send_keys(date)
             
            element = driver.find_element_by_id('fsAirport') # enter airport code
            element.clear()
            element.send_keys(airport)
            
            element.send_keys(Keys.RETURN) # send request
            sleep(1)
            print '  Enter airport, date success.'
            break
        except:
            print '  Failed to enter airport, date, try again...'
            pass

    ## crawl query result by airport and date
    try: # download page with different time option
        select = Select(driver.find_element_by_xpath("//div[@class='uiComponent674']//select[@id='airportQueryTime']"))
        options = [element.text for element in select.options] # get time period options
        topt.add(date, airport) # 只要页面读出来，就算爬过了
        print "    It's a mutiple option page"
        for option in options:
            fname = '%s %s %s.html' % (date, airport, process_time_interval(option))
            if not os.path.exists(os.path.join(path, fname)):
                print '    selecting option = %s' % option
                select = Select(driver.find_element_by_xpath("//div[@class='uiComponent674']//select[@id='airportQueryTime']"))
                select.select_by_visible_text(option)
                html = driver.page_source # 可能出错
                if sys.getsizeof(html) >= 10000: # 说明html工作正常
                    with open(os.path.join(path, fname), 'wb') as f:
                        f.write(html)
                    topt._dump()
                    print '      SUCCESS! %s %s %s %s' % (date, airport, option, datetime.datetime.now())
                else:
                    log.write('      departures bad html', '%s %s %s' % (date, airport, option))
    except:
        print "    It's a 24 Hour option page"
        try:
            option = '00-24'
            fname = '%s %s %s.html' % (date, airport, option)
            topt.add(date, airport) # 只要页面读出来，就算爬过了
            if not os.path.exists(os.path.join(path, fname)):
                html = driver.page_source # 可能出错
                if sys.getsizeof(html) >= 10000: # 说明html工作正常
                    with open(os.path.join(path, fname), 'wb') as f:
                        f.write(html)
                    topt._dump()
                    print '      SUCCESS! %s %s %s %s' % (date, airport, option, datetime.datetime.now())

                else:
                    log.write('      departures bad html', '%s %s %s' % (date, airport, option))
        except:
            print '      FAILED to get page sources.'
            pass

    driver.close()
    
if __name__ == '__main__':
    log = Log()
    topt = Task_optimizer(r'reference/topt_dpt.json') ## initial departures task optimizer
##    for date in dt_interval_generator(datetime.datetime.strftime( datetime.datetime.now(), '%Y-%m-%d'),
##                                      datetime.datetime.strftime( datetime.datetime.now() + datetime.timedelta(2), '%Y-%m-%d' )  ): 
    for date in dt_interval_generator('2014-09-16', '2014-09-17'): 
        for airport in topt.opt_list(date):
            crawl_by_airport_date(airport = airport, date = date)