from LinearSpider.jt import *

def exam1(): # 7682 store in total
    '''Find how many store CVS has over US
    '''
    task = load_jt('CVS_task.json')
    counter = 0
    for state_url, v in ignore_iteritems(task):
        for city_url, v1 in ignore_iteritems(v):
            for store_url, v2 in ignore_iteritems(v1):
                counter += 1
    print counter
    
exam1()

def exam2(): ## 7679 = good record, 17 crashed link
    '''Find how many store we have crawled
    '''
    cvs = load_jt(r'CVS_data.json')
    print len(cvs)
    
exam2()

def ExamTask_valid_addr_and_phone():
    task = load_jt('CVS_task.json')
    for state_url, v in ignore_iteritems(task):
        for city_url, v1 in ignore_iteritems(v):
            for store_url, v2 in ignore_iteritems(v1):
                    
                addr, phone = v2['data']['address'], v2['data']['phone']
                if (len(phone) != 12):
                    print addr, phone

# ExamTask_valid_addr_and_phone()