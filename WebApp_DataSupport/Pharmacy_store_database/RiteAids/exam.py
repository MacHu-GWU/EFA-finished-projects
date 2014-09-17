from LinearSpider.jt import *

def exam1(): # 4513 store in total
    '''Find how many store riteaid has over US
    '''
    task = load_jt('riteaid_task.json')
    for value in task.itervalues():
        print len(value) - 1
    
exam1()

def exam2(): ## 4496 = good record, 17 crashed link
    '''Find how many store we have crawled
    '''
    riteaid = load_jt(r'riteaid_data.json')
    print len(riteaid)
    
exam2()