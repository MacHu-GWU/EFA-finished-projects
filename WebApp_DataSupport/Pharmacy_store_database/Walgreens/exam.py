##encoding=utf8

from HSH.Data.jt import *
from itertools import count


def exam1(): # 8190 store in total
    '''Find how many store walgreens has over US
    '''
    task = load_jt('walgreens_task.json')
    c = 0
    for entrance, v in ignore_iteritems(task):
        for state_url, v1 in ignore_iteritems(v):
            for city_url, v2 in ignore_iteritems(v1):
                for store_url, v3 in ignore_iteritems(v2):
                    c += 1
    print c
    
# exam1()

def exam2(): # 8186, 4 stores link crashed
    '''Find how many store we have crawled
    '''
    walgreens = load_jt(r'walgreens_data.json')
    print len(walgreens)
    
# exam2()

def exam3_hours():
    '''
    结论：
    len(hours)有4种情况: 0, 1, 2, 3
        0 - 全close
        clinic, store, pharmacy 或开或不开
    每个商店的开门时间的格式也有4种情况: 0, 2, 3, 7
        0 - 1-7全部关门
        2 - 出错了，就设置为7天都是24小时
        3 - 普通情况 1-5, 6, 7 三种格式: '10AM-6PM', CLOSED, Open 24 hours
        7 - 1-7都有详细时间
    '''
    walgreens = load_jt(r'walgreens_data.json')
    len_hour = set()
    for id, v in walgreens.iteritems():
        hours = v['hours']
        for types, v1 in hours.iteritems():
            if len(v1) == 7:
                print id, v1
    
    
exam3_hours()