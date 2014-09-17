##encoding=utf8
##version =py27
##author  =sanhe
##date    =2014-09-07

from hsh_hashlib import md5_str, md5_obj
from hsh_geopy import dist
from LinearSpider.jt import *

from datetime import datetime
from itertools import count
from GoogleV3_parser import googleV3_location as V3

def parse_cvs(data):
    '''给一个cvs的json数据，返回
    name = 'cvs' 商家名字
    address = 原始，没有经过geocode的地址
    phone = 电话 202-123-4567 十二位格式
            # 时间格式是4位数字，后两位是分钟，前两位是小时，不开门是-1
    hours = store [mon open, mon close, ..., sun open, sun close]
            pharmacy [mon open, mon close, ..., sun open, sun close]
            clinic [mon open, mon close, ..., sun open, sun close]
    '''
    def process_hours(hours):
        '''
        valid store type = [u'MinuteClinic Hours', u'Pharmacy Hours', u'Store & Photo Hours']
        '''
        def process_line(line):
            if len(line) == 17: ## "M-F Open 24 hours"
                return 0, 2400
            elif (21 <= len(line) ) and (len(line) <= 40): # "M-F 08:00 AM - 09:00 PM"
                st, et = line.split(' ')[-5] + ' ' + line.split(' ')[-4], line.split(' ')[-2] + ' ' + line.split(' ')[-1]
                st, et = datetime.strptime( st, '%I:%M %p'), datetime.strptime( et, '%I:%M %p')
                return 100*st.hour + st.minute, 100*et.hour + et.minute
            else: # "M-F Closed"
                return -1, -1
            
        def flatten(array):
            res = list()
            for tup in array:
                for i in tup:
                    res.append(i)
            return res
        
        rule = {'store': 'MinuteClinic Hours',
                'pharmacy': 'Pharmacy Hours',
                'clinic': 'Store & Photo Hours'}
        res = list()
        for k in ['store', 'pharmacy', 'clinic']:
            v = hours.get(rule[k], ['closed', 'closed', 'closed'])
            if len(v) == 3:
                m_f, sat, sun = process_line(v[0]), process_line(v[1]), process_line(v[2])
                res = res + flatten( ( m_f, m_f, m_f, m_f, m_f, sat, sun ) )
            elif len(v) == 4: # len(v) == 4
                m_t, fri, sat, sun = process_line(v[0]), process_line(v[1]), process_line(v[2]), process_line([3])
                res = res + flatten( ( m_t, m_t, m_t, m_t, fri, sat, sun) )
            else: # len(v) == 5
                m_t, w_t, fri, sat, sun = process_line(v[0]), process_line(v[1]), process_line(v[2]), process_line(v[3]), process_line(v[4])
                res = res + flatten( ( m_t, m_t, w_t, w_t, fri, sat, sun) )
        return res  
                
            
    address, phone, hours = data['address'], data['phone'], data['hours']
#     print address, phone, hours, services
    res = process_hours(hours)
    return 'CVS', address, phone, res

def parse_riteaid(data):
    '''给一个riteaid的json数据，返回
    name = 'riteaid' 商家名字
    address = 原始，没有经过geocode的地址
    phone = 电话 202-123-4567 十二位格式
            # 时间格式是4位数字，后两位是分钟，前两位是小时，不开门是-1
    hours = store [mon open, mon close, ..., sun open, sun close]
            pharmacy [mon open, mon close, ..., sun open, sun close]
            clinic [mon open, mon close, ..., sun open, sun close]
    '''
    def process_hours(hours):
        def process_list(array):
            if len(array) == 3:
                return process_line(array[1]), process_line(array[2])
            elif len(array) == 5:
                return (0, 2400), process_line(array[4])
            else: # len(array) == 7 都是全天开门
                return (0, 2400), (0, 2400)
            
        def process_line(line):
            if line == 'Closed':
                return -1, -1
            else:
                st, et = line.split('-')
                st, et = datetime.strptime( st, '%I:%M%p'), datetime.strptime( et, '%I:%M%p')
                return 100*st.hour + st.minute, 100*et.hour + et.minute
            
        def flatten(array):
            res = list()
            for tup in array:
                for i in tup:
                    res.append(i)
            return res
        
        m_t, fri, sat, sun = hours[0], hours[1], hours[2], hours[3]
        S_m_t, P_m_t = process_list(m_t)
        S_fri, P_fri = process_list(fri)
        S_sat, P_sat = process_list(sat)
        S_sun, P_sun = process_list(sun)
        ## 排列顺序是 'store', 'pharmacy', 'clinic'
        res = flatten([S_m_t, S_m_t, S_m_t, S_m_t, S_fri, S_sat, S_sun]) + \
              flatten([P_m_t, P_m_t, P_m_t, P_m_t, P_fri, P_sat, P_sun]) + [-1] * 14
        return res
    
    address, phone, hours = data['address'], data['phone'], data['hours']
    res = process_hours(hours)
    return 'RiteAid', address, phone, res

def parse_walgreens(data):
    '''给一个walgreens的json数据，返回
    name = 'walgreens' 商家名字
    address = 原始，没有经过geocode的地址
    phone = 电话 202-123-4567 十二位格式
            # 时间格式是4位数字，后两位是分钟，前两位是小时，不开门是-1
    hours = store [mon open, mon close, ..., sun open, sun close]
            pharmacy [mon open, mon close, ..., sun open, sun close]
            clinic [mon open, mon close, ..., sun open, sun close]
    '''
    def process_hours(hours):
        def process_list(array):
            '''
            每个商店的开门时间的格式也有4种情况: 0, 2, 3, 7
                0 - 1-7全部关门
                2 - 出错了，就设置为7天都是24小时
                3 - 普通情况 1-5, 6, 7 三种格式: '10AM-6PM', CLOSED, Open 24 hours
                7 - 1-7都有详细时间
            '''
            if len(array) == 0:
                return [(-1, -1)] * 7
            elif len(array) == 2:
                return [(0, 2400)] * 7
            elif len(array) == 3:
                return [process_line(array[0]),
                        process_line(array[0]),
                        process_line(array[0]),
                        process_line(array[0]),
                        process_line(array[0]),
                        process_line(array[1]),
                        process_line(array[2]),]
            else: # len(array) == 7
                return [process_line(array[0]),
                        process_line(array[1]),
                        process_line(array[2]),
                        process_line(array[3]),
                        process_line(array[4]),
                        process_line(array[5]),
                        process_line(array[6]),]
            
        def process_line(line):
            if line == 'CLOSED':
                return (-1, -1)
            elif line == 'Open 24 hours':
                return (0, 2400)
            else:
                st, et = line.split('-')
                if ':' in st:
                    st = datetime.strptime( st, '%I:%M%p')
                else:
                    st = datetime.strptime( st, '%I%p')
                if ':' in et:
                    et = datetime.strptime( et, '%I:%M%p')
                else:
                    et = datetime.strptime( et, '%I%p')
                return (100*st.hour + st.minute, 100*et.hour + et.minute)
            
        def flatten(array):
            res = list()
            for tup in array:
                for i in tup:
                    res.append(i)
            return res
        
        if len(hours) == 0:
            res = [-1] * 42
        else:
            store, pharmacy, clinic = ( hours.get('store', []), # ["8AM-7:30PM", "9AM-4:30PM", "9AM-4:30PM"]
                                        hours.get('pharmacy', []),
                                        hours.get('clinic', [])  )
            res = flatten(process_list(store)) + flatten(process_list(pharmacy)) + flatten(process_list(clinic))
        return res
    
    address, phone, hours = data['address'], data['phone'], data['hours']
    res = process_hours(hours)
    return 'RiteAid', address, phone, res

def main():
    cvs_path = r'CVS/CVS_data.json'
    riteaid_path = r'riteaids/riteaid_data.json'
    walgreens_path = r'walgreens/walgreens_data.json'
    
    cvs = load_jt(cvs_path)
    riteaid = load_jt(riteaid_path)
    walgreens = load_jt(walgreens_path)
    
    addrgeo = load_jt(r'address_geocoded.json')
    records = list()
    ### === PROCESS CVS === ###
    c = count(0)
    for id, v in cvs.iteritems():
        name, address, phone, hours = parse_cvs(v)
        loc = V3(addrgeo[v['address'] ] )
        formatted_address, state, zipcode, latitude, longitude = (loc.formatted_address ,
                                                                  loc.administrative_area_level_1 ,
                                                                  loc.postal_code ,
                                                                  loc.latitude ,
                                                                  loc.longitude)
        GUID = md5_obj( (name, address) )
        records.append(  tuple( [GUID, 
                                 name, 
                                 formatted_address, 
                                 state, 
                                 zipcode, 
                                 latitude, 
                                 longitude, 
                                 phone]  + hours) )
        print 'cvs %s' % c.next()
    
    ### === PROCESS RiteAids === ###
    c = count(0)
    for id, v in riteaid.iteritems():
        name, address, phone, hours = parse_riteaid(v)
        loc = V3(addrgeo[v['address'] ] )
        formatted_address, state, zipcode, latitude, longitude = (loc.formatted_address ,
                                                                  loc.administrative_area_level_1 ,
                                                                  loc.postal_code ,
                                                                  loc.latitude ,
                                                                  loc.longitude)
        GUID = md5_obj( (name, address) )
        records.append(  tuple( [GUID, 
                                 name, 
                                 formatted_address, 
                                 state, 
                                 zipcode, 
                                 latitude, 
                                 longitude, 
                                 phone]  + hours) )
        print'riteaid %s' % c.next()
    ## === PROCESS Walgreens === ###
    c = count(0)
    for id, v in walgreens.iteritems():
        name, address, phone, hours = parse_walgreens(v)
        loc = V3(addrgeo[', '.join([v['street'],v['city'],v['state'],v['zipcode']]) ] )
        formatted_address, state, zipcode, latitude, longitude = (loc.formatted_address ,
                                                                  loc.administrative_area_level_1 ,
                                                                  loc.postal_code ,
                                                                  loc.latitude ,
                                                                  loc.longitude)
        GUID = md5_obj( (name, address) )
        records.append(  tuple( [GUID, 
                                 name, 
                                 formatted_address, 
                                 state, 
                                 zipcode, 
                                 latitude, 
                                 longitude, 
                                 phone]  + hours) )
        print'walgreens %s' % c.next()
    dump_jt(records, 'records.json', replace = True)    

def unit_test():
    text = '8:30AM-5PM'
    st, et = text.split('-')
    if ':' in st:
        st = datetime.strptime( st, '%I:%M%p')
    else:
        st = datetime.strptime( st, '%I%p')
    if ':' in et:
        et = datetime.strptime( et, '%I:%M%p')
    else:
        et = datetime.strptime( et, '%I%p')
    print st, et

if __name__ == '__main__':
    main()
    
#     unit_test()