##encoding=utf8
##version =py27
##author  =sanhe
##date    =2014-09-06

from HSH.Data.jt import *
from HSH.Data.hsh_hashlib import md5_obj
from bs4 import BeautifulSoup as BS4

import itertools
import os
import datetime
import sys
import hashlib
import pickle

valid_airport_code = load_jt(r'reference\constant\airport_codelist.json')

def md5_obj(obj):
    '''return md5 value from a PYTHON OBJECT
    '''
    m = hashlib.md5()
    m.update(pickle.dumps(obj) )
    return m.hexdigest()

def tag2row(tag_tr, valid_airport_code, date):
    '''extract airport, flight, airline, scheduled_time, actual_time, Terminal_Gate, status, equip
    from one html tag element (line of a table)
    '''
    def process_space(text):
        '''handle unexpected \n \r
        '''
        text = text.strip()
        text = text.replace('\n', '')
        text = text.replace('\r', '')
        return text
    
    def replace_special_char(text):
        '''delete unexpected char
        \xa0, \a0124, etc indicate delayed severity
        "^" indicate it's a code share flight
        "~" indicate it's a estimated time
        '''
        special_char1 = [u'\xa0', u'\xa0124', u'\xa031', u'\xa028']
        special_char2 = [u'^', u'~']
        for char in special_char1:
            text = text.replace(char, ' ')
        for char in special_char2:
            text = text.replace(char, '')
        return text.strip()
    
    def clear_double_space(text):
        array = text.split(' ')
        res = list()
        for i in array:
            if i != '':
                res.append(i)
        return ' '.join(res)
    
    results = tag_tr.find_all('td') # 每个td是一个列元素
    airport = process_space(results[0].text)
    flight = replace_special_char( process_space(results[1].text) )
#     on_time_rating = process_space(results[2].text)
    airline = process_space(results[3].text)
    scheduled_time = process_space(results[4].text)
    actual_time = replace_special_char( process_space(results[5].text) )
    Terminal_Gate = process_space(results[6].text)
    status = clear_double_space( replace_special_char( process_space(results[7].text) ) )
    equip = process_space(results[8].text)
#     track = process_space(results[9].text)
    airport = airport[:3]
    scheduled_time, actual_time = ( str( datetime.datetime.strptime( '%s %s' % (date, scheduled_time), '%Y-%m-%d %I:%M %p') ),
                                    str( datetime.datetime.strptime( '%s %s' % (date, actual_time), '%Y-%m-%d %I:%M %p') ) ) 
    
    return airport, flight, airline, scheduled_time, actual_time, Terminal_Gate, status, equip

def parse_fname(fname):
    '''a standard file name for raw_html flight data is like: "2014-09-10 SIT 00-24.html".
    It means flight information departure from or arrive at SIT in 2014-09-10, event time between 00-24
    INPUT: standard fname
    OUTPUT: date, airport_code (3 digit)
    '''
    name, _ = os.path.splitext(os.path.basename(fname))
    date, airport_code, _ = name.split(' ')
    return date, airport_code

def records_from_file(fname, mode):
    '''Given a search result html file, generate list of records
    '''
    date, airport_code = parse_fname(fname)
    if mode == 'departure':
        origin = airport_code
    else: # mode == 'arrival'
        destination = airport_code
        
    with open(fname, 'rb') as f:
        html = f.read()
        html = html.replace('a0:', '')
    soup = BS4(html)
    
    c = itertools.count(0)
    records = list()
    for tr in soup.find_all('tr', attrs = {}):
        try: # tag2row 很可能会出错，一旦出错说明这个tag不包含数据，那么就可以直接跳过了
            if mode == 'departure':
                destination, flight, airline, scheduled_time, actual_time, Terminal_Gate, status, equip = tag2row(tr, valid_airport_code, date)
            else: # mode == 'arrival'
                origin, flight, airline, scheduled_time, actual_time, Terminal_Gate, status, equip = tag2row(tr, valid_airport_code, date)
            if c.next() >= 2: # 从第3个tr tag开始，才是正确的航班信息
                if (origin in valid_airport_code) and (destination in valid_airport_code):
                    GUID = md5_obj( (origin, destination, flight, scheduled_time) )
                    records.append( (GUID, origin, destination, flight, airline, scheduled_time, actual_time, Terminal_Gate, status, equip) )
        except:
            pass
    return records

if __name__ == '__main__':
    records = records_from_file(r'C:\HSH\Workspace\py27_projects\EFA-on-going-projects\TSA real-time query\04_database\2014-09-11 LAX 08-09.html', mode = 'departure')
    from pprint import pprint as ppt
    ppt(records)
