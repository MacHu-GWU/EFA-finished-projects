##encoding=utf8
##version =py27
##author  =sanhe
##date    =2014-09-02

'''
Useful funcationality repack from geopy
'''
from geopy.geocoders import GoogleV3
from geopy.distance import vincenty
from LinearSpider.jt import *
from LinearSpider.logger import Log
import jsontree
import sys
import time
import itertools

def sleep(n):
    time.sleep(n)

def prt_LastError():
    print '\t %s' % sys.exc_info()[0] # exception class name
    print '\t %s' % sys.exc_info()[1] # exception index name
    
def dist(coordinate1, coordinate2, unit = 'mile'):
    '''Calculate distance
    INPUT: coordinate tuple = (longitude, latitude) in decimal format (not degree)
    Output: distance in mileage, kilometers or feet. Default is mileage.
    '''
    distance = vincenty(coordinate1, coordinate2)
    if unit == 'mile':
        return distance.miles
    elif unit == 'km':
        return distance.kilometers
    elif unit == 'feet':
        return distance.feet
    else:
        print 'Error! please set unit as "mile", "km" or "feet".'
        return None

def geocode_one(address, api_key):
    '''Return json reply from googleV3 geocoder result of one address
    if it is not able to geocode the address, return None
    '''
    engine = GoogleV3(api_key)
    location = engine.geocode(address) # 如果出错则会返回None
    if location:
        LOC = location.raw
        return LOC
    else:
        return None

def batch_geocoding(addr_list, addr_geocoded):
    '''
    addr_list = list of address need to geocode
    addr_geocoded = json data of successfully geocoded address
    '''
    api_keys = ['AIzaSyAuzs8xdbysdYZO1wNV3vVw1AdzbL_Dnpk', # sanhe
                'AIzaSyBfgV3y5z_od63NdoTSgu9wgEdg5D_sjnk', # rich
                'AIzaSyDsaepgzV7qoczqTW7P2fMmvigxnzg-ZdE', # meng yan
                'AIzaSyBqgiVid6V2xPZoADmv7dobIfvbhvGhEZA', # zhang tao
                'AIzaSyBtbvGbyAwiywSdsk8-okThcN3q515GDZQ', # jack
                'AIzaSyC5XmaneaaRYLr4H0x7HMRoFPgjW9xcu2w', # fenhan
                'AIzaSyDgM5xmKIjS_nooN_TBRLxrFDypVyON9bU',] # Amina
    cyc_api = itertools.cycle(api_keys) # cycle among all available api_keys
    
    n = 100 # dump data to local every 100 times successful geocoding
    cyc_save_flag = itertools.cycle(range(n)) 
    
    log = Log()
    exception_counter = 0
    
    for addr in addr_list:
        if addr not in addr_geocoded: # if never crawled before
            sleep(0.1)
            try:
                LOC = geocode_one(addr, cyc_api.next() )
                if LOC: # 如果geocode成功
                    addr_geocoded[addr] = LOC
                    print 'success. now we have geocoded %s' % len(addr_geocoded)
                    exception_counter = 0 # 成功就重置
                    if cyc_save_flag.next() == n - 1: # 每100次成功保存一次
                        dump_jt(addr_geocoded, 'address_geocoded.json', fastmode = True, replace = True)
                else: # 如过失败，说明这个地址不可被geocoding。则保存日志到本地
                    log.write('failed to geocoding', addr)
            except:
                '''
                timeout:
                    <class 'geopy.exc.GeocoderTimedOut'>
                    Service timed out
                denied:
                    <class 'geopy.exc.GeocoderQueryError'>
                    Your request was denied.
                reach_limit:
                    <class 'geopy.exc.GeocoderQuotaExceeded'>
                    The given key has gone over the requests limit in the 24 hour period or has submitted too many requests in too short a period of time.
                '''
                prt_LastError()
                exception_counter += 1 # 失败就+1，这样当连续出错n次时，可以停止程序，避免浪费API使用次数
                if exception_counter == len(api_keys): # 连续出错5次异常，就跳出程序
                    print '连续5次异常！退出程序！'
                    dump_jt(addr_geocoded, 'address_geocoded.json', fastmode = True, replace = True)
                    return None
    ## === 如果顺利结束，则最后做一次保存 ===
    dump_jt(addr_geocoded, 'address_geocoded.json', fastmode = True, replace = True)
    return None

def unit_test1():
    '''calculate distance'''
    cd1 = (38.953165, -77.396170) # EFA
    cd2 = (38.899697, -77.048557) # GWU
    print dist(cd1, cd2)

def unit_test2():
    addr_list = load_jt('address_list.json')
    print len(addr_list)
    print len(set(addr_list))
    
    addr_geocoded = list( load_jt('address_geocoded.json').keys() )
    print len(addr_geocoded)
    print len(set(addr_geocoded))
    
def main():
    '''batch_geocoding'''
    addr_list = load_jt('address_list.json') # <== the file you save your todo address list
    addr_geocoded = load_jt('address_geocoded.json') # <== the file you want to save your result
    batch_geocoding(addr_list, addr_geocoded)
    

if __name__ == '__main__':
#     main()
#     unit_test1()
    unit_test2()