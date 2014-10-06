##encoding=utf8
##version =py27
##author  =sanhe
##date    =2014-09-11

import psycopg2
from HSH.Data.jt import *

conn = psycopg2.connect(host = '10.0.80.180',  dbname = 'securiport', user = 'postgres', password = '')
c = conn.cursor()

def hist(array):
    res = dict()
    for i in array:
        if i in res:
            res[i] = 1
        else:
            res[i]+= 1
    return res

# c.execute("SELECT dpt_id FROM depart_from_flights")
# array = list()
# for row in c.fetchall():
#     array.append(row[0])
# dump_jt(array, 'dpt_id_list.json', fastmode = True, replace = True)

array = load_jt('dpt_id_list.json')
print len(array)
print len(set(array))
# print array