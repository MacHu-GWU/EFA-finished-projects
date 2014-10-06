##encoding=utf8
##version =py27
##author  =sanhe
##date    =2014-09-11

"""
本脚本用于检查，管理depart_from_flights和arrive_at_flights数据库
"""

import psycopg2
from HSH.DBA.hsh_postgres import iterC, prt_all
from HSH.Data.hsh_hashlib import md5_obj
from HSH.Data.jt import *
import datetime
from collections import OrderedDict

conn = psycopg2.connect(host = '10.0.80.180',  dbname = 'securiport', user = 'postgres', password = '')
c = conn.cursor()

def create_table():
    cmd = \
    """
    CREATE TABLE public.depart_from_flights
    (dpt_id varchar(32) PRIMARY KEY NOT NULL,
    origin varchar(3) NOT NULL,
    destination varchar(3) NOT NULL,
    flight varchar(16) NOT NULL,
    airline varchar(255) NOT NULL,
    scheduled_time timestamp NOT NULL,
    actual_time timestamp,
    Terminal_Gate varchar(16),
    status varchar(64),
    equip varchar(16));
    """
#     c.execute(cmd) # depart_from_flights
    cmd = \
    """
    CREATE TABLE public.arrive_at_flights
    (arv_id varchar(32) PRIMARY KEY NOT NULL,
    origin varchar(3) NOT NULL,
    destination varchar(3) NOT NULL,
    flight varchar(16) NOT NULL,
    airline varchar(255) NOT NULL,
    scheduled_time timestamp NOT NULL,
    actual_time timestamp,
    Terminal_Gate varchar(16),
    status varchar(64),
    equip varchar(16));
    """
#     c.execute(cmd) # arrive_at_flights
    cmd = \
    """
    CREATE TABLE public.confirmed_flights
    (id varchar(32) PRIMARY KEY NOT NULL,
    origin varchar(3) NOT NULL,
    destination varchar(3) NOT NULL,
    flight varchar(16) NOT NULL,
    airline varchar(255) NOT NULL,
    scheduled_time_dpt timestamp NOT NULL,
    actual_time_dpt timestamp,
    scheduled_time_arv timestamp NOT NULL,
    actual_time_arv timestamp,
    Terminal_Gate_dpt varchar(16),
    Terminal_Gate_arv varchar(16),
    equip varchar(16));
    """
    c.execute(cmd) # confirmed_flights
    conn.commit()

# create_table()

def how_many_data_we_have():
    c.execute("SELECT count(*) FROM (SELECT * FROM depart_from_flights) AS everything")
    for row in c.fetchall():
        print '%s departure flights records' % row[0]
    c.execute("SELECT count(*) FROM (SELECT * FROM arrive_at_flights) AS everything")
    for row in c.fetchall():
        print '%s arrive flights records' % row[0]

# how_many_data_we_have()

def how_many_by_date(one_date):
    """one_date format = '2014-09-15'
    """
    cmd = \
    """
    SELECT count(*) FROM (SELECT * FROM depart_from_flights WHERE scheduled_time >= '%s 00:00:00'::timestamp AND scheduled_time <= '%s 23:59:59'::timestamp) AS everything
    """ % (one_date, one_date)
    c.execute(cmd)
    for row in c.fetchall():
        print '%s we got %s departure records' % (one_date, row[0])
    cmd = \
    """
    SELECT count(*) FROM (SELECT * FROM arrive_at_flights WHERE scheduled_time >= '%s 00:00:00'::timestamp AND scheduled_time <= '%s 23:59:59'::timestamp) AS everything
    """ % (one_date, one_date)
    c.execute(cmd)
    for row in c.fetchall():
        print '%s we got %s arrival records' % (one_date, row[0])

# how_many_by_date('2014-10-02')

def data_integrity():
    ## actual_time的信息都是完整的
    c.execute("SELECT depart_from_flights.actual_time FROM depart_from_flights")
    for row in iterC(c):
        if not type(row[0]) == datetime.datetime:
            print row

    c.execute("SELECT arrive_at_flights.actual_time FROM arrive_at_flights")
    for row in iterC(c):
        if not type(row[0]) == datetime.datetime:
            print row
        
# data_integrity()

