##encoding=utf8
##version =py27
##author  =sanhe
##date    =2014-09-11

import psycopg2
from parse_html import records_from_file
from HSH.Data.jt import *
import os, shutil
import time

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
    c.execute(cmd) # flight_departure
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
    c.execute(cmd) # flight_arrival
    conn.commit()
    

def push_departure(path_dpt):
    ## push DEPARTURE
    for fname in os.listdir(path_dpt):
        records = records_from_file(os.path.join(path_dpt, fname), mode = 'departure')
        try:
            c.executemany("INSERT INTO public.depart_from_flights VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", records)
        except psycopg2.IntegrityError:
            conn.rollback()
            for record in records:
                try:
                    c.executemany("INSERT INTO public.depart_from_flights VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (record,) )
                except psycopg2.IntegrityError:
                    conn.rollback()
                    cmd = \
                    """
                    UPDATE public.depart_from_flights
                    SET
                    dpt_id = %s,
                    origin = %s,
                    destination = %s,
                    flight = %s,
                    airline = %s,
                    scheduled_time = %s,
                    actual_time = %s,
                    Terminal_Gate = %s,
                    status = %s,
                    equip = %s
                    WHERE
                    dpt_id = %s
                    """
                    c.execute(cmd, (record[0],
                                    record[1],
                                    record[2],
                                    record[3],
                                    record[4],
                                    record[5],
                                    record[6],
                                    record[7],
                                    record[8],
                                    record[9],
                                    record[0]) )
                    conn.commit()
                else:
                    conn.commit()
        else:
            conn.commit()
        os.remove(os.path.join(path_dpt, fname))
        print 'INSERT %s to DB success! %s records' % (os.path.join(path_dpt, fname), len(records) )

def push_arrival(path_arv):
    ## push ARRIVAL
    for fname in os.listdir(path_arv):
        records = records_from_file(os.path.join(path_arv, fname), mode = 'arrival')
        try:
            c.executemany("INSERT INTO public.arrive_at_flights VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", records)
        except psycopg2.IntegrityError:
            conn.rollback()
            for record in records:
                try:
                    c.executemany("INSERT INTO public.arrive_at_flights VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (record,) )
                except psycopg2.IntegrityError:
                    conn.rollback()
                    cmd = \
                    """
                    UPDATE public.arrive_at_flights
                    SET
                    arv_id = %s,
                    origin = %s,
                    destination = %s,
                    flight = %s,
                    airline = %s,
                    scheduled_time = %s,
                    actual_time = %s,
                    Terminal_Gate = %s,
                    status = %s,
                    equip = %s
                    WHERE
                    arv_id = %s
                    """
                    c.execute(cmd, (record[0],
                                    record[1],
                                    record[2],
                                    record[3],
                                    record[4],
                                    record[5],
                                    record[6],
                                    record[7],
                                    record[8],
                                    record[9],
                                    record[0]) )
                    conn.commit()
                else:
                    conn.commit()
        else:
            conn.commit()
        os.remove(os.path.join(path_arv, fname))
        print 'INSERT %s to DB success! %s records' % (os.path.join(path_arv, fname), len(records) )

if __name__ == '__main__':
    try:
        create_table()
    except:
        conn.rollback()
        
    base = r'C:\Users\Sanhe.Hu\AppData\Local\Temp'
    while 1:
        print 'PUSHING...'
        push_departure(r'departure')
        push_arrival(r'arrival')
        for fname in os.listdir(base): ## delete the temp file folder
            if fname.startswith('tmp'):
                try:
                    shutil.rmtree(os.path.join(base, fname))
                except:
                    pass
                
        print 'SLEEPING...'
        time.sleep(1200)
        
