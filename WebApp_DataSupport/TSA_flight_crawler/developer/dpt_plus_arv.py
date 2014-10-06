##encoding=utf8
##version =py27
##author  =sanhe
##date    =2014-09-11

import psycopg2
from HSH.DBA.hsh_postgres import iterC, prt_all
from HSH.Data.hsh_hashlib import md5_obj
import pandas as pd

'''
【说明】本脚本用于合并    出发，到达    数据库。并对合并后的数据库进行检查
例：搜索2014-09-11出发的航班，找到对应的到达航班，并匹配
1.2014-09-11出发的所有航班，取得dpt_id = md5( origin, destination, flight, equip )
2.找到2014-09-11和2014-09-12到达的所有航班
'''


conn = psycopg2.connect(host = '10.0.80.180',  dbname = 'securiport', user = 'postgres', password = '')
c = conn.cursor()
c1 = conn.cursor()
def create_table():
    cmd = \
    """
    CREATE TABLE public.us_flights
    (id varchar(32) PRIMARY KEY NOT NULL,
    origin varchar(3) NOT NULL,
    destination varchar(3) NOT NULL,
    flight varchar(16) NOT NULL,
    airline varchar(255) NOT NULL,
    
    dpt_scheduled_time timestamp NOT NULL,
    dpt_actual_time timestamp NOT NULL,
    dpt_status varchar(64),
    dpt_Terminal_Gate varchar(16),
    
    arv_scheduled_time timestamp NOT NULL,
    arv_actual_time timestamp NOT NULL,
    arv_status varchar(64),
    arv_Terminal_Gate varchar(16),
    
    equip varchar(16)
    );
    """
    c.execute(cmd)
    conn.commit()

# create_table()

def merge_dpt_arv():
    cmd = \
    """
    SELECT
    depart_from_flights.origin,
    depart_from_flights.destination,
    
    depart_from_flights.flight,
    depart_from_flights.airline,
    
    depart_from_flights.scheduled_time,
    depart_from_flights.actual_time,
    depart_from_flights.status,
    depart_from_flights.terminal_gate,
    
    arrive_at_flights.scheduled_time,
    arrive_at_flights.actual_time,
    depart_from_flights.status,
    arrive_at_flights.terminal_gate,
    
    depart_from_flights.equip
    
    FROM 
    depart_from_flights INNER JOIN arrive_at_flights
    ON 
    depart_from_flights.origin = arrive_at_flights.origin AND
    depart_from_flights.destination = arrive_at_flights.destination AND
    depart_from_flights.flight = arrive_at_flights.flight AND
    arrive_at_flights.scheduled_time > depart_from_flights.scheduled_time AND
    arrive_at_flights.scheduled_time < depart_from_flights.scheduled_time + INTERVAL '24 hour'
    ORDER BY
    depart_from_flights.origin ASC, 
    depart_from_flights.destination ASC, 
    depart_from_flights.scheduled_time ASC,
    depart_from_flights.flight ASC,
    arrive_at_flights.scheduled_time DESC;
    """
#     depart_from_flights.scheduled_time >= '2014-09-22 00:00:00' AND
#     depart_from_flights.scheduled_time < '2014-09-23 00:00:00' AND
    c.execute(cmd)
    
    # 插入的SQL语句
    cmd_insert = \
    """
    INSERT INTO public.us_flights VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    # 更新的SQL语句
    cmd_update = \
    """
    UPDATE public.us_flights SET
    origin = %s,
    destination = %s,
    flight = %s,
    airline = %s,
    
    dpt_scheduled_time = %s,
    dpt_actual_time = %s,
    dpt_status = %s,
    dpt_Terminal_Gate = %s,
    
    arv_scheduled_time = %s,
    arv_actual_time = %s,
    arv_status = %s,
    arv_Terminal_Gate = %s,
    
    equip = %s
    WHERE id = %s;
    """
    for row in iterC(c):
        org, dst, flight, airline, dpt_sch, dpt_act, dpt_status, dpt_term, arv_sch, arv_act, arv_status, arv_term, equip = row
        ID = md5_obj(  (org, dst, flight, dpt_sch)  )
        try:
            c1.execute(cmd_insert, (ID, org, dst, flight, airline, dpt_sch, dpt_act, dpt_status, dpt_term, arv_sch, arv_act, arv_status, arv_term, equip))
        except psycopg2.IntegrityError:
            conn.rollback()
            c1.execute(cmd_update, (org, dst, flight, airline, dpt_sch, dpt_act, dpt_status, dpt_term, arv_sch, arv_act, arv_status, arv_term, equip, ID))
        else:
            conn.commit()
            
# merge_dpt_arv()

def how_many_data_we_have():
    c.execute("SELECT count(*) FROM (SELECT * FROM us_flights) AS everything")
    for row in c.fetchall():
        print '%s us flights records' % row[0]
        
# how_many_data_we_have()

def how_many_by_date(one_date):
    """one_date format = '2014-09-15'
    """
    cmd = \
    """
    SELECT count(*) FROM (SELECT * FROM us_flights WHERE dpt_scheduled_time >= '%s 00:00:00'::timestamp AND dpt_scheduled_time <= '%s 23:59:59'::timestamp) AS everything
    """ % (one_date, one_date)
    c.execute(cmd)
    for row in c.fetchall():
        print '%s we got %s us flights records' % (one_date, row[0])

# how_many_by_date('2014-09-21')

def tableau():
    c.execute("SELECT * FROM us_flights WHERE dpt_scheduled_time >= '2014-09-15 00:00:00' AND dpt_scheduled_time <= '2014-10-21 23:59:59'")
    df = pd.DataFrame(c.fetchall(), columns = ["ID","dpt","arv","flight","airline","dpt_sch","dpt_act","dpt_status","dpt_term","arv_sche","arv_act","arv_status","arv_term","equip"])
    writer = pd.ExcelWriter("us_flights.xlsx")
    df.to_excel(writer, "2014-09-15 to 2014-09-21")
    writer.save()
    
tableau()