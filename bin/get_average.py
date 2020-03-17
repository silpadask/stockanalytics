#!/usr/bin/python3
# coding: utf-8

#Libraries
import warnings
warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
import datetime
import pymysql
import conf_reader
print('get average execution started...')
def db_connet(database,user,password,host):
    cnx = pymysql.connect(user=user, password=password,host=host,database=database,charset='utf8', autocommit=True)
    cur=cnx.cursor()
    return cur,cnx

config = conf_reader.get_config()
inputdbprop = config.get("inputdbprop", None)

#### Connect to database
cur,cnx = db_connet(inputdbprop.get('dbname'),inputdbprop.get('dbusername'),inputdbprop.get('dbpassword'),inputdbprop.get('hostname'))
databasename=inputdbprop.get('dbname')
usedb="USE %s "% (databasename)
cur.execute(usedb)
#### Import required data from database
fetchsql="select SYMBOL, SERIES, TIMESTAMP, CLOSE, PREVCLOSE, TOTTRDQTY, TOTTRDVAL from nsedailybhavhist where SERIES='EQ' and date(timestamp)= (select date(max(timestamp)) from nsedailybhavhist) order by symbol, timestamp desc;"

cur.execute(fetchsql)
nsedailybhavhistrecords = cur.fetchall()
count=0
ptime=datetime.datetime.today()
cur.execute("select date(max(timestamp)) from nsedailybhavhist")
datestring=str(cur.fetchall()[0][0])
#DateString='2019-12-31'
for samplerecord in nsedailybhavhistrecords:
    symbolstring= samplerecord[0]
    SERIES= samplerecord[1]
    #DateString = str(SampleRecord[2])
    sampleinsertsql=" INSERT INTO prcs_rslt  (PRCS_DT, SYMBOL, TIMESTAMP, CLOSE, PREVCLOSE, TOTTRDQTY, TOTTRDVAL, TIMESTAMP_07, CLOSE_07, AVG_07, AVG_TOTTRDQTY_07, AVG_TOTTRDVAL_07, CNT_07, TIMESTAMP_15, CLOSE_15, AVG_15, AVG_TOTTRDQTY_15, AVG_TOTTRDVAL_15, CNT_15, TIMESTAMP_30, CLOSE_30, AVG_30, AVG_TOTTRDQTY_30, AVG_TOTTRDVAL_30, CNT_30, TIMESTAMP_60, CLOSE_60, AVG_60, AVG_TOTTRDQTY_60, AVG_TOTTRDVAL_60, CNT_60, TIMESTAMP_90, CLOSE_90, AVG_90, AVG_TOTTRDQTY_90, AVG_TOTTRDVAL_90, CNT_90, TIMESTAMP_120, CLOSE_120, AVG_120, AVG_TOTTRDQTY_120, AVG_TOTTRDVAL_120, CNT_120, TIMESTAMP_180, CLOSE_180, AVG_180, AVG_TOTTRDQTY_180, AVG_TOTTRDVAL_180, CNT_180, TIMESTAMP_365, CLOSE_365, AVG_365, AVG_TOTTRDQTY_365, AVG_TOTTRDVAL_365, CNT_365, TIMESTAMP_730, CLOSE_730, AVG_730, AVG_TOTTRDQTY_730, AVG_TOTTRDVAL_730, CNT_730) "+\
    "select date_format(now(),'%Y-%m-%d') as rundt, SYMBOL, "+\
    "max(case when timestamp = str_to_date( '"+datestring+"' , '%Y-%m-%d') then timestamp end) as timestamp,"+\
    "max(case when timestamp = str_to_date( '"+datestring+"' , '%Y-%m-%d') then close end) as close,"+\
    "max(case when timestamp = str_to_date( '"+datestring+"' , '%Y-%m-%d') then prevclose end) as prevclose,"+\
    "max(case when timestamp = str_to_date( '"+datestring+"' , '%Y-%m-%d') then tottrdqty end) as tottrdqty,"+\
    "max(case when timestamp = str_to_date( '"+datestring+"' , '%Y-%m-%d') then tottrdval end) as tottrdval,"+\
    "min(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 week) then timestamp end) as timestamp_07,"+\
    "(select close from nsedailybhavhist where symbol =  '"+symbolstring+"' and SERIES='EQ' and timestamp =(select min(timestamp) from nsedailybhavhist where timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and   timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 week) and symbol= '"+datestring+"' and SERIES='EQ')) as close_07,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 week) then close end) as avg_07,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 week) then tottrdqty end) as Avg_tottrdqty_07,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 week) then tottrdval end) as avg_tottrdval_07,"+\
    "count(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 week) then timestamp end) as cnt_07,"+\
    "min(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 week) then timestamp end) as timestamp_15,"+\
    "(select close from nsedailybhavhist where symbol =  '"+symbolstring+"' and SERIES='EQ' and timestamp =(select min(timestamp) from nsedailybhavhist where timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and   timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 week) and symbol= '"+symbolstring+"' and SERIES='EQ')) as close_15,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 week) then close end) as avg_15,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 week) then tottrdqty end) as tottrdqty_15,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 week) then tottrdval end) as tottrdval_15,"+\
    "count(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 week) then timestamp end) as cnt_15,"+\
    "min(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 month) then timestamp end) as timestamp_30,"+\
    "(select close from nsedailybhavhist where symbol =  '"+symbolstring+"' and SERIES='EQ' and timestamp =(select min(timestamp) from nsedailybhavhist where timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and   timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 month) and symbol= '"+symbolstring+"' and SERIES='EQ')) as close_30,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 month) then close end) as avg_30,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 month) then tottrdqty end) as tottrdqty_30,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 month) then tottrdval end) as tottrdval_30,"+\
    "count(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 month) then timestamp end) as cnt_30,"+\
    "min(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 month) then timestamp end) as timestamp_60,"+\
    "(select close from nsedailybhavhist where symbol =  '"+symbolstring+"' and SERIES='EQ' and timestamp =(select min(timestamp) from nsedailybhavhist where timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and   timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 month) and symbol= '"+symbolstring+"' and SERIES='EQ' )) as CLOSE_60,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 month) then close end) as avg_60,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 month) then tottrdqty end) as tottrdqty_60,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 month) then tottrdval end) as tottrdval_60,"+\
    "count(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 month) then timestamp end) as cnt_60,"+\
    "min(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -3 month) then timestamp end) as timestamp_90,"+\
    "(select close from nsedailybhavhist where symbol =  '"+symbolstring+"' and SERIES='EQ' and timestamp =(select min(timestamp) from nsedailybhavhist where timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and   timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -3 month) and symbol= '"+symbolstring+"' and SERIES='EQ')) as  CLOSE_90,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -3 month) then close end) as avg_90,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -3 month) then tottrdqty end) as tottrdqty_90,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -3 month) then tottrdval end) as tottrdval_90,"+\
    "count(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -3 month) then timestamp end) as cnt_90,"+\
    "min(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -4 month) then timestamp end) as timestamp_120,"+\
    "(select close from nsedailybhavhist where symbol =  '"+symbolstring+"' and SERIES='EQ' and timestamp =(select min(timestamp) from nsedailybhavhist where timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and   timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -4 month) and symbol= '"+symbolstring+"' and SERIES='EQ')) as CLOSE_120,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -4 month) then close end) as avg_120,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -4 month) then tottrdqty end) as tottrdqty_120,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -4 month) then tottrdval end) as tottrdval_120,"+\
    "count(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -4 month) then timestamp end) as cnt_120,"+\
    "min(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -6 month) then timestamp end) as timestamp_180,"+\
    "(select close from nsedailybhavhist where symbol =  '"+symbolstring+"' and SERIES='EQ' and timestamp =(select min(timestamp) from nsedailybhavhist where timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and   timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -6 month) and symbol= '"+symbolstring+"' and SERIES='EQ')) as CLOSE_180,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -6 month) then close end) as avg_180,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -6 month) then tottrdqty end) as tottrdqty_180,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -6 month) then tottrdval end) as tottrdval_180,"+\
    "count(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -6 month) then timestamp end) as cnt_180,"+\
    "min(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 year) then timestamp end) as timestamp_365,"+\
    "(select close from nsedailybhavhist where symbol =  '"+symbolstring+"' and SERIES='EQ' and timestamp =(select min(timestamp) from nsedailybhavhist where timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and   timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 year) and symbol= '"+symbolstring+"' and SERIES='EQ')) as CLOSE_365,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 year) then close end) as avg_365,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 year) then tottrdqty end) as tottrdqty_365,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 year) then tottrdval end) as tottrdval_365,"+\
    "count(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -1 year) then timestamp end) as cnt_365,"+\
    "min(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 year) then timestamp end) as timestamp_730,"+\
    "(select close from nsedailybhavhist where symbol =  '"+symbolstring+"' and SERIES='EQ' and timestamp =(select min(timestamp) from nsedailybhavhist where timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and   timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 year) and symbol= '"+symbolstring+"' and SERIES='EQ')) as CLOSE_730,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 year) then close end) as avg_730,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 year) then tottrdqty end) as tottrdqty_730,"+\
    "avg(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 year) then tottrdval end) as tottrdval_730,"+\
    "count(case when timestamp <= str_to_date( '"+datestring+"' , '%Y-%m-%d') and timestamp > date_add(str_to_date( '"+datestring+"' , '%Y-%m-%d'), interval -2 year) then timestamp end) as cnt_730 "+\
    "from nsedailybhavhist "+\
    "where symbol = '"+symbolstring+"' and SERIES='EQ' and symbol is not null"
    try:
        cur.execute(sampleinsertsql)
    except:pass
    count=count+1
    if count%100==0:
        ltime=datetime.datetime.today()
        print(str(count)+'|'+str(ltime-ptime))
