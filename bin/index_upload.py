#!/usr/bin/python3
# coding: utf-8

import os
import pymysql
import pandas as pd
import datetime
ptime=datetime.datetime.now()
import warnings
import conf_reader

warnings.filterwarnings("ignore")
print('index upload execution started...')
def filedateconv(fname):
    y=int(fname[18:22])
    m=int(fname[16:18])
    d=int(fname[14:16])
    dct={'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}
    for k,v in dct.items():
        if m==k:
            m=int(v)
    return(datetime.date(y,m,d))
print("Date parsing function created...")

def dtformat(strg):
    strg=strg.split(' ')[0]
    d=int(strg[:2])
    m=int(strg[3:5])
    y=int(strg[6:])
    return(datetime.date(y,m,d))

def db_connet(database,user,password,host):
    cnx = pymysql.connect(user=user, password=password,host=host,database=database,charset='utf8', autocommit=True)
    cur=cnx.cursor()
    return cur,cnx

config = conf_reader.get_config()
inputdbprop = config.get("inputdbprop", None)

# #### Connect to database
cur,cnx = db_connet(inputdbprop.get('dbname'),inputdbprop.get('dbusername'),inputdbprop.get('dbpassword'),inputdbprop.get('host_name'))
databasename=inputdbprop.get('dbname')
usedb="USE %s "% (databasename)
cur.execute(usedb)

try:
    fetchsql="select distinct(date(timestamp)) from nsedailyindexhist where substr(date(timestamp),1,4)=substr(date(now()),1,4) order by date(timestamp) desc;"
    cur.execute(fetchsql)
    dates=tuple(pd.DataFrame(list(cur.fetchall()))[0])
except:
    dates=()

count=0
for fname in os.listdir('../data/'):
    if str(fname)[0]=='i':
        dt=filedateconv(fname)
        if (dt.year==datetime.date.today().year)&(dt not in dates):
#        if (dt.year==2018)&(dt not in dates):
            print(fname)
            df=pd.read_csv('../data/'+fname, sep=',')
            df['Index Date']=df['Index Date'].apply(lambda x : dtformat(x))
            df['CTRD_DATE']=str(datetime.date.today())
            df=df.fillna(0)
            for col in df.columns:
                df[col]=df[col].apply(lambda x : str(x))
            for numcol in df.columns[2:-1]:
                df[numcol]=df[numcol].apply(lambda x : x.replace('-','0'))
            insertSQL="INSERT INTO nsedailyindexhist (INDEX_NAME, TIMESTAMP, OPEN, HIGH, LOW, CLOSE, POINTS_CHANGE, PCT_CHANGE, TOTTRDQTY, TOTTRDVAL, PE, PB, DIV_YIELD, CTRD_DATE) VALUES "   
            val=str(list(df.itertuples(index=False, name=None)))[1:-1]
            isql=insertSQL+val
            try:
                cur.execute(isql)
                count=count+1
                if count%25==0:
                    print(str(count)+'|'+fname+'|',str(datetime.datetime.now()-ptime))
            except:
                pass
print(str(count)+'|'+str(datetime.datetime.now()-ptime))

