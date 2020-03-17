#! /usr/bin/python3
# coding: utf-8
import os
import pymysql
import pandas as pd
import datetime
from zipfile import ZipFile
import warnings
import conf_reader
warnings.filterwarnings("ignore")
print(' bhav upload execution started...')
def filedateconv(fname):
    y=int(fname[7:11])
    m=fname[4:7]
    d=int(fname[2:4])
    dct={'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}
    for k,v in dct.items():
        if m==k:
            m=int(v)
    return(datetime.date(y,m,d))
print("Date parsing function created...")

def dtformat(strg):
    dct={'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}
    d=int(strg[:2])
    m=strg[3:6]
    for k,v in dct.items():
        if m==k:
            m=int(v)
    y=int(strg[7:])
    return(datetime.datetime(y,m,d))
print("Date format function created")

def db_connet(database,user,password,host):
    cnx = pymysql.connect(user=user, password=password,host=host,database=database,charset='utf8', autocommit=True)
    cur=cnx.cursor()
    return cur,cnx

config = conf_reader.get_config()
inputdbprop = config.get("inputdbprop", None)

# #### Connect to database
cur,cnx = db_connet(inputdbprop.get('dbname'),inputdbprop.get('dbusername'),inputdbprop.get('dbpassword'),inputdbprop.get('hostname'))
databasename=inputdbprop.get('dbname')
usedb="use %s "% (databasename)
cur.execute(usedb)

print("Database connection established")
try:
    fetchsql="select distinct(date(timestamp)) from nse.nsedailybhavhist where date(timestamp)>'2015-12-01';"
    cur.execute(fetchsql)
    dates=tuple(pd.DataFrame(list(cur.fetchall()))[0])
except:
    dates=()

count=0
ptime=datetime.datetime.now()
for fname in os.listdir('../data/'):
    if (str(fname)[0]=='c')&(str(fname)[-1]=='p'):
        dt=filedateconv(fname)
        if dt not in dates:
            df=pd.read_csv('../data/'+fname, compression='zip', sep=',')
            df['TIMESTAMP']=df['TIMESTAMP'].apply(lambda x : dtformat(x))
            df=df.iloc[:,:13]
            df["FACTOR"]=0
            df["OPEN_ADJ"]=df["OPEN"]
            df["HIGH_ADJ"]=df["HIGH"]
            df["LOW_ADJ"]=df["LOW"]
            df["CLOSE_ADJ"]=df["CLOSE"]
            df["LAST_ADJ"]=df["LAST"]
            df["TOTTRDQTY_ADJ"]=df["TOTTRDQTY"]
            for col in df.columns:
                df[col]=df[col].apply(lambda x : str(x))
            insertSQL="INSERT INTO nsedailybhavhist (SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE, TOTTRDQTY, TOTTRDVAL, TIMESTAMP, TOTALTRADES, ISIN, FACTOR, OPEN_ADJ, HIGH_ADJ, LOW_ADJ, CLOSE_ADJ, LAST_ADJ, TOTTRDQTY_ADJ) VALUES "
            val=str(list(df.itertuples(index=False, name=None)))[1:-1]
            isql=insertSQL+val
            try:
                cur.execute(isql)
            except:pass
            count=count+1
            if count%25==0:
                print(str(count)+'|'+fname+'|',str(datetime.datetime.now()-ptime))
print(str(count)+" days data has been loaded in to database")
