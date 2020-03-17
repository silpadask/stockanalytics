#!/usr/bin/python3
# coding: utf-8

import io
import pandas as pd
import requests
import pymysql
import datetime
print("Libraries imported...")
import warnings
import conf_reader
print('get symbols execution started...')
warnings.filterwarnings("ignore")
ptime=datetime.datetime.now()
print(str(ptime)+' execution started...')

def dateconv(datestring):
    y=int(datestring[7:])
    m=datestring[3:6]
    d=int(datestring[:2])
    dct={'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}
    for k,v in dct.items():
        if m==k:
            m=int(v)
    return(datetime.datetime(y,m,d))
print("Date parsing function created...")

def db_connet(database,user,password,host):
    cnx = pymysql.connect(user=user, password=password,host=host,database=database,charset='utf8', autocommit=True)
    cur=cnx.cursor()
    return cur,cnx

url="http://www1.nseindia.com/content/equities/EQUITY_L.csv"
cont=requests.get(url).content
df=pd.read_csv(io.StringIO(cont.decode('utf-8')))
df.columns = df.columns.str.replace(' ', '')
print("Symbols csv file loaded...")


config = conf_reader.get_config()
inputdbprop = config.get("inputdbprop", None)

# #### Connect to database
cur,cnx = db_connet(inputdbprop.get('dbname'),inputdbprop.get('dbusername'),inputdbprop.get('dbpassword'),inputdbprop.get('hostname'))
databasename=inputdbprop.get('dbname')
usedb="USE %s "% (databasename)
cur.execute(usedb)

print("Database connection estrablished...")

fetchsql="select symbol from nsesymbols;"
cur.execute(fetchsql)
symboltup= cur.fetchall()
symbol=[]
for i in symboltup:
    symbol.append(i[0])
print("Symbols fetched from file...")

dfupload=df[~df.SYMBOL.isin(symbol)]
print(str(len(dfupload))+' new symbols found in csv...')

dfupload.DATEOFLISTING=dfupload.DATEOFLISTING.apply(lambda x : dateconv(x))
print("Date conversion has been completed...")

for col in dfupload:
     dfupload[col]=dfupload[col].apply(lambda x : str(x))
dfupload=dfupload.reset_index(drop=True)

insertsql="INSERT INTO nsesymbols (symbol,nameofcompany,series,dateoflisting,paidupvalue,marketlot,isinnumber,facevalue) VALUES"
count=0
for i in dfupload.index:
    val=str(tuple(dfupload.iloc[i,:]))
    cur.execute(insertsql+' '+val+';')
    count=count+1
cur.close()
print(str(count)+'|'+str(datetime.datetime.now()-ptime))
