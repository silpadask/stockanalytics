#!/usr/bin/env python3
# coding: utf-8
# packages
import warnings
warnings.filterwarnings("ignore")
import conf_reader

def warn(*args, **kwargs):
    warnings.filterwarnings("ignore",category=DeprecationWarning)
    pass
warnings.warn = warn

import numpy as np
import pandas as pd
import re
import datetime
import time
import pymysql
print('corpact clean execution started...')
#Basic functions
def div(word,i):
    if word in i.split():
        i=word[0]
    else:
        i=''
    return(i)
def flt_conv(s):
    lst=[]
    for i in s.split():
        try:
            result = float(i)
            lst.append(result)
        except:
            continue
    return(lst)
def val_create(lst):
    try:
        for i in lst:
            return(i)
    except:
        pass
def replace_str(i,j):
    df_clean.purpose=df_clean.purpose.apply(lambda x : x.replace(i,j))
def replaced_nc(p):
    for k in p:
        replace_str(k,' ')

#Conversion factor functions        
def flag(text):
    lst=[]
    for i in ['dividend','split','bonus','rights']: 
        if len(div(i,text))>0:
            lst.append(div(i,text).upper())
    return(''.join(sorted(lst)))
def split_cov(text):
    ls=val_create(re.findall('spl[i]?t.*?(\d+).*?(\d+).*?',text))
    try:
        val=float(ls[1])/float(ls[0])
        return(val)
    except:
        return(1)
def bonus_cov(text):
    ls=val_create(re.findall('bonus.*?(\d+).*?(\d+).*?',text))
    try:
        val=(float(ls[1])/(float(ls[0])+float(ls[1])))
        return(val)
    except:
        return(1)
def rights_conv(text,cumprice):
    try:
        ratio=val_create(re.findall('right.*?(\d+).*?(\d+).*?',text))
        val=val_create(re.findall('right.*?\d+.*?\d+.*?(\d+).*?',text))
        return((float(ratio[1])*float(cumprice))/((float(ratio[1])*float(cumprice))+(float(ratio[0])*float(val))))
    except:
        return(1)
def div_conv(text,cumprice):
    sdv=re.findall('special dividend.*?(\d?[\.\*]?\d+).*?',text)
    sdv=val_create([float(i) for i in sdv])
    idv=re.findall('interim dividend.*?(\d?[\.\*]?\d+).*?',text)
    idv=val_create([float(i) for i in idv])
    div=re.findall("dividen?d.*?(\d?[\.\*]?\d+).*?",text)
    div=[float(i) for i in div]
    dis=re.findall("dist.*?(\d?[\.\*]?\d+).*?",text)
    dis=val_create([float(i) for i in dis])
    #Removing duplicate dividend values
    for i in div:
        try:
            div=div.remove(sdv)
            div=div.remove(idv)
        except:
            pass
    div=val_create(div)
    lst=[]
    
    for i in [sdv,div,idv,dis]:
        try:
            lst.append(float(i))
        except:
            pass
    div_tot=sum(lst)
    return(1-(float(div_tot)/float(cumprice)))
def div_total(text):
    sdv=re.findall('special dividend.*?(\d?[\.\*]?\d+).*?',text)
    sdv=val_create([float(i) for i in sdv])
    idv=re.findall('interim dividend.*?(\d?[\.\*]?\d+).*?',text)
    idv=val_create([float(i) for i in idv])
    div=re.findall("dividen?d.*?(\d?[\.\*]?\d+).*?",text)
    div=[float(i) for i in div]
    dis=re.findall("dist.*?(\d?[\.\*]?\d+).*?",text)
    dis=val_create([float(i) for i in dis])
    #Removing duplicate dividend values
    for i in div:
        try:
            div=div.remove(sdv)
            div=div.remove(idv)
        except:
            pass
    div=val_create(div)
    lst=[]
    for i in [sdv,div,idv,dis]:
        try:
            lst.append(float(i))
        except:
            pass
    return(sum(lst))

# #### Connect to database

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

# #### Import required data from database
fetchsql="select a.SYMBOL,a.COMPANY,a.SERIES,a.FACEVALUE,a.purpose,a.exdate,a.recorddate,a.bcstartdate,a.bcenddate,a.downday, (SELECT close FROM nsedailybhavhist WHERE symbol = a.symbol AND series = a.series AND timestamp = a.exdate) AS ext_price, c.close as cum_price, c.timestamp as cum_date from nsecorporateaction a inner join nsedailybhavhist c on a.symbol=c.symbol and a.series=c.series and c.timestamp=(SELECT MAX(timestamp) FROM nsedailybhavhist WHERE symbol = a.SYMBOL AND series = a.series AND timestamp < a.exdate) where not exists (select 1 from nsecorpactclean b where a.symbol=b.symbol and a.series=b.series and a.exdate=b.exdate);"
cur.execute(fetchsql)
table_rows = cur.fetchall()

#Load data into a dataframe
df=pd.DataFrame(list(table_rows),columns=['SYMBOL', 'COMPANY', 'SERIES', 'FACEVALUE', 'PURPOSE', 'EXDATE',
       'RECORDDATE', 'BCSTARTDATE', 'BCENDDATE', 'DOWNDAY','EXT_PRICE','CUMPRICE','CUMDATE'])
df.columns=map(str.lower,df.columns)
#Basic data cleaning
df_clean=df.dropna(thresh=3)
df_clean.purpose=df_clean.purpose.apply(lambda x : str(x).lower())
df_clean.purpose=df_clean.purpose.apply(lambda x : x.encode('ascii',errors='ignore').decode())
nc=['_','@','#','\n','\t','/','-',':']
replaced_nc(nc)
df_clean.purpose=df_clean.purpose.apply(lambda x : x.replace('rs.','rs '))
df_clean.purpose=df_clean.purpose.apply(lambda x : x.replace('re.','re '))
df_clean=df_clean.loc[~(df_clean.exdate=='0000-00-00')]
df_clean['numerical']=df_clean.purpose.apply(lambda x : flt_conv(x))
df_clean=df_clean[(df_clean.numerical.apply(lambda x :len(x)>0))&(df_clean.numerical.apply(lambda x :len(x)<5))].reset_index(drop=True)
df_clean=df_clean.drop(columns=['numerical'])

#Create flags for each action
df_clean['flag']=df_clean.purpose.apply(lambda x:flag(x))
#Create conversion factors
df_clean['split_val']=df_clean.purpose.apply(lambda x : split_cov(x))
df_clean['bonus_val']=df_clean.purpose.apply(lambda x : bonus_cov(x))
df_clean['rights_val']=0
for i in df_clean.index:
    df_clean.iloc[i,-1]=rights_conv(df_clean.iloc[i,4],df_clean.iloc[i,11])
df_clean['div_tot']=df_clean.purpose.apply(lambda x : div_total(x))
df_clean['rat_factor']=df_clean.split_val*df_clean.bonus_val*df_clean.rights_val
#Convert data to a string type
df_corpact_clean=df_clean.reset_index(drop=True)

for i in df_corpact_clean.index:
    df_corpact_clean.iloc[i,-2]=df_corpact_clean.iloc[i,-2]*df_corpact_clean.iloc[i,-1]

df_corpact_clean['div_val']=0
for i in df_corpact_clean.index:
    df_corpact_clean.iloc[i,-1]=div_conv(df_corpact_clean.iloc[i,4],df_clean.iloc[i,11])

df_corpact_clean['factor']=1
df_corpact_clean['factor']=df_corpact_clean.rat_factor*df_corpact_clean.div_val
df_corpact_clean=df_corpact_clean.drop(columns='rat_factor')
df_corpact_clean=df_corpact_clean.astype('str')

#Convert and clean date values
df_corpact_clean.downday=df_corpact_clean.downday.apply(lambda x : x.split(' ')[0])
df_corpact_clean.cumdate=df_corpact_clean.cumdate.apply(lambda x : x.split(' ')[0])
df_corpact_clean.exdate=df_corpact_clean.exdate.apply(lambda x : x.replace('None','1970-01-01'))
df_corpact_clean.recorddate=df_corpact_clean.recorddate.apply(lambda x : x.replace('None','1970-01-01'))
df_corpact_clean.bcenddate=df_corpact_clean.bcenddate.apply(lambda x : x.replace('None','1970-01-01'))
df_corpact_clean.bcstartdate=df_corpact_clean.bcstartdate.apply(lambda x : x.replace('None','1970-01-01'))
df_corpact_clean.downday=df_corpact_clean.downday.apply(lambda x : x.replace('None','1970-01-01'))
df_corpact_clean.ext_price=df_corpact_clean.ext_price.apply(lambda x : x.replace('None','0'))
df_corpact_clean.cumprice=df_corpact_clean.cumprice.apply(lambda x : x.replace('None','0'))

#Round the float data to 2 decimal places
df_corpact_clean.split_val=df_corpact_clean.split_val.apply(lambda x : str(round(float(x),2)))
df_corpact_clean.bonus_val=df_corpact_clean.bonus_val.apply(lambda x : str(round(float(x),2)))
df_corpact_clean.rights_val=df_corpact_clean.rights_val.apply(lambda x : str(round(float(x),2)))
df_corpact_clean.div_val=df_corpact_clean.div_val.apply(lambda x : str(round(float(x),2)))
df_corpact_clean.factor=df_corpact_clean.factor.apply(lambda x : str(round(float(x),2)))
df_corpact_clean.div_tot=df_corpact_clean.div_tot.apply(lambda x : str(round(float(x),2)))

#Create a list for tuples of data to be inserted to database table
lst=[]
for i in df_corpact_clean.index:
    val=tuple(df_corpact_clean.iloc[i,:])
    lst.append(val)
data=''.join(str(lst)[1:-1].replace("\\",""))

#Insert statement to insert data to table
iquery='INSERT INTO nsecorpactclean (symbol, company, series, facevalue, purpose, exdate, recorddate, bcstartdate, bcenddate, downday, ext_price, cumprice, cumdate, flag, split_val, bonus_val, rights_val, div_tot, div_val, factor) values '+data
try:
    cur.execute(iquery)
    print(str(len(lst))+' rows inserted')
except:
    print('No records to insert')
