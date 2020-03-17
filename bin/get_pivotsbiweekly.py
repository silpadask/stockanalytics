#!/usr/bin/python3
# coding: utf-8
import numpy as np
import pandas as pd
import datetime
import dateutil
import pymysql
import conf_reader
print('get pivotsbiweekly execution started...')
def pivot_classic(h,l,c):
    pivot=(h+l+c)/3
    r3=h+2*(pivot-l)
    r2=pivot+(h-l)
    r1=(2*pivot)-l
    s1=(2*pivot)-h
    s2=pivot-(h-l)
    s3=l-2*(h-pivot)
    val=pd.DataFrame([round(pivot,2),round(r1,2),round(r2,2),round(r3,2),round(s1,2),round(s2,2),round(s3,2)]).T.rename(columns={0:'pivot_classic',1:'r1_classic',2:'r2_classic',3:'r3_classic',4:'s1_classic',5:'s2_classic',6:'s3_classic'})
    return(val)

def pivot_camarilla(h,l,c):
    pivot=(h+l+c)/3
    r3=c+(h-l)*1.1/4
    r2=c+(h-l)*1.1/6
    r1=c+(h-l)*1.1/12
    s1=c-(h-l)*1.1/12
    s2=c-(h-l)*1.1/6
    s3=c-(h-l)*1.1/4
    val=pd.DataFrame([round(pivot,2),round(r1,2),round(r2,2),round(r3,2),round(s1,2),round(s2,2),round(s3,2)]).T.rename(columns={0:'pivo_camarillat',1:'r1_camarilla',2:'r2_camarilla',3:'r3_camarilla',4:'s1_camarilla',5:'s2_camarilla',6:'s3_camarilla'})
    return(val)

def pivot_fibonacci(h,l,c):
    pivot=(h+l+c)/3
    r3=pivot+1.000*(h-l)
    r2=pivot+0.618*(h-l)
    r1=pivot+0.382*(h-l)
    s1=pivot-0.382*(h-l)
    s2=pivot-0.618*(h-l)
    s3=pivot-1.000*(h-l)
    val=pd.DataFrame([round(pivot,2),round(r1,2),round(r2,2),round(r3,2),round(s1,2),round(s2,2),round(s3,2)]).T.rename(columns={0:'pivot_fibonacci',1:'r1_fibonacci',2:'r2_fibonacci',3:'r3_fibonacci',4:'s1_fibonacci',5:'s2_fibonacci',6:'s3_fibonacci'})
    return(val)

def pivot_woodie(h,l,c):
    pivot=(h+l+2*c)/4
    r2=pivot+(h-l)
    r1=(2*pivot)-l
    r3=h+2*(pivot-l)
    s1=(2*pivot)-h
    s2=pivot-(h-l)
    s3=l-2*(h-pivot)
    val=pd.DataFrame([round(pivot,2),round(r1,2),round(r2,2),round(r3,2),round(s1,2),round(s2,2),round(s3,2)]).T.rename(columns={0:'pivot_woodie',1:'r1_woodie',2:'r2_woodie',3:'r3_woodie',4:'s1_woodie',5:'s2_woodie',6:'s3_woodie'})
    return(val)

def pivots(s,t,h,l,c):
    pclc=pivot_classic(h,l,c)
    pfib=pivot_fibonacci(h,l,c)
    pcam=pivot_camarilla(h,l,c)
    pwod=pivot_woodie(h,l,c)
    return(pd.concat([s,t,pclc,pfib,pcam,pwod],axis=1))

def bucket_values(valseries,days):
    bkt=list(valseries[-days:])
    return(bkt)

print("Required functions created...")

def db_connet(database,user,password,host):
    cnx = pymysql.connect(user=user, password=password,host=host,database=database,charset='utf8', autocommit=True)
    cur=cnx.cursor()
    return cur,cnx

config = conf_reader.get_config()
inputdbprop = config.get("inputdbprop", None)

# #### Connect to database
cur,cnx = db_connet(inputdbprop.get('dbname'),inputdbprop.get('dbusername'),inputdbprop.get('dbpassword'),inputdbprop.get('hostname'))
databasename=inputdbprop.get('dbname')
usedb="USE %s "% (databasename)
cur.execute(usedb)

print("Database connection established...")
cur.execute("select symbol from nsesymbols;")
symbols=cur.fetchall()
lst=[]
for sym in symbols:
    lst.append(sym[0])

count=0
ptime=datetime.datetime.now()

for sym in lst:
    count=count+1
    cur.execute("select * from nsedailybhavhist where timestamp between (select date_sub(max(timestamp), interval 15 day) from nsedailybhavhist) and (select max(timestamp) from nsedailybhavhist) and series='EQ' and symbol="+"'"+sym+"'"+" order by timestamp;")
    df_sym=pd.DataFrame(list(cur.fetchall()),columns=['SYMBOL','SERIES','OPEN','HIGH','LOW','CLOSE','LAST','PREVCLOSE','TOTTRDQTY','TOTTRDVAL','TIMESTAMP','TOTALTRADES','ISIN','FACTOR','OPEN_ADJ','HIGH_ADJ','LOW_ADJ','CLOSE_ADJ','LAST_ADJ','TOTTRDQTY_ADJ'])
    df_sym.columns=map(str.lower,df_sym.columns)
    df_sym=df_sym.sort_values('timestamp', ascending=True).reset_index(drop=True)
    try:
        h=max(bucket_values(df_sym.high_adj,len(df_sym)))
        l=min(bucket_values(df_sym.low_adj,len(df_sym)))
        c=list(df_sym.close)[-1]
        df_sym=pivots(df_sym['symbol'],df_sym['timestamp'],float(h),float(l),float(c))
        if len(df_sym)==0:
            print('No records to insert...')
        else:
            for col in df_sym.columns:
                df_sym[col]=df_sym[col].apply(lambda x : str(x))
            df_sym['timestamp']=max(df_sym['timestamp'])
            insertSQL="INSERT INTO nsepivotsbiweekly (symbol, timestamp, pivot_classic, r1_classic, r2_classic, r3_classic, s1_classic, s2_classic, s3_classic, pivot_fibonacci, r1_fibonacci, r2_fibonacci, r3_fibonacci, s1_fibonacci, s2_fibonacci, s3_fibonacci, pivot_camarilla, r1_camarilla, r2_camarilla, r3_camarilla, s1_camarilla, s2_camarilla, s3_camarilla, pivot_woodie, r1_woodie, r2_woodie, r3_woodie, s1_woodie, s2_woodie, s3_woodie) VALUES "   
            val=str(list(df_sym.itertuples(index=False, name=None))[0])
            isql=insertSQL+val
            cur.execute(isql)
        if count%100==0:
            print(str(count)+"|"+str(datetime.datetime.now()-ptime))
    except:
        pass
print(str(count)+"|"+str(datetime.datetime.now()-ptime))
