#!/usr/bin/python3
# coding: utf-8
#Libraries
import warnings
def warn(*args, **kwargs):
    warnings.filterwarnings("ignore",category=DeprecationWarning)
    pass
warnings.warn = warn

import numpy as np
import pandas as pd
import datetime
import pymysql
import statsmodels.api as sm
import ConfReader

def db_connet(database,user,password,host):
    cnx = pymysql.connect(user=user, password=password,host=host,database=database,charset='utf8', autocommit=True)
    cur=cnx.cursor()
    return cur,cnx

config = ConfReader.get_config()
InputDBprop = config.get("InputDBprop", None)

# #### Connect to database
cur,cnx = db_connet(InputDBprop.get('dbName'),InputDBprop.get('dbUserName'),InputDBprop.get('dbPassword'),InputDBprop.get('hostName'))
databasename=InputDBprop.get('dbName')
USEDB="USE %s "% (databasename)
cur.execute(USEDB)

# #### Import required data from database
dframeReturns=pd.DataFrame(columns=['symbol','timestamp','nsescrip','nseidx'])
returnssql="SELECT a.symbol, a.TIMESTAMP, (a.CLOSE/a.CLOSE_30)-1 as nsescrip, (b.CLOSE/b.CLOSE_30)-1 as nseidx FROM PRCS_RSLT a, PRCS_INDEX_RSLT b where a.TIMESTAMP = b.TIMESTAMP and b.INDEX_NAME = 'Nifty 50' and a.symbol in (select SYMBOL from nsedailybhavhist where timestamp > date_add(now(), interval -12 month) and series = 'EQ' group by symbol having count(1) > 135 order by SYMBOL) order by TIMESTAMP ;"

cur.execute(returnssql)
table_rows = cur.fetchall()
dframeReturns=pd.DataFrame(list(table_rows),columns=['symbol','timestamp','nsescrip','nseidx'])

count=0
ptime=datetime.datetime.today()
for i in list(dframeReturns.symbol.unique()):
    try:
        df=dframeReturns[dframeReturns.symbol==i].reset_index(drop=True)
        X=np.array(df.nseidx.apply(lambda x :float(x)))
        y=np.array(df.nsescrip.apply(lambda x :float(x)))
        X = sm.add_constant(X) 
        olsmodel=sm.OLS(y,X)
        olsmodel=olsmodel.fit()
        rslt_SYMBOL=i
        rslt_INDEXNAME='Nifty 50'
        rslt_start=min(dframeReturns.timestamp)
        rslt_end=max(dframeReturns.timestamp)
        rslt_RECORDS =len(X)
        rslt_SCRIP_MIN =min(y)
        rslt_INDX_MIN =min(X[:,1])
        rslt_SCRIP_MAX =max(y)
        rslt_INDX_MAX =max(X[:,1])
        rslt_SCRIP_MEDIAN =np.median(y)
        rslt_INDX_MEDIAN =np.median(X[:,1])
        rslt_SCRIP_MEAN =np.mean(y)
        rslt_INDX_MEAN =np.mean(X[:,1])
        rslt_SCRIP_VAR =np.var(y)
        rslt_INDX_VAR =np.var(X[:,1])
        rslt_SCRIP_COEFF_VAR =(float(np.std(y))/float(np.mean(y)))
        rslt_INDX_COEFF_VAR =(float(np.std(X[:,1]))/float(np.mean(X[:,1])))
        rslt_ALPHA=olsmodel.params[0]
        rslt_STE_ALPHA=olsmodel.bse[0]
        rslt_T_VAL_ALPHA=olsmodel.tvalues[0]
        rslt_BETA=olsmodel.bse[1]
        rslt_STE_BETA=0
        rslt_T_VAL_BETA=olsmodel.tvalues[1]
        rslt_RSQUARED=olsmodel.rsquared
        rslt_RSQUARED_ADJ=olsmodel.rsquared_adj
        insertSQL="INSERT INTO GNTHM_EQ_NSE_500_RETURNS_12MNTHS (SYMBOL, INDEX_NAME, START_DATE, END_DATE, RECORDS, SCRIP_MIN, SCRIP_MAX, SCRIP_MEDIAN, SCRIP_MEAN, SCRIP_VAR, SCRIP_COEFF_VAR, INDX_MIN, INDX_MAX, INDX_MEDIAN, INDX_MEAN, INDX_VAR, INDX_COEFF_VAR, ALPHA, STE_ALPHA, T_VAL_ALPHA, BETA, STE_BETA, T_VAL_BETA, RSQUARED, RSQUARED_ADJ) VALUES"+ str((rslt_SYMBOL,rslt_INDEXNAME,rslt_start,rslt_end,rslt_RECORDS ,rslt_SCRIP_MIN ,rslt_INDX_MIN ,rslt_SCRIP_MAX ,rslt_INDX_MAX ,rslt_SCRIP_MEDIAN ,rslt_INDX_MEDIAN ,rslt_SCRIP_MEAN ,rslt_INDX_MEAN ,rslt_SCRIP_VAR ,rslt_INDX_VAR ,rslt_SCRIP_COEFF_VAR ,rslt_INDX_COEFF_VAR ,rslt_ALPHA,rslt_STE_ALPHA,rslt_T_VAL_ALPHA,rslt_BETA,rslt_STE_BETA,rslt_T_VAL_BETA,rslt_RSQUARED,rslt_RSQUARED_ADJ))+";"
        cur.execute(insertSQL)
        count=count+1
        ltime=datetime.datetime.today()
        print(str(rslt_SYMBOL)+" | "+str(ltime-ptime)+" | count "+str(rslt_RECORDS))
    except Exception as E:
        print("Error : "+str(E))
