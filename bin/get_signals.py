#!/usr/bin/python3
# coding: utf-8

import numpy as np
import pandas as pd
import datetime
import dateutil
import pymysql
ptime=datetime.datetime.now()
import warnings
import conf_reader
warnings.filterwarnings("ignore")
print('get signal execution started...')
print("Required libraries imported...")

# create a bucket of values for a value series
def bucket_values(valseries,days):
    bkt=list(valseries[-days:])
    return(bkt)

# create a slope for a list of values
def trend_detector_slope(lst):
    try:
        val=('positive' if np.polyfit(range(len(lst)), lst, 1)[-2]>0 else ('negative' if np.polyfit(range(len(lst)), lst, 1)[-2]<0 else 'neutral'))
    except:
        val='neutral'
    return(val)

# create a truely incremental/decremental check for a list of values
def trend_detector_pure(lst):
    inc=(all(i<j for i,j in zip(lst, lst[1:])))
    #print(inc)
    dec=(all(i>j for i,j in zip(lst, lst[1:])))
    #print(dec)
    if inc==True:
        return('positive')
    elif dec==True:
        return('negative')
    else:
        return('neutral')

def returns(close,days):
    diff=close.values[-1]-close.values[-days]
    return(100*diff/close.values[-1])
def sma(close,days):
    try:
        sma=np.mean(bucket_values(close,days))
    except:
        pass
    return(sma)
def ema(series, periods, fillna=False):
    if fillna:
        return (series.ewm(span=periods, min_periods=0).mean())
    return (series.ewm(span=periods, min_periods=periods).mean())

# Atleast 2 values required to find if a crossover exists
def crossover_golddeath(close):
    sma50=close.rolling(50).mean()[-2:]
    sma200=close.rolling(200).mean()[-2:]
    val=sma50-sma200
    val=val.apply(lambda x : 'gold' if x>0 else 'death')
    cr=(val==val.shift()).apply(lambda x : 'cross' if x is False else 'continue')
    return((val+' '+cr).values[-1])

def crossover_sma(close,days):
    sma=close.rolling(days).mean()
    val=(close-sma).apply(lambda x : 'bullish' if x>0 else 'bearish')
    cr=(val==val.shift()).apply(lambda x : 'cross' if x is False else 'continue')
    return((val+' '+cr).values[-1])
def decisionmaker_sma(c,sma,param,metric):
    if metric=='percent':
        percent=param
    elif metric=='days':
        mn=min(bucket_values(sma,param))
        mx=max(bucket_values(sma,param))
        percent=(100*(mx-mn)/(2*c.values[-1]))
    else:
        print('Error: Invalid metric')
    try:
        val=100*(c[-1]-sma[-1])/c[-1]
    except:
        val=0
    if val>percent:
        return('buy')
    elif val>percent/4:
        return('strong buy')
    elif val>np.negative(percent/4):
        return('neutral')
    elif val>np.negative(percent):
        return('strong sell')
    else:
        return('sell')

def crossover_ema(close,days):
    ema=close.ewm(span=days, min_periods=days).mean()
    val=(close-ema).apply(lambda x : 'bullish' if x>0 else 'bearish')
    cr=(val==val.shift()).apply(lambda x : 'cross' if x is False else 'continue')
    return((val+' '+cr).values[-1])

def decisionmaker_ema(c,ema,param,metric):
    if metric=='percent':
        percent=param
    elif metric=='days':
        mn=min(bucket_values(ema,param))
        mx=max(bucket_values(ema,param))
        percent=(100*(mx-mn)/(2*c.values[-1]))
    else:
        print('Error: Invalid metric')
    try:
        val=100*(c[-1]-ema[-1])/c[-1]
    except:
        val=0
    if val>percent:
        return('buy')
    elif val>percent/4:
        return('strong buy')
    elif val>np.negative(percent/4):
        return('neutral')
    elif val>np.negative(percent):
        return('strong sell')
    else:
        return('sell')

def crossover_macd(close):
    close=close[-27:]
    macd=(close.ewm(span=12, adjust=False).mean()-close.ewm(span=26, adjust=False).mean())
    signal=(macd.ewm(span=9, adjust=False).mean())
    val=(macd-signal).apply(lambda x : 'bullish' if x>0 else 'bearish')
    cr=(val==val.shift()).apply(lambda x : 'crossover' if x is False else 'continue')
    return((val+' '+cr).values[-1])

def decisionmaker_macd(s):
    s=s[-30:]
    macd=(s.ewm(span=12,adjust=False).mean()-s.ewm(span=26, adjust=False).mean())
    signal=macd.ewm(span=9,adjust=False).mean()
    return((macd-signal).apply(lambda x : 'buy' if x>0 else 'sell'))

def rsi(s, n=14, fillna=False):
    diff = s.diff(1)
    which_dn = diff < 0
    up, dn = diff, diff*0
    up[which_dn], dn[which_dn] = 0, -up[which_dn]
    emaup = ema(up, n, fillna)
    emadn = ema(dn, n, fillna)
    rsi = 100 * emaup / (emaup + emadn)
    if fillna:
        rsi = rsi.replace([np.inf, -np.inf], np.nan).fillna(50)
    return pd.Series(rsi, name='rsi')

def decisionmaker_rsi(rsival, days, metric):
    trend = bucket_values(rsival, days)
    if (metric=='slope'):
        trend = trend_detector_slope(trend)
    elif (metric=='truelyid'):
        trend = trend_detector_pure(trend)
    else:
        return('Error: Invalid metric')
    rsival=rsival.values[-1]
    if trend=='positive':
        if rsival<=20:
            return('strong buy')
        elif rsival<=30:
            return('buy')
        else:
            return('neutral')
    elif trend=='negative':
        if rsival>=80:
            return('strong sell')
        elif rsival>=70:
            return('sell')
        else:
            return('neutral')
    else:
        return('neutral')

# single value
def stochrsivalue(rsi,period):
    bkt=bucket_values(rsi,period)
    rsi=rsi.values[-1]
    try:
        mn=min(bkt)
        mx=max(bkt)
    except:
        mn=np.nan
        mx=np.nan
    strsi=round((rsi-mn)/(mx-mn),3)
    return(strsi)

# single value
def decisionmaker_stochrsi(stochrsi):
    if stochrsi>0.9:
        return('strong sell')
    elif stochrsi>0.8:
        return('sell')
    elif stochrsi<0.1:
        return('strong buy')
    elif stochrsi<0.2:
        return('buy')
    else:
        return('neutral')

# create a trend indicator
def trend_ind(trend,metric):
    if (metric=='slope'):
        trend = trend.apply(lambda x : trend_detector_slope(x) if len(x)!=0 else np.nan)
        return(trend)
    elif (metric=='truelyid'):
        trend = trend.apply(lambda x : trend_detector_pure(x) if len(x)!=0 else np.nan)
        return(trend)
    else:
        return('Error: Invalid metric')

# single value
def rsi_reading(rsival):
    rsireading=('over sold' if rsival<=20 else ('over bought' if rsival>=80 else 'neutral'))
    return(rsireading)

def rsi_divergence(price,rsival,days,metric):
    price=price[-(days+1):]
    rsival=rsival[-(days+1):]
    # bucket values
    rsitrend = bucket_values(rsival, days)
    pricetrend=bucket_values(price,days)
    # check trend if positive or negative
    if (metric=='slope'):
        rsitrend = trend_detector_slope(rsitrend)
        pricetrend = trend_detector_slope(pricetrend)
    elif (metric=='truelyid'):
        rsitrend = trend_detector_pure(rsitrend)
        pricetrend = trend_detector_pure(pricetrend)
    else:
        return('Error: Invalid metric')
    # check and assign the conditions
    if (rsitrend=='positive')&(pricetrend=='negative'):
        return('Bullish divergence')
    elif (rsitrend=='negative')&(pricetrend=='positive'):
        return('Bearish divergence')
    else:
        return('No divergence')

# single value
def get_min_max(x1,x2,var):
    if var=='min':
        return(np.min([x1,x2]))
    else:
        return(np.max([x1,x2]))

def adx_pos(high, low, close, n=14, fillna=False):
    high=high[-16:]
    low=low[-16:]
    close=close[-16:]
    cs = close.shift(1)
    pdm = high.combine(cs, lambda x1, x2: get_min_max(x1, x2, 'max'))
    pdn = low.combine(cs, lambda x1, x2: get_min_max(x1, x2, 'min'))
    tr = pdm - pdn
    trs_initial = np.zeros(n-1)
    trs = np.zeros(len(close) - (n - 1))
    trs[0] = tr.dropna()[0:n].sum()
    tr = tr.reset_index(drop=True)
    for i in range(1, len(trs)-1):
        trs[i] = trs[i-1] - (trs[i-1]/float(n)) + tr[n+i]
    up = high - high.shift(1)
    dn = low.shift(1) - low
    pos = abs(((up > dn) & (up > 0)) * up)
    neg = abs(((dn > up) & (dn > 0)) * dn)
    dip_mio = np.zeros(len(close) - (n - 1))
    dip_mio[0] = pos.dropna()[0:n].sum()
    pos = pos.reset_index(drop=True)
    for i in range(1, len(dip_mio)-1):
        dip_mio[i] = dip_mio[i-1] - (dip_mio[i-1]/float(n)) + pos[n+i]
    dip = np.zeros(len(close))
    for i in range(1, len(trs)-1):
        dip[i+n] = 100 * (dip_mio[i]/trs[i])
    dip = pd.Series(data=dip, index=close.index)
    if fillna:
        dip = dip.replace([np.inf, -np.inf], np.nan).fillna(20)
    return(list(dip)[-1])

# single value
def adx_neg(high, low, close, n=14, fillna=False):
    high=high[-16:]
    low=low[-16:]
    close=close[-16:]
    cs = close.shift(1)
    pdm = high.combine(cs, lambda x1, x2: get_min_max(x1, x2, 'max'))
    pdn = low.combine(cs, lambda x1, x2: get_min_max(x1, x2, 'min'))
    tr = pdm - pdn
    trs_initial = np.zeros(n-1)
    trs = np.zeros(len(close) - (n - 1))
    trs[0] = tr.dropna()[0:n].sum()
    tr = tr.reset_index(drop=True)
    for i in range(1, len(trs)-1):
        trs[i] = trs[i-1] - (trs[i-1]/float(n)) + tr[n+i]
    up = high - high.shift(1)
    dn = low.shift(1) - low
    pos = abs(((up > dn) & (up > 0)) * up)
    neg = abs(((dn > up) & (dn > 0)) * dn)
    din_mio = np.zeros(len(close) - (n - 1))
    din_mio[0] = neg.dropna()[0:n].sum()
    neg = neg.reset_index(drop=True)
    for i in range(1, len(din_mio)-1):
        din_mio[i] = din_mio[i-1] - (din_mio[i-1]/float(n)) + neg[n+i]
    din = np.zeros(len(close))
    for i in range(1, len(trs)-1):
        din[i+n] = 100 * (din_mio[i]/float(trs[i]))
    din = pd.Series(data=din, index=close.index)
    if fillna:
        din = din.replace([np.inf, -np.inf], np.nan).fillna(20)
    return(list(din)[-1])

# single value
def adx(high, low, close, n=14, fillna=False):
    high=high[-30:].reset_index(drop=True)
    low=low[-30:].reset_index(drop=True)
    close=close[-30:].reset_index(drop=True)
    cs = close.shift(1)
    pdm = high.combine(cs, lambda x1, x2: get_min_max(x1, x2, 'max'))
    pdn = low.combine(cs, lambda x1, x2: get_min_max(x1, x2, 'min'))
    tr = pdm - pdn
    trs_initial = np.zeros(n-1)
    trs = np.zeros(len(close) - (n - 1))
    trs[0] = tr.dropna()[0:n].sum()
    tr = tr.reset_index(drop=True)
    for i in range(1, len(trs)-1):
        trs[i] = trs[i-1] - (trs[i-1]/float(n)) + tr[n+i]
    up = high - high.shift(1)
    dn = low.shift(1) - low
    pos = abs(((up > dn) & (up > 0)) * up)
    neg = abs(((dn > up) & (dn > 0)) * dn)
    dip_mio = np.zeros(len(close) - (n - 1))
    dip_mio[0] = pos.dropna()[0:n].sum()
    pos = pos.reset_index(drop=True)
    for i in range(1, len(dip_mio)-1):
        dip_mio[i] = dip_mio[i-1] - (dip_mio[i-1]/float(n)) + pos[n+i]
    din_mio = np.zeros(len(close) - (n - 1))
    din_mio[0] = neg.dropna()[0:n].sum()
    neg = neg.reset_index(drop=True)
    for i in range(1, len(din_mio)-1):
        din_mio[i] = din_mio[i-1] - (din_mio[i-1]/float(n)) + neg[n+i]
    dip = np.zeros(len(trs))
    for i in range(len(trs)):
        dip[i] = 100 * (dip_mio[i]/trs[i])
    din = np.zeros(len(trs))
    for i in range(len(trs)):
        din[i] = 100 * (din_mio[i]/trs[i])
    dx = 100 * np.abs((dip - din) / (dip + din))
    adx = np.zeros(len(trs))
    adx[n] = dx[0:n].mean()
    for i in range(n+1, len(adx)):
        adx[i] = ((adx[i-1] * (n - 1)) + dx[i-1]) / float(n)
    adx = np.concatenate((trs_initial, adx), axis=0)
    adx = pd.Series(data=adx, index=close.index)
    if fillna:
        adx = adx.replace([np.inf, -np.inf], np.nan).fillna(20)
    return(list(adx)[-1])

def adx_reading(adxval):
    if adxval<=25:
        return('weak')
    elif adxval<=50:
        return('strong')
    elif adxval<=75:
        return('very strong')
    else:
        return('extremely strong')

# single value
def adx_action(high, low, close):
    high=high[-30:]
    low=low[-30:]
    close=close[-30:]
    adxval=adx(high, low, close, n=14, fillna=False)
    adxval=('weak' if adxval<=25 else ('strong' if adxval<=50 else ('very strong' if adxval<=75 else 'extremely strong')))
    dipos=adx_pos(high, low, close, n=14, fillna=False)
    dineg=adx_neg(high, low, close, n=14, fillna=False)
    diflag=(1 if (dipos-dineg)>0 else 0)
    if (diflag==1)&((adxval=='very strong')|(adxval=='extremely strong')):
        return('strong buy')
    elif (diflag==0)&((adxval=='very strong')|(adxval=='extremely strong')):
        return('strong sell')
    elif (diflag==1)&(adxval=='strong'):
        return('buy')
    elif (diflag==0)&(adxval=='strong'):
        return('sell')
    else:
        return('neutral')

def generatesignals(df):
    try:
        dsql="select max(timestamp) from nse.nsedailybhavhist where timestamp>(select max(timestamp) from nse.nsesignals where symbol="+"'"+sym+"'"+");"
        NSEDBCursor.execute(dsql)
        date=NSEDBCursor.fetchall()[0][0]
        df=df[df['symbol']==sym].sort_values('timestamp',ascending=True).reset_index(drop=True)
        tstamp=max(df['timestamp'])
        ptime=datetime.datetime.now()
        sma5=round(sma(df.close_adj,5),2)
        ema5=round(ema(df.close_adj,5).values[-1],2)
        smadecision5=decisionmaker_sma(df.close_adj,df.close_adj.rolling(5).mean(),3,'days')
        smasignal5=crossover_sma(df.close_adj,5)
        sma20=round(sma(df.close_adj,20),2)
        ema20=round(ema(df.close_adj,20).values[-1],2)
        smadecision20=decisionmaker_sma(df.close_adj,df.close_adj.rolling(20).mean(),3,'days')
        smasignal20=crossover_sma(df.close_adj,20)
        sma50=round(sma(df.close_adj,50),2)
        ema50=round(ema(df.close_adj,50).values[-1],2)
        smadecision50=decisionmaker_sma(df.close_adj,df.close_adj.rolling(50).mean(),3,'days')
        smasignal50=crossover_sma(df.close_adj,50)
        sma100=round(sma(df.close_adj,100),2)
        ema100=round(ema(df.close_adj,100).values[-1],2)
        smadecision100=decisionmaker_sma(df.close_adj,df.close_adj.rolling(100).mean(),3,'days')
        smasignal100=crossover_sma(df.close_adj,100)
        sma200=round(sma(df.close_adj,200),2)
        ema200=round(ema(df.close_adj,200).values[-1],2)
        smadecision200=decisionmaker_sma(df.close_adj,df.close_adj.rolling(200).mean(),3,'days')
        smasignal200=crossover_sma(df.close_adj,200)
        sma240=round(sma(df.close_adj,240),2)
        ema240=round(ema(df.close_adj,240).values[-1],2)
        smadecision240=decisionmaker_sma(df.close_adj,df.close_adj.rolling(240).mean(),3,'days')
        smasignal240=crossover_sma(df.close_adj,240)
        rsivalue= round(rsi(df['close_adj'], n=14,fillna=False).values[-1],2)
        rsisignal=decisionmaker_rsi(rsi(df.close_adj),3,'slope')
        rsidivergence=rsi_divergence(df['close_adj'],rsi(df['close_adj'], n=14,fillna=False),3,'slope')
        goldencrosssignal=crossover_golddeath(df.close_adj)
        macdvalue = round(((df.close_adj.ewm(span=12, adjust=False).mean()-df.close_adj.ewm(span=26, adjust=False).mean()).apply(lambda x : round(x,2))).values[-1],2)
        signalvalue=round(((df.close_adj.ewm(span=12, adjust=False).mean()-df.close_adj.ewm(span=26, adjust=False).mean()).apply(lambda x : round(x,2))).ewm(span=9, adjust=False).mean().apply(lambda x : round(x,2)).values[-1],2)
        macddecision=decisionmaker_macd(df.close_adj).values[-1]
        macdsignal=crossover_macd(df.close_adj)
        stochrsival=round(stochrsivalue(rsi(df.close_adj,14),10),2)
        stochrsidecision=decisionmaker_stochrsi(stochrsival)
        adxsignal=adx_reading(adx(df['high_adj'],df['low_adj'],df['close_adj']))
        adxdecision=adx_action(df['high_adj'],df['low_adj'],df['close_adj'])
        lst=[str(sym),str('EQ'),str(tstamp),str(sma5),str(ema5),str(smadecision5),str(smasignal5),str(sma20),str(ema20),str(smadecision20),str(smasignal20),str(sma50),str(ema50),str(smadecision50),str(smasignal50),str(sma100),str(ema100),str(smadecision100),str(smasignal100),str(sma200),str(ema200),str(smadecision200),str(smasignal200),str(sma240),str(ema240),str(smadecision240),str(smasignal240),str(rsivalue),str(rsisignal),str(rsidivergence),str(goldencrosssignal),str(macdvalue ),str(signalvalue),str(macddecision),str(macdsignal),str(stochrsival),str(stochrsidecision),str(adxsignal),str(adxdecision)]
        lst=[i.replace('nan', '0') for i in lst]
#        return((sym,'EQ',tstamp,sma5,ema5,smadecision5,smasignal5,sma20,ema20,smadecision20,smasignal20,sma50,ema50,smadecision50,smasignal50,sma100,ema100,smadecision100,smasignal100,sma200,ema200,smadecision200,smasignal200,sma240,ema240,smadecision240,smasignal240,rsivalue,rsisignal,rsidivergence,goldencrosssignal,macdvalue ,signalvalue,macddecision,macdsignal,stochrsival,stochrsidecision,adxsignal,adxdecision))
        return(tuple(lst))
    except:
        pass
print("Function for signal mining created...")

def db_connet(database,user,password,host):
    cnx = pymysql.connect(user=user, password=password,host=host,database=database,charset='utf8', autocommit=True)
    cur=cnx.cursor()
    return cur,cnx

config = conf_reader.get_config()
inputdbprop = config.get("inputdbprop", None)
database_name=inputdbprop.get('dbname')

# #### Connect to database
cur,cnx = db_connet(inputdbprop.get('dbname'),inputdbprop.get('dbusername'),inputdbprop.get('dbpassword'),inputdbprop.get('hostname'))
databasename=inputdbprop.get('dbname')
usedb="USE %s "% (databasename)
cur.execute(usedb)

print("Database connection established...")

fsql="select symbol,series,open_adj,high_adj,low_adj,close_adj,last_adj,tottrdqty_adj,tottrdval,timestamp from nsedailybhavhist where timestamp > (SELECT DATE_SUB(date(sysdate()), INTERVAL 1 YEAR)) and series='EQ' order by timestamp;"
cur.execute(fsql)
print("Required data fetched from database...")
df=pd.DataFrame(list(cur.fetchall()),columns=['symbol', 'series', 'open_adj', 'high_adj', 'low_adj', 'close_adj', 'last_adj', 'tottrdqty_adj', 'tottrdval', 'timestamp'])

fetchsymbols="select symbol from nsesymbols;"
cur.execute(fetchsymbols)
symbols=list((pd.DataFrame(list(cur.fetchall())))[0])

count=0
lst=[]
lstN=[]
for sym in symbols:
    tp=generatesignals(df[df.symbol==sym].sort_values('timestamp').reset_index(drop=True))
    if tp!=None:
        lst.append(tp)
        count=count+1
        if (count!=0)&((count%100)==0):
            print(count)
    else:
        lstN.append(tp)

insertSQL="INSERT INTO nsesignals (symbol, series, timestamp, smavalue5, emavalue5, smadecision5, smasignal5, smavalue20, emavalue20, smadecision20, smasignal20, smavalue50, emavalue50, smadecision50, smasignal50, smavalue100, emavalue100, smadecision100,smasignal100, smavalue200, emavalue200,smadecision200, smasignal200, smavalue240,emavalue240, smadecision240, smasignal240, rsivalue,rsisignal, rsidivergence, goldencrosssignal, macdvalue,signalvalue, macddecision, macdsignal, stochrsivalue, stochrsidecision, adxsignal, adxdecision) VALUES "

val=str(lst)[1:-1]
isql=insertSQL+val
try:
    cur.execute(isql)
except:pass
print(str(datetime.datetime.now()-ptime))
