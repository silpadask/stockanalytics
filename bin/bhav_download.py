#!/usr/bin/env python3
# coding: utf-8
# packages

try:
    import warnings
    warnings.filterwarnings("ignore")
    import datetime
    import os
    import requests
    import urllib
    print('All libraries imported successfully...')
except Exception as E:
    print(E)
print("bhav_download execution started...")

def filedateconv(fname):
    y=int(fname[7:11])
    m=fname[4:7]
    d=int(fname[2:4])
    dct={'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}
    for k,v in dct.items():
        if m==k:
            m=int(v)
    return(datetime.date(y,m,d))
def month_conv(m):
    count=0
    for i in list(mdct.keys()):
        count=count+1
        if m==i:
            return(count)
print("Date parsing function created...")

mdct={'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}
#for yyyy in range(2019,datetime.datetime.now().year+1):
for yyyy in [datetime.date.today().year]:
    count=0
    ytime=datetime.datetime.now()
    for mmm in list(mdct.keys()):
        for dd in range(1,32):
            try:
                if datetime.date(int(yyyy),int(month_conv(mmm)),int(dd))<=datetime.date.today():
                    if len(str(dd))==1:
                        dd='0'+str(dd)
                    fname="cm"+str(dd)+mmm+str(yyyy)+"bhav.csv.zip"
                    if fname not in os.listdir('../data/'):
                        page="https://www1.nseindia.com/content/historical/EQUITIES/"+str(yyyy)+"/"+mmm+"/"+fname
                        try:
                            response = requests.get(page)
                            if response.status_code==200:
                                with open(os.path.join("../data/", fname), 'wb') as f:
                                    f.write(response.content)
                                    count=count+1
                                    print(fname)
                        except Exception as E:
                            print(E)
            except:
                pass
    print('|'+str(yyyy)+'|'+str(count)+' working days|'+str(datetime.datetime.now()-ytime)+'|')


