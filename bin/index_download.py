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
print("index_download execution started...")  
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

mdct={'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}

#for yyyy in range(2010,datetime.datetime.now().year):
for yyyy in [datetime.datetime.now().year]:
    count=0
    ytime=datetime.datetime.now()
    for mm in list(mdct.values()):
        for dd in range(1,32):
            try:
                if datetime.date(int(yyyy),int(mm),int(dd))<=datetime.date.today():
                    if len(str(dd))==1:
                        dd='0'+str(dd)
                    if len(str(mm))==1:
                        mm='0'+str(mm)
                    fname="ind_close_all_"+str(dd)+str(mm)+str(yyyy)+".csv"
                    if fname not in os.listdir('../data/'):
                        page="https://www1.nseindia.com/content/indices/"+fname
                        try:
                            response = requests.get(page)
                            if response.status_code==200:
                                with open(os.path.join("../data/", fname), 'wb') as f:
                                    f.write(response.content)
                                    count=count+1
                                    if count%25==0:
                                        print(str(count)+'|'+fname)
                        except Exception as E:
                            print(E)
            except:
                pass
    print('|'+str(yyyy)+'|'+str(count)+' working days|'+str(datetime.datetime.now()-ytime)+'|')
