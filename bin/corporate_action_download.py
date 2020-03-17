#!/usr/bin/env python3
# coding: utf-8
# packages
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import time
import os
import shutil, os
import urllib.request
import conf_reader
from datetime import datetime, timedelta

config = conf_reader.get_config()
latestdatafromnsepath= config.get("latestdatafromnsepath")
olddatafromnsepath=config.get("olddatafromnsepath")
print("corporate action download new csv file generating execution started...")
try:
	files = [str(latestdatafromnsepath)+'.csv']
	for f in files:
	    shutil.move(f, str(olddatafromnsepath)+'.csv')
	ye_date=datetime.strftime(datetime.now() - timedelta(1), '%d%m%Y')

	os.rename(str(latestdatafromnsepath)+'.csv',str(latestdatafromnsepath)+'_'+str(ye_date)+'.csv') 
except:pass
urllib.request.urlretrieve("https://www1.nseindia.com/corporates/datafiles/CA_LAST_24_MONTHS.csv",str(latestdatafromnsepath)+'.csv')

