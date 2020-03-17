#!/usr/bin/env python3
# coding: utf-8
# packages
import warnings
warnings.filterwarnings("ignore")
import mysql.connector
import conf_reader
import pandas as pd
import os
from datetime import datetime, timedelta
import datetime as dt
import shutil, os
print(' corporate action new data fetching execution started...')
def sql_connet(database,user,password,host):
    cnx = mysql.connector.connect(user=user, password=password,host=host,database=database,autocommit=True)
    cur=cnx.cursor()
    return cur,cnx

def get_latest_data():
	config = conf_reader.get_config()
	inputdbprop = config.get("inputdbprop", None)
	sqltablename= config.get("sqltablename")
	latestdatafromnsepath= config.get("latestdatafromnsepath")
	olddatafromnsepath=config.get("olddatafromnsepath")

	cur,cnx = sql_connet(inputdbprop.get('dbname'),inputdbprop.get('dbusername'),inputdbprop.get('dbpassword'),inputdbprop.get('hostname')) 
	
	table_name=sqltablename
	
	lastupdated="select max(exdate) from nsecorporateaction;"

	cur.execute(lastupdated)
	lastupdated = cur.fetchall()
	lastupdated=lastupdated[0]
	lastupdated=lastupdated[0]

	todays_date = dt.datetime.today()
	dateformat=todays_date.strftime("%d%m%Y")

	downday=todays_date.strftime("%Y-%m-%d %H:%M:%S")
	downday=dt.datetime.strptime(downday, '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

	
	input_path =str(latestdatafromnsepath)+".csv"
	dataset = pd.read_csv(input_path)

	dataset.columns=['SYMBOL','COMPANY','INDUSTRY', 'SERIES', 'FACEVALUE','PURPOSE','EXDATE', 'RECORDDATE', 'BCSTARTDATE', 'BCENDDATE','NDSTARTDATE', 'NDENDDATE']
	dataset['DOWNDAY']=downday
	dataset['EXDATE'] = pd.to_datetime(dataset['EXDATE'])
	filtered_set = dataset[(dataset['EXDATE'] > lastupdated)]

	try:
	    filtered_set['RECORDDATE'] =dt.datetime.strptime(filtered_set['RECORDDATE'], '%d-%b-%Y').strftime("%Y-%m-%d")
	except:filtered_set['RECORDDATE']='0000-00-00'
	try:
	    filtered_set['BCSTARTDATE'] =dt.datetime.strptime(filtered_set['BCSTARTDATE'], '%d-%b-%Y').strftime("%Y-%m-%d")
	except:filtered_set['BCSTARTDATE']='0000-00-00'
	try:
	    filtered_set['BCENDDATE'] =dt.datetime.strptime(filtered_set['BCENDDATE'], '%d-%b-%Y').strftime("%Y-%m-%d")
	except:filtered_set['BCENDDATE']='0000-00-00'
	try:
	    filtered_set['NDSTARTDATE'] =dt.datetime.strptime(filtered_set['NDSTARTDATE'], '%d-%b-%Y').strftime("%Y-%m-%d")
	except:filtered_set['NDSTARTDATE']='0000-00-00'
	try:
	    filtered_set['NDENDDATE'] =dt.datetime.strptime(filtered_set['NDENDDATE'], '%d-%b-%Y').strftime("%Y-%m-%d")
	except:filtered_set['NDENDDATE']='0000-00-00'

	try:
		ye_date=datetime.strftime(datetime.now() - timedelta(1), '%d%m%Y')
		files = [str(latestdatafromnsepath)+'_latest'+str(ye_date)+'.csv']
		for f in files:
		    shutil.move(f, str(olddatafromnsepath)+'_latest'+str(ye_date)+'.csv')
	except:pass

	filename = str(latestdatafromnsepath)+"_latest"+str(dateformat)+".csv"
	filtered_set.to_csv(filename,index=False)   

if __name__ == "__main__":  
	get_latest_data = get_latest_data() 
	print(get_latest_data)

	