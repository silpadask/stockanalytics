#!/usr/bin/env python3
# coding: utf-8
# packages
import warnings
warnings.filterwarnings("ignore")
import mysql.connector
import conf_reader
import pandas as pd
import os
import check_list_sql

def sql_connet(user,password,host):
	cnx = mysql.connector.connect(user=user, password=password,host=host,autocommit=True)
	cur=cnx.cursor()
	return cur,cnx

def loadtodb():
	config = conf_reader.get_config()
	inputdbprop = config.get("inputdbprop", None)
	inputstockdatapath= config.get("inputstockdatapath")
	dbname=inputdbprop.get('dbname')
	checklist = check_list_sql.tabledatacount()
	files=os.listdir(inputstockdatapath)
	for i in files:
		tablename=i.split(".")[0]
		recordcount = checklist.get(tablename)
		if recordcount == 1:
			print('data already there in database...')
			continue
		if recordcount == 0:
			cur,cnx = sql_connet(inputdbprop.get('dbusername'),inputdbprop.get('dbpassword'),inputdbprop.get('hostname'))
			files=os.listdir(inputstockdatapath)
			for i in files:
				with open(inputstockdatapath+str(i), "rt",encoding='utf-8') as f:
					count=0
					for line in f:
						l = line.strip()
						query = l.split(';')
						query=query[0]
						print(str(query)+'load to sql execution started...')
						if count <=1:
							query=query.replace("?",dbname)
						if count >=2:
							query=query
						try:		
							cur.execute(query)
						except:pass
if __name__ == "__main__":	
	loadtodb = loadtodb() 
	print('load data execution completed...')
		
