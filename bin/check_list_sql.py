#!/usr/bin/env python3
# coding: utf-8
# packages
import warnings
warnings.filterwarnings("ignore")
import mysql.connector
import conf_reader
import pandas as pd
import os

def db_connet(database,user,password,host):
    cnx = mysql.connector.connect(user=user, password=password,host=host,database=database)
    cur=cnx.cursor()
    return cur,cnx
def sql_connet(user,password,host):
    cnx = mysql.connector.connect(user=user, password=password,host=host)
    cur=cnx.cursor()
    return cur,cnx
def tabledatacount():
	config = conf_reader.get_config()
	inputdbprop = config.get("inputdbprop", None)
	inputstockdatapath= config.get("inputstockdatapath")
	try:
		cur,cnx = db_connet(inputdbprop.get('dbname'),inputdbprop.get('dbusername'),inputdbprop.get('dbpassword'),inputdbprop.get('hostname'))
	except:
		cur,cnx = sql_connet(inputdbprop.get('dbusername'),inputdbprop.get('dbpassword'),inputdbprop.get('hostname'))
	files=os.listdir(inputstockdatapath)
	tablecount=dict()
	for i in files:
		tablename=i.split(".")[0]
		try:
			query="select count(1) as count from(select *from %s limit 1) data"% (tablename)
			cur.execute(query)
			recordcount = cur.fetchall()
			recordcount=recordcount[0]
			recordcount=recordcount[0]
		except:recordcount = 0
		tablecount[tablename] = recordcount
	return tablecount

if __name__ == "__main__":	
	tabledatacount = tabledatacount() 
	print(str(tabledatacount)+' check list execution done...')
