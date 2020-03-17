#!/usr/bin/env python3
# coding: utf-8
# packages
import warnings
warnings.filterwarnings("ignore")
import mysql.connector
import conf_reader
import datetime
import pandas as pd
print('corporate action new data loading to database  execution started...')
def sql_connet(database,user,password,host):
    cnx = mysql.connector.connect(user=user, password=password,host=host,database=database,autocommit=True)
    cur=cnx.cursor()
    return cur,cnx

def loaddatatosql():
	todays_date = datetime.datetime.today()
	dateformat=todays_date.strftime("%d%m%Y")

	config = conf_reader.get_config()
	inputdbprop = config.get("inputdbprop", None)
	inputstockdatapath= config.get("inputstockdatapath")
	sqltablename= config.get("sqltablename")
	latestdatafromnsepath= config.get("latestdatafromnsepath")
	cur,cnx = sql_connet(inputdbprop.get('dbname'),inputdbprop.get('dbusername'),inputdbprop.get('dbpassword'),inputdbprop.get('hostname')) 
	table_name=sqltablename
	input_path =str(latestdatafromnsepath)+"_latest"+str(dateformat)+".csv"
	filtered_set = pd.read_csv(input_path)

	lst=[]
	for i in filtered_set.values:
	    lst.append(tuple(i))
	valstrg=str(lst)[1:-1]
	columns = ', '.join("`"+key+"`" for key in filtered_set.keys())
	sql = "INSERT INTO %s ( %s ) VALUES %s" % (table_name, columns,valstrg)
	try:
		cur.execute(sql)
	except:pass

if __name__ == "__main__":  
	loaddatatosql = loaddatatosql()
