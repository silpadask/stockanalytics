#!/usr/bin/env python3
# coding: utf-8
# packages
import warnings
warnings.filterwarnings("ignore")
import pymysql
import conf_reader
print(' adjust price execution started...')
def db_connet(database,user,password,host):
    cnx = pymysql.connect(user=user, password=password,host=host,database=database,charset='utf8', autocommit=True,cursorclass=pymysql.cursors.DictCursor)
    cur=cnx.cursor()
    return cur,cnx

config = conf_reader.get_config()
input_db_prop = config.get("inputdbprop", None)

#### Connect to database
cur,cnx = db_connet(input_db_prop.get('dbname'),input_db_prop.get('dbusername'),input_db_prop.get('dbpassword'),input_db_prop.get('hostname'))
databasename=input_db_prop.get('dbname')

usedb="use %s "% (databasename)
cur.execute(usedb)

nse_init_sql = "SELECT * FROM nsecorpactclean order by symbol, exdate desc; "
try:
    cur.execute(nse_init_sql)
    nse_corp_act_records = cur.fetchall()
    for nse_corp_act in nse_corp_act_records:      
        symbol= nse_corp_act["symbol"]
        series= nse_corp_act["series"]
        exdate = nse_corp_act["exdate"]
        factor = nse_corp_act["factor"]
        factor_update_sql = "update nsedailybhavhist set factor = factor * " + str(factor)+" where symbol = '" + symbol+"' and series = '"+series +"' and timestamp < '"+str(exdate)+"'"
        affectedRows = cur.execute(factor_update_sql)
        if affectedRows is None:
            affectedRows = 0
        print(symbol + "|" + str(exdate) +"|" + str(factor) +"|Affected Rows|"+ str(affectedRows))
        
except Exception as E:
    print(E)
try:
    price_update_sql ="update nsedailybhavhist set OPEN_ADJ=FACTOR*OPEN,HIGH_ADJ=FACTOR*HIGH,LOW_ADJ=FACTOR*LOW,CLOSE_ADJ=FACTOR*CLOSE,LAST_ADJ=FACTOR*LAST, TOTTRDQTY_ADJ=TOTTRDQTY/FACTOR where FACTOR!=0;" 
    affectedrows = cur.execute(price_update_sql)
except:
    pass

 
