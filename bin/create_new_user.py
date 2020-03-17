#!/usr/bin/python3
# coding: utf-8
# packages
import mysql.connector
import conf_reader

def user_connect(user,password,host):
	cnx = mysql.connector.connect(user=user, password=password,host=host,autocommit=True)
	cur=cnx.cursor()
	return cur,cnx

def user_creation(mkuser,mkpass,host,cur,cnx):
	check_user = "SELECT EXISTS(SELECT 1 FROM mysql.user WHERE user = '%s' and host = '%s');"%(mkuser, host) 
	cur.execute(check_user)
	status = cur.fetchall()
	status=status[0]
	status=status[0]
	if status == 1:
		print("mysql user",mkuser," already exist")
	if status != 1:
		creation = "CREATE USER IF NOT EXISTS '%s'@'%s' IDENTIFIED BY '%s';" %(mkuser, host,mkpass) 
		print(creation)
		results = cur.execute(creation)
		print ("User creation done..")
		
		granting = "GRANT ALL ON *.* TO '%s'@'%s'"%(mkuser, host)
		print(granting)
		results = cur.execute(granting)
		print ("Granting of privileges done..")
		
		flushpri ="flush privileges;"
		results = cur.execute(flushpri)
		print ("Granting of privileges done..")
	
def main():
	config = conf_reader.get_config()
	inputdbprop = config.get("inputdbprop")

	# olduser = input ("Enter sql old user name :")
	# oldpass = input ("Enter sql old user password :")
	# host    = input ("Enter sql hostname :")
	olduser = config.get("sql_old_username")
	oldpass = config.get("sql_old_userpassword")
	host    = inputdbprop.get("hostname")
	
	mkuser = inputdbprop.get("dbusername")
	mkpass = inputdbprop.get("dbpassword")


	cur,cnx = user_connect(olduser,oldpass,host)
	results=user_creation(mkuser,mkpass,host,cur,cnx)

if __name__ == "__main__":
		main()
