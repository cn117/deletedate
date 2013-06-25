#!/usr/bin/python
import ConfigParser
import math
import MySQLdb
import time
class deletedate:
	config_name=""
	host_list=[]
	user_list=[]
	passwd_list=[]
	db_list=[]
	table_list=[]
	column_list=[]
	column_values_list=[]
	limit_count_list=[]
	def __init__(self,config_name):
		if config_name==None:
			print "The path is none!"
		else:
			print "#####"+time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))+"deletedate begin!######"
			self.config_name=config_name
	def readconfig(self):
		config=ConfigParser.ConfigParser()
		config.readfp(open(self.config_name),"rb")
		#for i in range(0,2):
		date_list=config.sections();
		for i in date_list:
			#print config.get("global"+str(i),"ip")
			self.host_list.append(config.get(i,"host"))
			self.user_list.append(config.get(i,"user"))
			self.passwd_list.append(config.get(i,"passwd"))
			self.db_list.append(config.get(i,"db"))
			self.table_list.append(config.get(i,"table"))
			self.column_list.append(config.get(i,"column"))
			self.column_values_list.append(config.get(i,"column_values"))
			self.limit_count_list.append(config.get(i,"limit_count"))
		for j in self.host_list:
			print j
	def deletedate(self):
		num=0
		for host in self.host_list:
			print host+"###"+self.user_list[num]+"###"+self.passwd_list[num]+"###"+self.db_list[num]+"###"+self.table_list[num]+"###"+self.column_list[num]+"###"+self.column_values_list[num]+"###"+self.limit_count_list[num]
			try:	
				con=MySQLdb.connect(host=host,user=self.user_list[num],passwd=self.passwd_list[num],db=self.db_list[num])
			except Exception, e:
				print e
				sys.exit()
			cursor=con.cursor()
			sql="DELETE FROM {0} where {1}={2} limit {3}"
			print sql
			print sql.format(self.table_list[num],self.column_list[num],self.column_values_list[num],self.limit_count_list[num])
			try:
				n=cursor.execute(sql.format(self.table_list[num],self.column_list[num],self.column_values_list[num],self.limit_count_list[num]))
				con.commit()
				print "deletedate num is: "+str(n)
			except Exception, e:
				print e
			cursor.close()
			con.close()
			num=num+1
		

if __name__ == '__main__':
	dd=deletedate("datelist.ini");
	dd.readconfig()
	dd.deletedate()
	
