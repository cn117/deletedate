#!/usr/bin/python
import ConfigParser
import math
import MySQLdb
import time
import sys
import logging
class deletedate:
	config_name=""
	host_list=[]
	user_list=[]
	passwd_list=[]
	db_list=[]
	table_list=[]
	status_column_list=[]
	status_column_values_list=[]
	limit_count_list=[]
	save_day_list=[]
	day_column_list=[]
	logger=logging.getLogger()
	file=logging.FileHandler(time.strftime('%Y-%m-%d',time.localtime(time.time()    ))+"_log.log")
	logger.addHandler(file) 
	formatter=logging.Formatter("%(asctime)s %(levelname)s %(message)s")
	file.setFormatter(formatter)
	logger.setLevel(logging.NOTSET)
	def __init__(self,config_name):
		if config_name==None:
			print "The path is none!"
			logger.error("The path is none!")
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
			self.status_column_list.append(config.get(i,"status_column"))
			self.status_column_values_list.append(config.get(i,"status_column_values"))
			self.limit_count_list.append(config.get(i,"limit_count"))
			self.save_day_list.append(config.get(i,"save_day"))
			self.day_column_list.append(config.get(i,"day_column"))
		for j in self.host_list:
			print j
	def deletedate(self):
		try:
			num=0
			for host in self.host_list:
				print "***begin:****"
				print host+"###"+self.user_list[num]+"###"+self.passwd_list[num]+"###"+self.db_list[num]+"###"+self.table_list[num]+"###"+self.status_column_list[num]+"###"+self.status_column_values_list[num]+"###"+self.limit_count_list[num]+"###"+self.save_day_list[num]+"###"+self.day_column_list[num]
				try:	
					con=MySQLdb.connect(host=host,user=self.user_list[num],passwd=self.passwd_list[num],db=self.db_list[num])
				except MySQLdb.Error,e:
					print "Connect db is error!"
					print "Mysql Error %d: %s" % (e.args[0], e.args[1])
					break
				cursor=con.cursor()
				#sql_select="SELECT COUNT(*) FROM {0} where {1}={2}"
				sql_select="select count(*) from {0} where {1}={2} and {3} <SUBDATE(NOW(),INTERVAL {4} DAY);"
				#sql_delete="DELETE FROM {0} where {1}={2} limit {3}"
				sql_delete="DELETE FROM {0} where {1}={2} and {3} <SUBDATE(NOW(),INTERVAL {4} DAY) limit {5};"
				print "sql_delete is: "+sql_delete
				print "sql_delete is: "+sql_delete.format(self.table_list[num],self.status_column_list[num],self.status_column_values_list[num],self.day_column_list[num],self.save_day_list[num],self.limit_count_list[num])
				print "sql_select is: "+sql_select
				print "sql_select is: "+sql_select.format(self.table_list[num],self.status_column_list[num],self.status_column_values_list[num],self.day_column_list[num],self.save_day_list[num])
				try:
					cursor.execute(sql_select.format(self.table_list[num],self.status_column_list[num],self.status_column_values_list[num],self.day_column_list[num],self.save_day_list[num]))
					row=cursor.fetchone()
					print "select_count is :"+str(row[0])
				except MySQLdb.Error,e:
					print "select_count is error!"
					print "Mysql Error %d: %s" % (e.args[0], e.args[1])
					cursor.close()
					con.close()
					break
				#print int(row[0])>int(self.limit_count_list[num])
				if int(row[0])>int(self.limit_count_list[num]):
					print "The table con't deleteall in once"
					for limit_count in range(0,int(row[0]),int(self.limit_count_list[num])):
						try:
							n=cursor.execute(sql_delete.format(self.table_list[num],self.status_column_list[num],self.status_column_values_list[num],self.day_column_list[num],self.save_day_list[num],self.limit_count_list[num]))
							con.commit()
							print "deletedate num is: "+str(n)
						except MySQLdb.Error,e:
							print "delect_date is error!"
							print "Mysql Error %d: %s" % (e.args[0], e.args[1])
							cursor.close()
							con.close()
							break
					cursor.close()
					con.close()
				else:
					try:
						n=cursor.execute(sql_delete.format(self.table_list[num],self.status_column_list[num],self.status_column_values_list[num],self.limit_count_list[num]))
						con.commit()
						print "deletedate num is: "+str(n)
					except MySQLdb.Error,e:
						print "delect_date is error!"
						print "Mysql Error %d: %s" % (e.args[0], e.args[1])
						break
					finally:
						cursor.close()
						con.close()
		except Exception, e:
			print e
			sys.exit()
		finally:
			print "***end***"	

if __name__ == '__main__':
	dd=deletedate("datelist.ini");
	dd.readconfig()
	dd.deletedate()
	
