#!/usr/bin/python
import ConfigParser
import math
import MySQLdb
class deletedate:
	config_name=""
	host_list=[]
	user_list=[]
	passwd_list=[]
	db_list=[]
	table_list=[]
	def __init__(self,config_name):
		if config_name==None:
			print "The path is none!"
		else:
			print "deletedate begin!"
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
		for j in self.host_list:
			print j
	def deletedate(self):
		num=0
		for host in self.host_list:
			print host+self.user_list[num]+self.passwd_list[num]+self.db_list[num]
			#try:	
				#con=MySQLdb.connect(host=host,user=user_list[num],passwd=passwd_list[num],db=db_list[num])
			num=num+1
		

if __name__ == '__main__':
	dd=deletedate("datelist.ini");
	dd.readconfig()
	dd.deletedate()
	
