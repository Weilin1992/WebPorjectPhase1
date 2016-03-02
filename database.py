import MySQLdb
from yahoo_finance import Share
from datetime import datetime,date,timedelta
import time as T


db = MySQLdb.connect("localhost","root","password")
cursor = db.cursor()

sql = "create database if not exists Stock"
cursor.execute(sql)

cursor.execute("use Stock")

sql = "create table if not exists Company(name varchar(10) not null, primary key(name))"
cursor.execute(sql)

sql = "create table if not exists oneYearStock(name varchar(10) not null, time Date not null,open double not null,close double not null, high double not null,low double not null,volume double not null,primary key(name,time),foreign key(name) references Company(name))"
cursor.execute(sql)

sql = "create table if not exists oneDayStock(name varchar(10) not null, time DateTime not null,price double not null,volume double not null,primary key(name,time),foreign key(name) references Company(name))"
cursor.execute(sql)

company_name = ["YHOO","GOOG"]
for name in company_name:
	sql = "select * from Company\
		   where name = '%s'" % (name)
	result = cursor.execute(sql)
	#print result
	if(result == 0):
		try:
			sql = "insert into Company(name) values('%s')" %(name)
			cursor.execute(sql)
			db.commit()
		except:
			db.rollback()

#result = stock.get_historical('2014-04-25','2014-04-29')

today = date.today() - timedelta(days = 1)
interval = timedelta(days = 365)
oneYearBefore = today - interval
print today

now = datetime.now()
print now.minute
now  = now - timedelta(minutes = 1)


# sql = "select * from oneYearStock"
# result = cursor.execute(sql)
# ret = cursor.fetchall()
# print ret
while True:
	print "thread executing"
	if date.today() > today:
		today = date.today()
		interval = timedelta(days = 365)
		oneYearBefore = today - interval

		for name in company_name:
			stock = Share(name);
			#print stock.get_price()
			#print stock.get_trade_datetime()
			result = stock.get_historical(str(oneYearBefore),str(today))

			for ret in result:
				name = ret['Symbol']
				time = ret['Date']
				sql = "select * from oneYearStock\
						where name = '%s' and time = '%s'" %(name,time)
				result = cursor.execute(sql)
				if result == 0:
					Open = ret['Open']
					Close = ret['Close']
					Volume = ret["Volume"]
					High = ret["High"]
					Low = ret['Low']
					sql = "insert into oneYearStock(name,time,open,close,high,low,volume) values('%s','%s','%s','%s','%s','%s','%s')" %(name,time,Open,Close,High,Low,Volume)
					try:
						cursor.execute(sql)
						db.commit()
					except:
						db.rollback()
		# print ret
	if datetime.now().minute != now.minute:
		now = datetime.now()
		for name in company_name:
			stock = Share(name)
			price = stock.get_price()
			volume = stock.get_volume()
			sql = "insert into oneDayStock(name,time,price,volume) values('%s','%s','%s''%s')" % (str(name),str(now),str(price),str(volume))
			print sql
			try:
				cursor.execute(sql)
				db.commit()
			except:
				db.rollback()
	T.sleep(40)




db.close()
















































