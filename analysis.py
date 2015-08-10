import pandas as pd 
import sqlite3 as lite
import collections
import datetime
import time
import numpy as np 

con = lite.connect('citi_bike.db')
cur = con.cursor()

# read into pandas dataframe
cur.execute("SELECT * FROM available_bikes")

rows = cur.fetchall()
cols = [desc[0] for desc in cur.description]
bike_data = pd.DataFrame(rows, columns=cols)

cur.execute("SELECT id, longitude,latitude FROM citibike_reference")

rows = cur.fetchall()
cols = [desc[0] for desc in cur.description]
positions = pd.DataFrame(rows, columns=cols)

id_col = bike_data.columns.values[1:]

cur.execute('DROP TABLE torque')

cur.execute("CREATE TABLE torque (id INT, difference INT, execution_time INT);")

for hour in range(24):
	for q in [0, 30]:
			
		beginning = int((datetime.datetime(2015,8,9,hour,q,0)).strftime('%s'))
		end = int((datetime.datetime(2015,8,9,hour,(q+29), 59)).strftime('%s'))
		activity = bike_data[(bike_data['execution_time'] > beginning) & (bike_data['execution_time'] <= end)]
				
		for i in id_col:
				
			if (sum(activity[i]) > 0) & (sum(activity[i]) <= 5):
				time = datetime.datetime.fromtimestamp(beginning).strftime('%Y-%m-%d %H:%M:%S')
				cur.execute('INSERT INTO torque (id, difference, execution_time) VALUES (?,?,?)', (i[1:], ' 1-6', time))
			elif (sum(activity[i]) > 5) & (sum(activity[i]) <= 12):
				time = datetime.datetime.fromtimestamp(beginning).strftime('%Y-%m-%d %H:%M:%S')
				cur.execute('INSERT INTO torque (id, difference, execution_time) VALUES (?,?,?)', (i[1:], ' 7-14', time))
			elif (sum(activity[i]) > 12) & (sum(activity[i]) <= 19):
				time = datetime.datetime.fromtimestamp(beginning).strftime('%Y-%m-%d %H:%M:%S')
				cur.execute('INSERT INTO torque (id, difference, execution_time) VALUES (?,?,?)', (i[1:], '15-24', time))
			elif (sum(activity[i]) > 19) & (sum(activity[i]) <= 29):
				time = datetime.datetime.fromtimestamp(beginning).strftime('%Y-%m-%d %H:%M:%S')
				cur.execute('INSERT INTO torque (id, difference, execution_time) VALUES (?,?,?)', (i[1:], '25-39', time))
			elif (sum(activity[i]) > 30):
				time = datetime.datetime.fromtimestamp(beginning).strftime('%Y-%m-%d %H:%M:%S')
				cur.execute('INSERT INTO torque (id, difference, execution_time) VALUES (?,?,?)', (i[1:], '40+', time))

con.commit()

cur.execute("SELECT * FROM torque")

rows = cur.fetchall()
cols = [desc[0] for desc in cur.description]
data = pd.DataFrame(rows, columns=cols)



result = pd.merge(data, positions, how='left', on='id')



print result

result.to_csv('bicycle_data.csv', sep='\t')
















	




		
	