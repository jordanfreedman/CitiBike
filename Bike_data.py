import requests
import sqlite3 as lite
import pandas as pd 
import time
from dateutil.parser import parse
import collections

con = lite.connect('citi_bike.db')
cur = con.cursor()
cur.execute('DELETE FROM available_bikes')

# insert values into moving table:
for i in range(60):

	r = requests.get('http://www.citibikenyc.com/stations/json')
	exec_time = parse(r.json()['executionTime'])

	cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%s'),))
	con.commit()

	id_bikes = collections.defaultdict(int)

	for station in r.json()['stationBeanList']:
		id_bikes[station['id']] = station['availableBikes']

	for k,v in id_bikes.iteritems():
		try: cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = " + exec_time.strftime('%s') + ";")
		except:
			print "No column for " + str(k)
	
	con.commit()
	time.sleep(60)
	print "cycle " + str(i+1) + " complete"

con.close()






