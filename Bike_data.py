import requests
import sqlite3 as lite
import pandas as pd 
import time
from dateutil.parser import parse
import collections

con = lite.connect('citi_bike.db')
cur = con.cursor()
#cur.execute('DELETE FROM available_bikes')
old_values = {}
first_row = True
total = 1520

# insert values into moving table:
for i in range(total):

	r = requests.get('http://www.citibikenyc.com/stations/json')

	try: exec_time = parse(r.json()['executionTime'])
	except: pass

	cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%s'),))
	con.commit()

	diff_bikes = collections.defaultdict(int)
	
	for station in r.json()['stationBeanList']:
		if first_row == False:
			diff_bikes[station['id']] = abs((station['availableBikes'] - old_values[station['id']]))
		else: diff_bikes[station['id']] = 0

		old_values[station['id']] = station['availableBikes']	

	for k,v in diff_bikes.iteritems():
		try: cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = " + exec_time.strftime('%s') + ";")
		except:
			print "No column for " + str(k)
	
	con.commit()
	time.sleep(60)
	print str(i*100/total) + "% complete"
	first_row = False

con.close()







