import requests
import sqlite3 as lite
import pandas as pd 
import time
from dateutil.parser import parse
import collections

con = lite.connect('citi_bike.db')
cur = con.cursor()


cur.execute('DELETE FROM available_bikes')

# create dictionary to store number of available bikes, to compare to next minute
old_values = {}

# set value as True to ensure first row skipped, which would result in error otherwise
first_row = True

# set number of minutes for programme to run
total = 1520

# insert values into moving table:
for i in range(total):

	r = requests.get('http://www.citibikenyc.com/stations/json')

	exec_time = parse(r.json()['executionTime'])

	# insert execution time into table
	cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%s'),))
	con.commit()

	# create dictionary to store difference values for each station
	diff_bikes = collections.defaultdict(int)
	
	# store difference per station in dictionary
	for station in r.json()['stationBeanList']:
		if first_row == False:
			diff_bikes[station['id']] = abs((station['availableBikes'] - old_values[station['id']]))
		else: diff_bikes[station['id']] = 0

		# store value to use as comparison in next loop
		old_values[station['id']] = station['availableBikes']	

	# loop through dictionry and store values in table
	for k,v in diff_bikes.iteritems():
		try: cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = " + exec_time.strftime('%s') + ";")
		except:
			print "No column for " + str(k)
	
	con.commit()

	# sleep for a minute before restarting
	time.sleep(60)
	print str(i*100/total) + "% complete"

	# after first loop, set value as False
	first_row = False

con.close()







