import pandas as pd 
import sqlite3 as lite
import collections
import datetime

con = lite.connect('citi_bike.db')
cur = con.cursor()

# read into pandas dataframe
df = pd.read_sql_query("SELECT * FROM available_bikes ORDER BY execution_time", con, index_col = 'execution_time')

#create dictionary with keys - station ids and values - sum of changes
hour_change = collections.defaultdict(int)
for i,value in enumerate(df.columns):
	column = df.iloc[:,i]
	difference_tot = 0
	for i, v in enumerate(column):
		try: 
			difference = column.iloc[i+1] - column.iloc[i]
			difference_tot += abs(difference)
		except: pass
	hour_change[str(value[1:])] = difference_tot


# find station with highest sum
def key_with_max(d):
	v = list(d.values())
	k = list(d.keys())
	return k[v.index(max(v))]

max_station = key_with_max(hour_change)

# print most active station
cur.execute("SELECT id, stationname, latitude, longitude FROM citibike_reference WHERE id = ?", (max_station,))
data = cur.fetchone()
print "The most active station is station id %s at %s latitude: %s longitude: %s " % data
print "with " + str(hour_change[max_station]) + " bicycles coming and going in the hour between " + datetime.datetime.fromtimestamp(int(df.index[0])).strftime('%Y-%m-%dT%H:%M:%S') + " and " + datetime.datetime.fromtimestamp(int(df.index[-1])).strftime('%Y-%m-%dT%H:%M:%S')

con.close()


