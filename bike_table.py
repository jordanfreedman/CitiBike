import requests
from pandas.io.json import json_normalize
import sqlite3 as lite
import pandas as pd 

# read data
r = requests.get('http://www.citibikenyc.com/stations/json')
df = json_normalize(r.json()['stationBeanList'])

con = lite.connect('citi_bike.db')
cur = con.cursor()

# log station id's in list
station_ids = df['id'].tolist()

# add '_' before station id number to allow use as column heading
station_ids = ['_' + str(x) + ' INT' for x in station_ids]

# create table with a column for execution time and a column each for station ids
cur.execute('DROP TABLE available_bikes')
cur.execute("CREATE TABLE available_bikes (execution_time INT, " +  ", ".join(station_ids) + ");")
