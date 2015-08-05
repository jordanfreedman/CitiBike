import requests
from pandas.io.json import json_normalize
import sqlite3 as lite
import pandas as pd 

# read data
r = requests.get('http://www.citibikenyc.com/stations/json')
df = json_normalize(r.json()['stationBeanList'])

con = lite.connect('citi_bike.db')
cur = con.cursor()


# create table for static info 
cur.execute('DROP TABLE citibike_reference')


cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT)')

# insert values into static table
sql = 'INSERT INTO citibike_reference (id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, longitude) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)' 


for station in r.json()['stationBeanList']:
	cur.execute(sql, (station['id'], station['totalDocks'], station['city'], station['altitude'], station['stAddress2'], station['longitude'], station['postalCode'], station['testStation'], station['stAddress1'], station['stationName'], station['landMark'], station['latitude'], station['location']))
con.commit()

cur.execute('SELECT * FROM citibike_reference')
rows = cur.fetchall()
cols = [desc[0] for desc in cur.description]
df = pd.DataFrame(rows, columns=cols)

print df

