from flask import Flask, request
import sqlite3
import requests
from tqdm import tqdm
import json 
import numpy as np
import pandas as pd


app = Flask(__name__) 

@app.route('/')
def home():
    return 'Hello World'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

@app.route('/trips/<trip_id>')
def route_trips_id(trip_id):
    conn = make_connection()
    trip = get_trip_id(trip_id, conn)
    return trip.to_json()

@app.route('/json', methods = ['POST']) 
def json_example():
    
    req = request.get_json(force=True) # Parse the incoming json data as Dictionary
    
    name = req['name']
    age = req['age']
    address = req['address']
    
    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

@app.route('/trips/add', methods=['POST']) 
def route_add_trip():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

@app.route('/trips/station_use')
def station_utilization():
    conn = make_connection()
    station_use = get_station_use(conn)
    return station_use.to_json()

@app.route('/trips/bike_use/<bikeid>')
def bike_utilization(bikeid):
    conn = make_connection()
    bike_use = get_bike_use(bikeid, conn)
    return bike_use.to_json()




from flask import jsonify

@app.route('/station_status', methods=['POST'])
def station_status():
    data = request.get_json(force=True)
    start_date = data['start_date']
    end_date = data['end_date']

    conn = make_connection()
    query = f'''
        SELECT status, COUNT(station_id) AS status_count, SUM(number_of_docks) AS total_docks
        FROM stations
        WHERE modified_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY status
    '''    
    result = conn.execute(query)
    rows = result.fetchall()
    results_list = [{'status': row[0], 'status_count': row[1], 'total_docks': row[2]} for row in rows]
    conn.close()
    return jsonify(results_list)


######## Functions ########

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result
    
def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def get_station_use(conn):
    query = f''' 
    SELECT start_station_name, COUNT(trips.id) AS trip_count
    FROM trips
    GROUP BY start_station_name
    ORDER BY trip_count DESC'''
    result = pd.read_sql_query(query, conn)
    return result

def get_bike_use(bikeid, conn):
    query = f''' 
    SELECT bikeid, 
           COUNT(id) AS trip_count,
           SUM(duration_minutes / 60) AS operating_hours
    FROM trips
    WHERE bikeid = {bikeid}
    GROUP BY bikeid
    ORDER BY operating_hours DESC'''
    result = pd.read_sql_query(query, conn)
    return result


if __name__ == '__main__':
    app.run(debug=True, port=5000)