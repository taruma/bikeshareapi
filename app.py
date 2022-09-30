import sqlite3
import ast
# import requests
# from tqdm import tqdm

from flask import Flask, request
# import json 
# import numpy as np
import pandas as pd

app = Flask(__name__) 

# LOGIC FUNCTIONS

## STATIONS

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

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except Exception as e:
        print(e)
        return 'Error'
    conn.commit()
    return 'OK'

## TRIPS

def get_all_trips(conn):
    query = """
    SELECT * FROM trips
    -- LIMIT 10
    """
    return pd.read_sql_query(query, conn)

def get_trip_id(trip_id, conn):
    query = f"""
    SELECT *
    FROM trips
    WHERE id = {trip_id}
    """
    return pd.read_sql_query(query, conn)

def insert_into_trips(data, conn):
    query = f"""
    INSERT INTO trips
    VALUES {data}
    """
    try:
        conn.execute(query)
    except Exception as e:
        print(e)
        return 'Error'
    conn.commit()
    return 'OK'

## OWN ANALYTIC FUNCTIONS

def top_subscribers(conn):
    query = """
    SELECT 
        trips.subscriber_type,
        COUNT(subscriber_type) AS total_subscriber
    FROM trips
    GROUP BY trips.subscriber_type
    ORDER BY total_subscriber DESC
    """
    return pd.read_sql_query(query, conn)    

def top_days_start_in_station(start_station_id, conn):
    try:    
        return (
            pd.read_sql_query(
                f"""
                SELECT bikeid, start_time, start_station_id
                FROM trips
                WHERE trips.start_station_id = {start_station_id}
                """,
                conn,
                parse_dates='start_time'
            ).assign(
                start_day_of_week=lambda x: x.start_time.dt.day_name()
            ).pivot_table(
                index='start_day_of_week',
                values='start_time',
                aggfunc='count'
            ).sort_values('start_time', ascending=False)
        )
    except:
        return f"STATION ID {start_station_id} NOT FOUND!"

def top_duration_mean(subscriber_type, month_number, conn):
    return (
        pd.read_sql_query(
            f"""
            SELECT *
            FROM trips
            WHERE 
                trips.subscriber_type LIKE '{subscriber_type}'
                AND trips.start_time LIKE '%-{month_number}-%'
            """,
            conn,
            parse_dates='start_time'
        )
        .assign(
            start_year=lambda df: df.start_time.dt.year,
            start_dow=lambda df: df.start_time.dt.day_name()
        )
        .groupby(['start_year', 'start_dow']).agg({
            'bikeid': 'count',
            'duration_minutes': ['median', 'mean', 'max'],
        })
        .droplevel(0, axis='columns')
        .sort_values(['start_year', 'mean'], ascending=[False, False], axis='index')
        .rename({'median': 'duration_median', 'mean':'duration_mean', 'max':'duration_max'}, axis='columns')
    )

# ENDPOINT

@app.route('/')
@app.route('/homepage')
def home():
    return 'Hello World'

## MY SUBMISSION

### Static Endpoint
@app.route('/trips/top_subscribers/')
def route_trips_top_subscriber():
    conn = make_connection()
    top_list = top_subscribers(conn)
    return top_list.to_json()

### dynamic endpoints
@app.route('/trips/top_days_start_in_station/<station_id>')
def route_top_days_start_in_station(station_id):
    conn = make_connection()
    top_days = top_days_start_in_station(station_id, conn)
    return top_days.to_json() if isinstance(top_days, pd.DataFrame) else top_days

### post endpoint
@app.route('/trips/top_duration_mean', methods=['POST'])
def route_top_duration_mean():
    data_input = request.get_json(force=True)
    conn = make_connection()
    return top_duration_mean(conn=conn, **data_input).to_json()

## TUTORIAL

## STATIONS

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

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

## TRIPS 

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

@app.route('/trips/add', methods=['POST'])
def route_add_trip():
    data = pd.Series(ast.literal_eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

## EXAMPLE POST
@app.route('/json', methods=['POST']) 
def json_example():
    
    req = request.get_json(force=True) # Parse the incoming json data as Dictionary
    
    name = req['name']
    age = req['age']
    address = req['address']
    
    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')


if __name__ == '__main__':
    app.run(debug=True, port=5000)
    