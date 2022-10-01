# Your code here
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


# Your code here
import pandas as pd

def get_trip_id(trip_id, conn):
    query = f"""
    SELECT *
    FROM trips
    WHERE id = {trip_id}
    """
    return pd.read_sql_query(query, conn)
    
def get_all_trips(conn):
    query = f"""
    SELECT * FROM trips
    """
    return pd.read_sql_query(query, conn)

# Your code here
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