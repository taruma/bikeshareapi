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
