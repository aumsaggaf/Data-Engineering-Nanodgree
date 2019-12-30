import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ 
    Drops tables defined on the sql_queries script. 
  
    Drops tables defined on the sql_queries script using the defined queries:
    [staging_events_table_drop, staging_songs_table_drop, user_table_drop, artist_table_drop, song_table_drop, time_table_drop, songplay_table_drop].
  
    Parameters: 
    cur (psycopg2.cursor()): Cursor of the database
    conn (psycopg2.connect()): Connection to the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ 
    Creates and connects to sparkifydb database. 
  
    Creates tables defined on the sql_queries script using the defined queries:
    [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create].
  
  
    Parameters: 
    cur (psycopg2.cursor()): Cursor of the database
    conn (psycopg2.connect()): Connection to the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Initializes the connection to redshift.
    
    Initializes the connection to redshift for the staging tables. Drops and creates the database tables.    
    
    Usage: python create_tables.py
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()