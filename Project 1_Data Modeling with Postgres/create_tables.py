import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """ 
    Creates and connects to sparkifydb database. 
  
    Creates the sparkifydb database and connects to it.
  
    Returns: 
    Crouser of the sparkifydb database
    Connection to the sparkifydb database
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """ 
    Drops tables defined on the sql_queries script. 
  
    Drops tables defined on the sql_queries script using the defined queries:
    [user_table_drop, artist_table_drop, song_table_drop, time_table_drop, songplay_table_drop].
  
    Parameters: 
    cur (psycopg2.cursor()): Cursor of the database
    conn (psycopg2.connect()): Connection to the database
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ 
    Creates tables defined on the sql_queries script. 
  
    Creates tables defined on the sql_queries script using the defined queries:
    [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create].
  
    Parameters: 
    cur (psycopg2.cursor()): Cursor of the database
    conn (psycopg2.connect()): Connection to the database
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ 
    Connects to the database and create the database and its tables.
    
    Connects to the database and create the sparkifydb database.Drops and creates the database tables.    
    
    Usage: python create_tables.py
    """
    
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()