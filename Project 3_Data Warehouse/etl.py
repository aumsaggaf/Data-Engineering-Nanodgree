import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads the staging tables from S3.
    
    Loads the staging tables from the datasets that reside in S3.
    
    Parameters: 
    cur (psycopg2.cursor()): Cursor of the database
    conn (psycopg2.connect()): Connection to the database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts the data into the created tables.
    
    Inserts the data into the created tables of the star schema from the staging tables.
    
    Parameters: 
    cur (psycopg2.cursor()): Cursor of the database
    conn (psycopg2.connect()): Connection to the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Initializes the connection to redshift and runs the previous functions to load/insert the data.
    
    Initializes the connection to redshift, extracts and transforms all data from song and user activity logs, and loads it into the database.

    Usage: python etl.py
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()