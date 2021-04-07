import configparser
import psycopg2
import boto3
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    - Executes all queries in copy_table_queries to loads the data from S3 into staging tables
    
    Parameters:
           cur: cursor for the database connection
           conn: database connection

    Returns:
          None
    """
    print("Load staging tables")
    for query in copy_table_queries:
            cur.execute(query)
            conn.commit()


def insert_tables(cur, conn):
    """
    - Executes all queries in insert_table_queries to loads from staging tables into fact and dimension tables
    
    Parameters:
           cur: cursor for the database connection
           conn: database connection

    Returns:
          None
    """
    print("Insert data to tables")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Connects to dwhsparkify
    - Loads the data from S3 into staging tables
    - Inserts data into target tables (fact and dimension tables)
    - Closes database connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Open connection")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    print("Close connection")
    conn.close()

if __name__ == "__main__":
    main()