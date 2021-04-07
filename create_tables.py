import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    - Executes all queries in drop_table_queries to delete all tables
    
    Parameters:
           cur: cursor for the database connection
           conn: database connection

    Returns:
          None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    - Executes all queries in create_table_queries to create all tables
    
    Parameters:
           cur: cursor for the database connection
           conn: database connection

    Returns:
          None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Connects to dwhsparkify
    - Deletes all tables
    - Creates all tables
    - Closes database connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Open connection")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print("Drop tables")
    drop_tables(cur, conn)
    print("Create tables")
    create_tables(cur, conn)

    print("Close connection")
    conn.close()


if __name__ == "__main__":
    main()