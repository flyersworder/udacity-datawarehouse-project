# import libraries and sql queries
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: This function use the copy command to copy the JSON data from s3 to our redshift
    staging tables.

    Arguments:
        cur: the cursor object. 
        conn: the connection object.

    Returns:
        None
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e.pgerror)


def insert_tables(cur, conn):
    """
    Description: This function inserts values from the staging tables to our analytical tables.

    Arguments:
        cur: the cursor object. 
        conn: the connection object.

    Returns:
        None
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e.pgerror)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(f"host={config['CLUSTER']['HOST']} dbname={config['CLUSTER']['DB_NAME']} user={config['CLUSTER']['DB_USER']} password={config['CLUSTER']['DB_PASSWORD']} port={config['CLUSTER']['DB_PORT']}")
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()