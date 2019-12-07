# import libraries and sql queries
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: This function drops the tables if they exist. It is simply to make sure that the codes can
    be run and rerun.

    Arguments:
        cur: the cursor object. 
        conn: the connection object.

    Returns:
        None
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e.pgerror)


def create_tables(cur, conn):
    """
    Description: This function creates all the staging and analytical tables.

    Arguments:
        cur: the cursor object. 
        conn: the connection object.

    Returns:
        None
    """
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()