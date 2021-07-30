"""
The script create_tables.py connects to the redshift database. Then, it creates the staging, dimensional, and fact tables. Finally, the connection is closed.
"""

# Import libraries
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in 'drop_table_queries'.
    ...

    Attributes
    ----------
    cur : cursor of the connection with the redshift database
    conn : connection with the redshift database


    Returns
    -------
    None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in 'create_table_queries'.
    ...

    Attributes
    ----------
    cur : cursor of the connection with the redshift database
    conn : connection with the redshift database


    Returns
    -------
    None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    The main's method connects to the redshift database and calls the drop_tables and create_tables methods. Finally, it                 closes the connection.
    ...

    Attributes
    ----------
    None


    Returns
    -------
    None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    """
    It calls the main method.

    ...

    Attributes
    ----------
    None

    Returns
    -------
    None
    """
    main()