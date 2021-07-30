"""
The script etl.py connects to the redshift database. Then, it copies data from the S3 bucket to the staging tables. Finally, the script inserts data into the fact and dimensional tables using the staging tables, so it can be consumed by the analytics team.
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    The load_staging_tables' method copies data from S3 to the staging tables.

    ...

    Attributes
    ----------
    cur : cursor of the connection with the redshift database
    conn : connection with the redshift database


    Returns
    -------
    None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    The insert_tables' method inserts data into the fact and dimensional tables using the staging tables.

    ...

    Attributes
    ----------
    cur : cursor of the connection with the redshift database
    conn : connection with the redshift database


    Returns
    -------
    None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    The main's method connects to the redshift database and calls the load_staging_tables and insert_tables methods. Finally, it         closes the connection.

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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

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