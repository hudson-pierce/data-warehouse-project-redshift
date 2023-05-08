import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loads data from S3 into staging tables."""
    for query in copy_table_queries:
        print("--------------- Running query ---------------")
        print(query)
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """Inserts data from staging tables into analytics tables."""
    for query in insert_table_queries:
        print("--------------- Running query ---------------")
        print(query)
        cur.execute(query)
        conn.commit()

def main():
    """Loads data from S3 into staging tables, then inserts data from staging tables into analytics tables."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
