import configparser
import psycopg2

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    cur.execute("""
     select * from songplays limit 5
    """)
    for row in cur.fetchall():
        print(row)

    conn.close()

if __name__ == "__main__":
    main()