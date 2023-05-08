import configparser
import psycopg2

def run_query(cur, conn, query, query_question):
    """Runs a query and prints the results."""
    print("------------------------------------")
    print("Question: " + query_question)
    cur.execute(query)
    conn.commit()
    for row in cur.fetchall():
        print("Answer: " + str(row))

def main():
    """Runs all queries."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    query = """
        SELECT s.title, COUNT(*) as play_count
        FROM songplays sp
        JOIN songs s ON sp.song_id = s.song_id
        GROUP BY s.title
        ORDER BY play_count DESC
        LIMIT 1;
    """
    query_question = "What is the most played song?"
    run_query(cur, conn, query, query_question)

    query = """
        SELECT a.name, COUNT(*) as play_count
        FROM songplays sp
        JOIN artists a ON sp.artist_id = a.artist_id
        GROUP BY a.name
        ORDER BY play_count DESC
        LIMIT 1;
    """
    query_question = "Who is the most played artist?"
    run_query(cur, conn, query, query_question)

    query = """
        SELECT EXTRACT(hour FROM timestamp 'epoch' + sp.start_time/1000 * interval '1 second') as hour_of_day, COUNT(*) as play_count
        FROM songplays sp
        GROUP BY hour_of_day
        ORDER BY play_count DESC
        LIMIT 1;
    """
    query_question = "When is the highest usage time of day by hour for songs?"
    run_query(cur, conn, query, query_question)

    conn.close()

if __name__ == "__main__":
    main()