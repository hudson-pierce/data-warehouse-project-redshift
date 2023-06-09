import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events
    (
        artist VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender VARCHAR(1),
        item_in_session INTEGER,
        last_name VARCHAR,
        length DOUBLE PRECISION,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration DOUBLE PRECISION,
        session_id INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        user_agent VARCHAR,
        user_id INTEGER
    )
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs
    (
        artist_id VARCHAR,
        artist_latitude DOUBLE PRECISION,
        artist_location VARCHAR,
        artist_longitude DOUBLE PRECISION,
        artist_name VARCHAR,
        duration DOUBLE PRECISION,
        num_songs INTEGER,
        song_id VARCHAR,
        title VARCHAR,
        year INTEGER
    )
""")

songplay_table_create = ("""CREATE TABLE songplays 
    (
        songplay_id INTEGER IDENTITY(0,1),
        start_time BIGINT,
        user_id INTEGER,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR
    )
""")

user_table_create = ("""CREATE TABLE users
    (
        user_id INTEGER IDENTITY(0,1),
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR(1),
        level VARCHAR
    )
""")

song_table_create = ("""CREATE TABLE songs
    (
        song_id VARCHAR UNIQUE, 
        title VARCHAR,
        artist_id VARCHAR,
        year INTEGER,
        duration DOUBLE PRECISION
    )
""")

artist_table_create = ("""CREATE TABLE artists
    (
        artist_id VARCHAR UNIQUE, 
        name VARCHAR,
        location VARCHAR,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
    )
""")

time_table_create = ("""CREATE TABLE time
    (
        start_time BIGINT,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month VARCHAR,
        year INTEGER,
        weekday VARCHAR
    )
""")

# STAGING TABLES

staging_events_copy = (f"""COPY staging_events FROM 's3://udacity-dend/log_data'
    CREDENTIALS 'aws_iam_role={ARN}'
    REGION 'us-west-2'
    JSON 's3://udacity-dend/log_json_path.json'
""")

staging_songs_copy = (f"""COPY staging_songs FROM 's3://udacity-dend/song_data'
    CREDENTIALS 'aws_iam_role={ARN}'
    REGION 'us-west-2' 
    JSON 'auto'
""")

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        e.ts AS start_time, 
        e.user_id, 
        e.level, 
        s.song_id, 
        s.artist_id, 
        e.session_id, 
        e.location, 
        e.user_agent
    FROM 
        staging_events e
    LEFT JOIN 
        staging_songs s ON e.song = s.title AND e.artist = s.artist_name
    WHERE 
        e.page = 'NextSong';
""")

user_table_insert = ("""INSERT INTO users
    (   
        first_name,
        last_name,
        gender,
        level
    ) SELECT
        first_name,
        last_name,
        gender,
        level
    FROM staging_events
    WHERE NOT EXISTS (
        SELECT 1 FROM users u WHERE u.user_id = staging_events.user_id
    );
""")

song_table_insert = ("""INSERT INTO songs
    (
        song_id,    
        title,
        artist_id,
        year,
        duration
    ) SELECT 
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE NOT EXISTS (
        SELECT 1 FROM songs s WHERE s.song_id = staging_songs.song_id
    );
""")

artist_table_insert = ("""INSERT INTO artists
    (
        artist_id,
        name,
        location,
        latitude,
        longitude
    ) SELECT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE NOT EXISTS (
        SELECT 1 FROM artists a WHERE a.artist_id = staging_songs.artist_id
    );
""")

time_table_insert = ("""INSERT INTO time
    (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    ) SELECT 
        ts,
        DATE_PART(hour, TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS hour,
        DATE_PART(day, TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS day,
        DATE_PART(week, TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS week,
        DATE_PART(month, TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS month,
        DATE_PART(year, TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS year,
        TO_CHAR(TIMESTAMP 'epoch' + 1541105830796/1000 * INTERVAL '1 second', 'Day') AS weekday
    FROM staging_events
    WHERE NOT EXISTS (
        SELECT 1 FROM time t WHERE t.start_time = staging_events.ts
    );
""")
                     
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
