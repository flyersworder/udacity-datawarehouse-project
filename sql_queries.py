import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession int,
        lastName varchar,
        length numeric,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration bigint,
        sessionId int,
        song varchar,
        status int,
        ts timestamp,
        userAgent varchar,
        userId int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs int,
        artist_id varchar,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location varchar(max),
        artist_name varchar(max),
        song_id varchar,
        title varchar(max),
        duration numeric,
        year int
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id bigint IDENTITY(0, 1) PRIMARY KEY NOT NULL SORTKEY, 
        start_time timestamp NOT NULL, 
        user_id int NOT NULL, 
        level varchar, 
        song_id varchar NOT NULL, 
        artist_id varchar NOT NULL, 
        session_id int, 
        location varchar(max), 
        user_agent varchar
    ) DISTSTYLE AUTO;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int PRIMARY KEY NOT NULL SORTKEY, 
        first_name varchar, 
        last_name varchar, 
        gender varchar, 
        level varchar
    ) DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY NOT NULL SORTKEY, 
        title varchar(max), 
        artist_id varchar, 
        year int, 
        duration numeric
    ) DISTSTYLE ALL;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY NOT NULL SORTKEY, 
        name varchar(max), 
        location varchar(max), 
        latitude numeric, 
        longitude numeric
    ) DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp PRIMARY KEY NOT NULL SORTKEY, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday int
    ) DISTSTYLE AUTO;
""")

# STAGING TABLES

staging_events_copy = (f"""
    copy staging_events from {config['S3']['LOG_DATA']}
    credentials 'aws_iam_role={config['IAM_ROLE']['ARN']}'
    format as json {config['S3']['LOG_JSONPATH']}
    timeformat as 'epochmillisecs';
""")

staging_songs_copy = (f"""
    copy staging_songs from {config['S3']['SONG_DATA']}
    credentials 'aws_iam_role={config['IAM_ROLE']['ARN']}'
    format as json 'auto' compupdate off acceptinvchars;
""")

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT ts, userId, level, song_id, artists.artist_id, sessionId, artists.location, userAgent
    FROM staging_events, songs, artists
    WHERE songs.artist_id = artists.artist_id
    AND songs.title = staging_events.song
    AND artists.name = staging_events.artist
    AND songs.duration = staging_events.length
    AND page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events
    WHERE userId IS NOT NULL
    AND ts IN (SELECT MAX(ts) FROM staging_events GROUP BY userId);
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT ts, EXTRACT(h FROM ts), EXTRACT(d FROM ts), EXTRACT(w FROM ts), EXTRACT(mon FROM ts), EXTRACT(y FROM ts), EXTRACT(dw FROM ts) FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
