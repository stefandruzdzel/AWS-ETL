import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

S3_LOG_DATA         = config.get('S3', 'LOG_DATA')
S3_LOG_JSONPATH     = config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA        = config.get('S3', 'SONG_DATA')
DWH_IAM_ROLE_ARN    = config.get("IAM_ROLE", "ARN")

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
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist          VARCHAR NOT NULL,
        auth            VARCHAR NOT NULL,
        firstName       VARCHAR NOT NULL,
        gender          VARCHAR NOT NULL,
        itemInSession   INTEGER NOT NULL,
        lastName        VARCHAR NOT NULL,
        length          DECIMAL NOT NULL,
        location        VARCHAR NOT NULL,
        method          VARCHAR NOT NULL,
        page            VARCHAR NOT NULL,
        registration    BIGINT NOT NULL,
        sessionId       INTEGER NOT NULL,
        song            VARCHAR NOT NULL,
        status          INTEGER NOT NULL,
        ts              TIMESTAMP NOT NULL,
        userAgent       VARCHAR NOT NULL,
        userId          INTEGER NOT NULL
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs           INTEGER NOT NULL,
        artist_id           VARCHAR NOT NULL,
        artist_latitude     DECIMAL NOT NULL,
        artist_longitude    DECIMAL NOT NULL,
        artist_name         VARCHAR NOT NULL,
        song_id             VARCHAR NOT NULL,
        title               VARCHAR NOT NULL,
        duration            DECIMAL NOT NULL,
        year                INTEGER NOT NULL
    )
    """)

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id     INTEGER PRIMARY KEY, 
        start_time      INTEGER NOT NULL, 
        user_id         INTEGER NOT NULL, 
        level           VARCHAR NOT NULL, 
        song_id         VARCHAR, 
        artist_id       VARCHAR, 
        session_id      INTEGER NOT NULL, 
        location        VARCHAR NOT NULL, 
        user_agent      VARCHAR NOT NULL
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id     INTEGER PRIMARY KEY,
        first_name  VARCHAR NOT NULL, 
        last_name   VARCHAR NOT NULL, 
        gender      VARCHAR NOT NULL, 
        level       VARCHAR NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id     VARCHAR PRIMARY KEY, 
        title       VARCHAR NOT NULL, 
        artist_id   VARCHAR NOT NULL, 
        year        INTEGER NOT NULL, 
        duration    DECIMAL NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id   VARCHAR PRIMARY KEY, 
        name        VARCHAR NOT NULL, 
        location    VARCHAR NOT NULL, 
        lattitude   DECIMAL NOT NULL, 
        longitude   DECIMAL NOT NULL
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time  INTEGER PRIMARY KEY, 
        hour        INTEGER NOT NULL, 
        day         INTEGER NOT NULL, 
        week        INTEGER NOT NULL, 
        month       INTEGER NOT NULL, 
        year        INTEGER NOT NULL, 
        weekday     INTEGER NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role '{}'
    FORMAT AS json 'auto'
    """).format(S3_LOG_DATA, DWH_IAM_ROLE_ARN, S3_LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    iam_role '{}'
    FORMAT AS json 'auto'
    """).format(S3_SONG_DATA, DWH_IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT to_timestamp(to_char(events.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS') AS start_time,
        events.userId AS user_id,
        events.level AS level,
        songs.song_id AS song_id,
        songs.artist_id AS artist_id,
        events.sessionId AS session_id,
        events.location AS location,
        events.userAgent AS user_agent
    FROM staging_events events
    JOIN staging_songs songs ON (events.song = songs.title AND events.artist = songs.artist_name)
    AND events.page  =  'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT(userId) AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong' 
    AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id) AS song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id) as artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT to_timestamp(to_char(staging_events.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS') AS start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            EXTRACT(dayofweek FROM start_time) as weekday
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
