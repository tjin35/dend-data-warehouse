import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender CHAR(1), 
    item_in_session INTEGER,
    last_name VARCHAR,
    length DECIMAL,
    level CHAR(4),
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration NUMERIC,
    session_id INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    user_agent VARCHAR,
    user_id INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs INTEGER,
    artist_id VARCHAR(18),
    artist_latitude VARCHAR,
    artist_longitude VARCHAR,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR(18),
    title VARCHAR,
    duration FLOAT,
    year INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
    start_time TIMESTAMP NOT NULL, 
    user_id INTEGER NOT NULL, 
    level CHAR(4), 
    song_id VARCHAR, 
    artist_id VARCHAR, 
    session_id INTEGER, 
    location VARCHAR, 
    user_agent VARCHAR
);

""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER PRIMARY KEY, 
    first_name VARCHAR NOT NULL, 
    last_name VARCHAR NOT NULL, 
    gender CHAR(1), 
    level CHAR(4)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(18) PRIMARY KEY, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR, 
    year INTEGER, 
    duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(18) PRIMARY KEY, 
    name VARCHAR NOT NULL, 
    location VARCHAR, 
    lattitude VARCHAR, 
    longitude VARCHAR
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY, 
    hour INTEGER NOT NULL, 
    day INTEGER NOT NULL, 
    week INTEGER NOT NULL, 
    month INTEGER NOT NULL, 
    year INTEGER NOT NULL, 
    weekday INTEGER NOT NULL
);
""")

# STAGING TABLES
staging_events_copy = (""" 
COPY staging_events FROM {}
iam_role  '{}'
FORMAT AS JSON {};
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
COPY staging_songs FROM {}
iam_role  '{}'
FORMAT AS JSON 'auto';
""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS start_time, user_id, level, song_id, artist_id, session_id, location, user_agent 
FROM staging_events se LEFT JOIN staging_songs ss 
ON se.song = ss.title AND se.artist = ss.artist_name WHERE page = 'NextSong';
""")
    
user_table_insert = ("""
INSERT INTO users (user_id,first_name,last_name,gender,level)
SELECT DISTINCT user_id, first_name, last_name, gender, level FROM staging_events WHERE page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id,title,artist_id,year,duration)
SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,name,location,lattitude,longitude) 
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (start_time,hour,day,week,month,year,weekday)
SELECT DISTINCT (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS start_time,
EXTRACT(hour FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS hour,
EXTRACT(day FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS day,
EXTRACT(week FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS week,
EXTRACT(month FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS month,
EXTRACT(year FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS year,
EXTRACT(dayofweek FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS weekday
FROM staging_events WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
