import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

#### Staging tables

# Create the staging table for the events
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist          VARCHAR(50),
    auth            VARCHAR(20),
    firstName       VARCHAR(50),
    gender          VARCHAR(1),
    itemInSession   INTEGER,
    lastName        VARCHAR(50),
    length          FLOAT,
    level           VARCHAR(4),
    location        VARCHAR,
    method          VARCHAR(3),
    page            VARCHAR(8),
    registration    FLOAT,
    sessionId       INTEGER,
    song            VARCHAR(50),
    status          INTEGER,
    ts              BIGINT,
    userAgent       VARCHAR,
    userId          INTEGER
);
""")

# Create the staging table for the songs
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs           INTEGER,
    artist_id           VARCHAR(18),
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     VARCHAR,
    artist_name         VARCHAR(50),
    song_id             VARCHAR(18),
    title               VARCHAR(50),
    duration            FLOAT,
    year                INT
);
""")

#### Schema tables

# Create the songs fact table
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay
(
    songplay_id    INTEGER IDENTITY(0,1),
    start_time     TIMESTAMP,
    user_id        INTEGER,
    level          VARCHAR(4),
    song_id        VARCHAR(18),
    artist_id      VARCHAR(18) sortkey distkey,
    session_id     INTEGER,
    location       VARCHAR,
    user_agent     VARCHAR
);
""")

# Create the users dimension table
user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users
(
    user_id       INTEGER PRIMARY KEY sortkey distkey,
    first_name    VARCHAR(50),
    last_name     VARCHAR(50),
    gender        VARCHAR(1),
    level         VARCHAR(4)
);
""")

# Create the songs dimension table
song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs
(
    song_id        VARCHAR(18) PRIMARY KEY,
    title          VARCHAR(50),
    artist_id      VARCHAR(18) sortkey distkey,
    year           INTEGER,
    duration       FLOAT
);
""")

# Create the artists dimension table
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists
(
    artist_id      VARCHAR(18) PRIMARY KEY sortkey distkey,
    name           VARCHAR(50),
    location       VARCHAR,
    lattitude      FLOAT,
    longitude      FLOAT
);
""")

# Create the time dimension table
time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time
(
    start_time     TIMESTAMP PRIMARY KEY sortkey distkey, 
    hour           INTEGER, 
    day            INTEGER, 
    week           INTEGER, 
    month          INTEGER, 
    year           INTEGER, 
    weekday        INTEGER
);
""")

# STAGING TABLES - Copy source data into the staging tables

staging_events_copy = ("""
copy {} from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
TRUNCATECOLUMNS
FORMAT AS JSON {}
""").format('staging_events', LOG_DATA, IAM_ROLE, LOG_PATH)


staging_songs_copy = ("""
copy {} from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
TRUNCATECOLUMNS
FORMAT AS JSON 'auto'
""").format('staging_songs', SONG_DATA, IAM_ROLE)

# FINAL TABLES - Insert data into the final schema tables

songplay_table_insert = ("""
INSERT INTO fact_songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
                se.userId AS user_id,
                se.level AS level,
                ss.song_id AS song_id,
                ss.artist_id AS artist_id,
                se.sessionId AS session_id,
                se.location as location,
                se.userAgent as user_agent
FROM staging_events as se
JOIN staging_songs as ss ON se.song = ss.title AND se.artist = ss.artist_name;
""")

    
user_table_insert = ("""
INSERT INTO dim_users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT se.userId AS user_id,
                se.firstName AS first_name,
                se.lastName AS last_name,
                se.gender AS gender,
                se.level AS level
FROM staging_events as se
WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO dim_songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT ss.song_id AS song_id,
                ss.title AS title,
                ss.artist_id AS artist_id,
                ss.year AS year,
                ss.duration AS duration
FROM staging_songs as SS
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO dim_artists(artist_id, name, location, lattitude, longitude)
SELECT DISTINCT ss.artist_id AS artist_id,
                ss.artist_name AS name,
                ss.artist_location AS location,
                ss.artist_latitude AS lattitude,
                ss.artist_longitude AS longitude
FROM staging_songs as SS
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
               EXTRACT(hour from start_time) as hour,
               EXTRACT(day from start_time) as day,
               EXTRACT(week from start_time) as week,
               EXTRACT(month from start_time) as month,
               EXTRACT(year from start_time) as year,
               EXTRACT(weekday from start_time) as weekday
FROM staging_events AS se
WHERE start_time IS NOT NULL;

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
