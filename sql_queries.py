import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config.get('IAM_ROLE','ARN')

LOG_DATA_PATH = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA_PATH = config.get('S3','SONG_DATA')

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
            event_id           INTEGER IDENTITY(0,1) PRIMARY KEY, 
            artist_name        VARCHAR, 
            auth               VARCHAR, 
            first_name         VARCHAR, 
            gender             CHAR(1), 
            item_in_session    INTEGER, 
            last_name          VARCHAR, 
            length             REAL, 
            level              VARCHAR, 
            location           VARCHAR, 
            method             VARCHAR(3), 
            page               VARCHAR, 
            registration       REAL, 
            session_id         INTEGER, 
            song               VARCHAR, 
            status             INTEGER, 
            ts                 TIMESTAMP, 
            user_agent         VARCHAR, 
            user_id            INTEGER);
""")

staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs (
            artist_id          VARCHAR, 
            artist_latitude    REAL, 
            artist_longitude   REAL, 
            artist_location    VARCHAR, 
            artist_name        VARCHAR, 
            song_id            VARCHAR PRIMARY KEY, 
            title              VARCHAR,
            duration           REAL, 
            year               INTEGER);                                 
""")

songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays ( 
            songplay_id        INTEGER IDENTITY(0,1) PRIMARY KEY, 
            start_time         TIMESTAMP NOT NULL, 
            user_id            INTEGER NOT NULL distkey, 
            level              VARCHAR NOT NULL, 
            song_id            VARCHAR NOT NULL, 
            artist_id          VARCHAR NOT NULL, 
            session_id         INTEGER NOT NULL, 
            location           VARCHAR NOT NULL, 
            user_agent         VARCHAR NOT NULL) 
            sortkey(artist_id, session_id, location);
""")

user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users (
            user_id           INTEGER PRIMARY KEY, 
            first_name        VARCHAR NOT NULL, 
            last_name         VARCHAR NOT NULL, 
            gender            CHAR(1) NOT NULL, 
            level             VARCHAR NOT NULL);
""")

song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs ( 
            song_id           VARCHAR PRIMARY KEY, 
            title             VARCHAR NOT NULL, 
            artist_id         VARCHAR NOT NULL distkey, 
            year              INTEGER NOT NULL sortkey, 
            duration          REAL NOT NULL);
""")

artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists ( 
            artist_id         VARCHAR PRIMARY KEY, 
            name              VARCHAR NOT NULL, 
            location          VARCHAR sortkey, 
            latitude          REAL, 
            longitude         REAL) 
            diststyle         ALL;
""")

time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time ( 
            start_time        TIMESTAMP PRIMARY KEY, 
            hour              INTEGER NOT NULL, 
            day               INTEGER NOT NULL, 
            week              INTEGER NOT NULL, 
            month             INTEGER NOT NULL, 
            year              INTEGER NOT NULL, 
            weekday           INTEGER NOT NULL);
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {} 
    credentials 'aws_iam_role={}' 
    region 'us-west-2' 
    json {} -- or 'auto'
    EMPTYASNULL 
    timeformat 'epochmillisecs'; 
""").format(LOG_DATA_PATH, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""copy staging_songs from {}
    credentials 'aws_iam_role={}' 
    region 'us-west-2'
    format as json 'auto';
""").format(SONG_DATA_PATH, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (
                                start_time, 
                                user_id, 
                                level, 
                                song_id, 
                                artist_id, 
                                session_id, 
                                location, 
                                user_agent) 
                            SELECT 
                                ts AS start_time, 
                                user_id, 
                                level, 
                                s.song_id, 
                                s.artist_id, 
                                session_id, 
                                e.location, 
                                user_agent
                            FROM staging_events e 
                            LEFT JOIN staging_songs s 
                                ON (e.song = s.title 
                                AND e.length = s.duration
                                AND e.artist_name = s.artist_name)
                            WHERE e.page = 'NextSong'
                                AND e.user_id IS NOT NULL
                                AND e.level IS NOT NULL
                                AND e.session_id IS NOT NULL
                                AND e.location IS NOT NULL
                                AND e.user_agent IS NOT NULL                                
                                AND e.ts IS NOT NULL
                                AND s.song_id IS NOT NULL
                                AND s.artist_id IS NOT NULL;                           
""")

user_table_insert = ("""INSERT INTO users (
                            user_id, 
                            first_name, 
                            last_name, 
                            gender, 
                            level) 
                        SELECT 
                            DISTINCT user_id, 
                            first_name, 
                            last_name, 
                            gender, 
                            level 
                        FROM staging_events e 
                        WHERE e.page = 'NextSong' 
                            AND e.user_id IS NOT NULL
                            AND e.ts = (SELECT max(ts)
                                        FROM staging_events e2
                                        WHERE e.user_id = e2.user_id);
""")

song_table_insert = ("""INSERT INTO songs (
                            song_id, 
                            title, 
                            artist_id, 
                            year, 
                            duration)
                        SELECT 
                            song_id, 
                            title, 
                            artist_id, 
                            year, 
                            duration
                        FROM staging_songs;
""")

artist_table_insert = ("""INSERT INTO artists (
                                artist_id, 
                                name, 
                                location, 
                                latitude, 
                                longitude)
                            SELECT 
                                artist_id, 
                                artist_name, 
                                artist_location, 
                                artist_latitude, 
                                artist_longitude 
                            FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO  time (
                            start_time, 
                            hour, 
                            day, 
                            week, 
                            month, 
                            year, 
                            weekday)
                        SELECT 
                            ts AS start_time,
                            EXTRACT(hour FROM ts) AS hour, 
                            EXTRACT(day FROM ts) AS day, 
                            EXTRACT(week FROM ts) AS week, 
                            EXTRACT(month FROM ts) AS month, 
                            EXTRACT(year FROM ts) AS year, 
                            EXTRACT(weekday FROM ts) AS weekday 
                        FROM staging_events 
                        WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [song_table_insert, artist_table_insert, time_table_insert, user_table_insert, songplay_table_insert]