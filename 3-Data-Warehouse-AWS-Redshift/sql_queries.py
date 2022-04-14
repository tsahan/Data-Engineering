import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

S3_LOG_DATA = config.get("S3","LOG_DATA")
S3_LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
S3_SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE_ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist 			VARCHAR,
        auth			VARCHAR,
        firstName 		VARCHAR,
        gender 			CHAR(1),
        itemInSession 	INTEGER,
        lastName 		VARCHAR,
        length 			FLOAT,
        level 			VARCHAR,
        location 		VARCHAR,
        method 			VARCHAR,
        page 			VARCHAR,
        registration 	FLOAT,
        sessionId 		INTEGER,
        song 			VARCHAR,
        status 			INTEGER,
        ts 				TIMESTAMP,
        userAgent 		VARCHAR,
        userId 			INTEGER 
    );
    """
)

staging_songs_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs 			INTEGER,
        artist_id 			VARCHAR,
        artist_latitude 	DOUBLE PRECISION,
        artist_longitude 	DOUBLE PRECISION,
        artist_location 	VARCHAR,
        artist_name 		NVARCHAR,
        song_id 			NVARCHAR,
        title 				VARCHAR,
        duration 			FLOAT,
        year 				INTEGER
    );
    """
)

songplay_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id 	INTEGER 	IDENTITY(0,1) PRIMARY KEY,
        start_time 		TIMESTAMP 	NOT NULL SORTKEY DISTKEY,
        user_id 		INTEGER 	NOT NULL,
        level 			VARCHAR,
        song_id 		VARCHAR 	NOT NULL,
        artist_id 		VARCHAR 	NOT NULL,
        session_id 		INTEGER, 
        location 		VARCHAR,
        user_agent 		VARCHAR 
    );
    """
)

user_table_create = (
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id 	INTEGER PRIMARY KEY SORTKEY,
        first_name 	VARCHAR,
        last_name 	VARCHAR,
        gender 		CHAR(1),
        level 		VARCHAR 
    );
    """
)

song_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songs (
        song_id 	VARCHAR PRIMARY KEY SORTKEY, 
        title 		VARCHAR NOT NULL, 
        artist_id 	VARCHAR NOT NULL, 
        year 		INTEGER, 
        duration 	FLOAT NOT NULL 
    );
    """
)

artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id 	VARCHAR PRIMARY KEY SORTKEY,
        name 		NVARCHAR NOT NULL,
        location 	NVARCHAR,
        latitude 	DOUBLE PRECISION,
        longitude 	DOUBLE PRECISION 
    );  
    """
)

time_table_create = (
    """
    CREATE TABLE IF NOT EXISTS time (
        start_time 	TIMESTAMP PRIMARY KEY DISTKEY SORTKEY,
        hour 		INTEGER,
        day 		INTEGER,
        week 		INTEGER,
        month 		INTEGER,
        year 		INTEGER,
        weekday 	VARCHAR 
    );
    """
)

# STAGING TABLES

staging_events_copy = (
    """
    COPY staging_events FROM {} 
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF
    REGION 'us-west-2'
    TIMEFORMAT 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
    """).format(S3_LOG_DATA, IAM_ROLE_ARN, S3_LOG_JSONPATH)

staging_songs_copy = (
    """
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF
    REGION 'us-west-2'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON 'auto' 
    """).format(S3_SONG_DATA, IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = (
    """
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT to_timestamp(e.ts,'YYYY-MM-DD HH24:MI:SS'), 
           e.userId, 
           e.level, 
           s.song_id, 
           s.artist_id, 
           e.sessionId, 
           e.location, 
           e.userAgent
    FROM staging_events e
    JOIN staging_songs  s  ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page = 'NextSong';
    """
)

user_table_insert = (
    """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId,
           firstName,
           lastName,
           gender,
           level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page = 'NextSong';
    """
)

song_table_insert = (
    """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
           title,
           artist_id,
           year,
           duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
    """
)

artist_table_insert = (
    """
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
           artist_name,
           artist_location,
           artist_latitude,
           artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
    """
)

time_table_insert = (
    """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT to_timestamp(ts,'YYYY-MM-DD HH24:MI:SS'),
           EXTRACT(hour FROM ts),
           EXTRACT(day FROM ts),
           EXTRACT(week FROM ts),
           EXTRACT(month FROM ts),
           EXTRACT(year FROM ts),
           EXTRACT(dayofweek FROM ts)
    FROM staging_events
    WHERE ts IS NOT NULL;
    """
)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
