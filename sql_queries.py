# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS song_play"
user_table_drop = " DROP TABLE IF EXISTS users "
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS  artists"
time_table_drop = "DROP TABLE IF EXISTS time_info"

# CREATE TABLES

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songs_play\
    (\
        songplay_id INT GENERATED ALWAYS AS IDENTITY, \
        start_time TIMESTAMP NOT NULL, \
        user_id INT NOT NULL, \
        level VARCHAR(50), \
        song_id VARCHAR(25) NOT NULL, \
        artist_id VARCHAR(25) NOT NULL, \
        session_id VARCHAR(25) NOT NULL, \
        location VARCHAR(500), \
        user_agent TEXT,\
        PRIMARY KEY (songplay_id),\
    CONSTRAINT fk_user  FOREIGN KEY(user_id) \
   REFERENCES users(user_id)\
   ON DELETE RESTRICT\
   ON UPDATE RESTRICT,\

   CONSTRAINT fk_artist FOREIGN KEY(artist_id)\
   REFERENCES artists(artist_id)\
   ON DELETE RESTRICT\
   ON UPDATE RESTRICT,\
    
   CONSTRAINT fk_song FOREIGN KEY(song_id) \
   REFERENCES songs(song_id)\
   ON DELETE RESTRICT\
   ON UPDATE RESTRICT,\
              
   CONSTRAINT fk_time FOREIGN KEY(start_time) \
   REFERENCES time_info(start_time)\
   ON DELETE RESTRICT\
   ON UPDATE RESTRICT\
       );      
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (user_id INT NOT NULL, \
    first_name VARCHAR(50) NOT NULL, \
    last_name VARCHAR(50) NOT NULL,\
    gender VARCHAR(10), \
    level VARCHAR(50), \
    PRIMARY KEY (user_id));
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR(50) NOT NULL, \
    title VARCHAR(500) NULL, \
    artist_id VARCHAR(25) NOT NULL,\
    year INT, \
    duration NUMERIC, \
    PRIMARY KEY(song_id));
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists(artist_id VARCHAR(50) NOT NULL, \
    name VARCHAR(500) NULL, \
    location VARCHAR(250), \
    latitude DECIMAL(11,8), \
    longitude DECIMAL(11,8), \
    PRIMARY KEY(artist_id));
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time_info (start_time TIMESTAMP NOT NULL,\
    hour INT, \
    day INT, \
    week INT, \
    month INT, \
    year INT, \
    weekday VARCHAR(10), \
    PRIMARY KEY(start_time));
""")

# INSERT RECORDS

songplay_table_insert = (""" INSERT INTO songs_play(start_time, \
    user_id , \
    level, \
    song_id, \
    artist_id, \
    session_id, \
    location, \
    user_agent) \
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = (""" INSERT INTO users(user_id, \
    first_name, \
    last_name, \
    gender,level) \
    VALUES(%s,%s,%s,%s,%s);
""")

song_table_insert = ("""INSERT INTO songs(song_id,\
    title,\
    artist_id,\
    year, \
    duration)\
    VALUES (%s,%s,%s,%s,%s);
""")

artist_table_insert = (""" INSERT INTO artists(artist_id, \
    name, \
    location, \
    latitude, \
    longitude) \
    VALUES(%s,%s,%s,%s,%s);
""")


time_table_insert = (""" INSERT INTO time_info(start_time, \
    hour, \
    day, \
    week, \
    month, \
    year, \
    weekday) \
    VALUES (%s,%s,%s,%s,%s,%s,%s);
""")

# FIND SONGS

song_select = ("""  SELECT s.song_id, a.artist_id \
    FROM songs s\
    JOIN artists a ON s.artist_id = a.artist_id\
    WHERE s.title = %s AND a.name = %s AND s.duration = %s;
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]