import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN=config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS  users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (artist varchar , auth varchar, \
firstName varchar, gender varchar, itemInSession int, \
lastName varchar, length numeric , level varchar,location varchar,\
method varchar, page varchar sortkey,registration bigint, \
sessionId bigint, song varchar distkey, status bigint, \
ts bigint, userAgent varchar, userId bigint );
""")


staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (num_songs bigint, artist_id varchar distkey, \
artist_latitude varchar,artist_longitude varchar, artist_location varchar,  \
artist_name varchar, song_id varchar sortkey, title varchar,\
duration numeric, year bigint);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id bigint identity(0, 1) sortkey, \
start_time varchar, user_id int , level varchar, \
song_id varchar NOT NULL distkey, artist_id varchar NOT NULL , session_id int ,\
location varchar, user_agent varchar);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users ( user_id int sortkey distkey, \
first_name varchar, last_name varchar, gender varchar, \
level varchar);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs ( song_id varchar sortkey, \
title varchar, artist_id varchar, year int, duration numeric) \
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists ( artist_id varchar sortkey, \
name varchar , location varchar,  \
latitude varchar, longitude varchar) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time ( start_time varchar sortkey distkey, \
hour varchar, day varchar, week varchar, month varchar, \
year varchar, weekday varchar);
""")

# STAGING TABLES

staging_events_copy = ("copy staging_events from \
's3://udacity-dend-2/log_data/' iam_role '{}' \
json 'auto ignorecase' region 'us-east-1';").format(DWH_ROLE_ARN) 


staging_songs_copy = ("copy staging_songs from \
's3://udacity-dend-2/song_data/' iam_role '{}' \
json 'auto ignorecase' region 'us-east-1';").format(DWH_ROLE_ARN)


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time,user_id,level,song_id,artist_id, \
session_id,location,user_agent) \
select timestamp 'epoch' + a.ts/1000 * interval '1 second' AS start_time \
,a.userid , \
a."level" , \
case when b.songplay_id is null then 'none' \
else b.songplay_id end as songplay_id , \
case when b.artist_id is null then 'none' \
else b.artist_id end as artist_id, \
a.sessionid , \
a.location, \
a.useragent \
from staging_events a left join \
(  \
Select distinct a.song_id songplay_id, b.artist_id, \
b.name, a.title,a.duration  from  \
songs a join  \
artists b on \
a.artist_id=b.artist_id \
) b \
on trim(a.song)= trim(b.name) \
and trim(a.artist) = trim(b.title) \
and trim(a.length) = trim(b.duration) \
where a.page='NextSong'; 
""")

user_table_insert = ("""
INSERT INTO users (user_id,first_name,last_name,gender,level) \
select userid, firstname , lastname ,gender ,"level"  \
from staging_events where userid is not null and page= 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) \
select  \
song_id,title,artist_id,year,duration  \
from staging_songs where song_id is not null;  \
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) \
select artist_id , artist_name, artist_location,artist_latitude,\
artist_longitude from staging_songs where artist_id is not null;
""")

time_table_insert = ("""
INSERT INTO time (start_time,hour,day,week,month,year,weekday)  \
SELECT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time, \
extract(hour from start_time) as hour_time,  \
extract(day from start_time) as day_time,  \
extract(week from start_time) as week_time,  \
extract(month from start_time) as month_time,  \
extract(year from start_time) as year_time,  \
extract(weekday from start_time) as weekday_time  \
from staging_events  \
where page= 'NextSong'  \
and start_time is not null;
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
