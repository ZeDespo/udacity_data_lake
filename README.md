# *SparkifyDB*

Using Spark, ingest the user logs and song metadata to form an analytical datalake on S3.

*Note: With regards to the code, both iterative and declarative approaches were used to demonstrate familiarity
with each approach. Mostly, the declarative approach was used for the more complicated queries.*

Sparkify's analysts wish to determine which songs each user is listening to, thus one must use a singular fact table
and multiple dimensional json files (the datalake equivalent of a star schema). All statistical information is placed
within the fact json, whereas metadata like song name, user id, user agents, are placed in the dimension jsons. For all
intents and purposes, the files will be described in SQL terms.

## Songplay

Holds all factual data for queries to perform JOINS onto. Comprised of data from both the events and songs JSON logs. 
Contains some data not viewable in other tables (user_agent, session_id)_
```
songplay_id INT AUTOINCREMENT NOT NULL
start_time timestamp NOT NULL, 
user_id int NOT NULL, 
level varchar(4) NOT NULL, 
song_id varchar,
artist_id varchar, 
session_id int NOT NULL, 
user_agent varchar NOT NULL, 
location varchar NOT NULL
```

## Songs

Each record pertains to a song by some artist / group. Made up solely from the song logs.
```
song_id varchar PRIMARY KEY NOT NULL, 
title varchar, 
artist_id varchar NOT NULL, 
year int, 
duration real
```

## Artists

Each record pertains to an artist or group that has recorded songs. Made up solely from the song logs.
```
artist_id varchar PRIMARY KEY NOT NULL,
name varchar, 
location varchar, 
latitude varchar, 
longitude varchar
```

## Time

Filtered by users actively listening to some song, holds information on all times users queried Sparkify. 
Made up solely from the event logs.
```
start_time timestamp PRIMARY KEY NOT NULL,
hour int,
day int, 
week int, 
month int, 
year int, 
weekday varchar
```


## Users
Filtered by users actively listening to some song, holds all user specific information. Made up solely from the 
event logs.
```
user_id int PRIMARY KEY NOT NULL,
first_name varchar,
last_name varchar,
gender varchar(1), 
level varchar(4) NOT NULL
```