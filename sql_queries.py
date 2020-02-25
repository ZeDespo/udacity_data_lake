aggregate_songplay_data = ("""
    SELECT l.ts, l.user_id, l.level, s.song_id, a.artist_id, l.session_id, l.location, l.user_agent
    FROM logs l
    JOIN songs s ON l.song_name = s.title
    JOIN artists a ON l.artist_name = a.artist_name
""")  # Performs joins on three dataframes to gather facts of the users in the events log.


artist_select_distinct = ("""
    SELECT 
    artist_id, 
    artist_name, 
    artist_location, 
    artist_latitude, 
    artist_longitude 
    FROM sparkify
    GROUP BY artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    ORDER BY MAX(id) DESC
""")  # Grab the rows in the dataframe that only have the most up-to-date artist information.


get_songplay_log_data = ("""
    SELECT 
    userId as user_id,
    level,
    sessionId as session_id,
    location, 
    userAgent as user_agent,
    song as song_name,
    artist as artist_name, 
    ts
    FROM sparkify
""")  # Standard select query to lay foundation for songplay facts dataframe


user_select_distinct = ("""
    SELECT userId AS user_id, firstName AS first_name, lastName AS last_name, gender, level 
    FROM sparkify 
    GROUP BY user_id, first_name, last_name, gender, level
    ORDER BY MAX(id) DESC
""")  # Grab the rows in the dataframe that only have the most up-to-date user information.

