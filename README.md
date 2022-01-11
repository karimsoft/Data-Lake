## Project Data Lake
### Data Engineering Nanodegree - Udacity

A data lake is a centralized repository that allows you to store all your structured and unstructured data at any scale. You can store your data as-is, without having to first structure the data, and run different types of analytics—from dashboards and visualizations to big data processing, real-time analytics, and machine learning to guide better decisions.

### Data format

We have a directory called data, contains log data and song data. Here is the data in the song_data:

##### Song Dataset:
{"num_songs": 1, "artist_id": "ARD7TVE1187B99BFB1", "artist_latitude": null, "artist_longitude": null, "artist_location": "California - LA", "artist_name": "Casual", "song_id": "SOMZWCG12A8C13C480", "title": "I Didn't Mean To", "duration": 218.93179, "year": 0}

##### Log Dataset:

{"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}

### Star Schema
We are reading those json file and creating star schema 

##### Fact Table
1.	songplays - records in log data associated with song plays i.e. records with page NextSong 
o	songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
##### Dimension Tables
2.	users - users in the app
o	user_id, first_name, last_name, gender, level
3.	songs - songs in music database
o	song_id, title, artist_id, year, duration
4.	artists - artists in music database
o	artist_id, name, location, lattitude, longitude
5.	time - timestamps of records in songplays broken down into specific units
o	start_time, hour, day, week, month, year, weekday
