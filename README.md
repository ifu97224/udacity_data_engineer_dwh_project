# ETL Pipeline for Sparkify to analyze Song Play data  
  
#### About this project  
  
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project contains the code for  an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

#### Datasets provided     
  
The data provided consist of 2 folders containing JSON files with the following data:    
  
1.  Song Dataset    
  
The data that resides in the song_data directory is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.  
  
2.  Log Dataset  
  
The data that reises in the log_data directory in JSON format generated by an event simulator based on the songs in the song_data directory. These simulate app activity logs from the music streaming app based.  

#### ETL processs  

The ETL process runs as follows:  

1. On the first run of the code tables are dropped (if they exist) and then empty staging and schema tables are created  
2. Log data and song data are copied into the staging tables  
3. Data is inserted into the schema tables from staging tables  


#### Schema    
  
The schema created has the following fact and dimension tables in order to optimize queries for song play analysis, allowing analysis of song plays with as few reads and joins as possible:    
  
**Fact Table**  
  
*fact_songplay*  
  
The fact_songplay table is the fact table.  Records in log data associated with song plays i.e. records with page = "NextSong".  The table contains the following columns:  
    
songplay_id: Unique ID for each user, each time they play a song  
start_time: The start time for the song (in milliseconds)  
user_id: The unique ID for the user  
level: Indicates if the user is free or paid user  
song_id: Unique ID for a song  
artist_id: Unique ID for an artist  
session_id: Unique ID for an individual user session  
location: City and State of the user  
user_agent: Browser and oprating system used  
  
The artist_id field is used as the 'key' for distribution.  Placing all artists on the same node will likely evenly distribute the data and make joins to the dim_artists table fasters.  As there are so many artists it would not be possible to copy the artists dimension table to every node with 'all' distribution.   
  
**Dimension Tables**  
  
*dim_users*  
  
Information about users in the app  
  
user_id: Unique ID for each user   
first_name: First name of the user  
last_name:  Last name of the user  
gender:  Gender of the user  
level: Indicates if the user is free or paid  
    
The user_id field is the primary key, the sort_key and the distribution key.  The table will be joined to the fact table on user_id so distributing users equally will likely lead to faster joins.
    
*dim_songs*  
  
Songs in the music database  
  
song_id: Unique ID for each song  
title: Song title  
artist_id: Unique ID for each artist  
year: Year the song was released  
duration: Length of the song  

The song_id is the primary key.  The artist_id is the sort key and the distribution key making joins with the fact table on artist_id faster.  
    
*Artists* 
  
Artists in music database  
  
artist_id: Unique ID for each artist  
name: Name of the artist  
location: Location of the artist  
latitude: Latitude of the artist  
longitude: Longitude of the artist  
  
The artist_id is the primary key and distribution key making joins with the fact table on artist_id faster.
  
*Time*  
  
Timestamps of records in songplays broken down into specific units  
  
start_time: Timestamp  
hour: Hour of the timestamp  
day: Day of the timestamp  
week: Week of the timestamp  
month: Month of the timestamp  
year: Year of the timestamp  
weekday: Weekday of the timestamp  
    
The start_time is the primary key and the distribution key.  Since start_time will be used to join to the fact table it should be evenly distributed across the nodes to potentially speed up joins.


#### Files contained in the repo  

The following files are contained within the repo:

*dwh.cfg*

Config file to hold the AWS keys, the configuration of the cluster and paths to the raw data.  

*Create_tables.py*  

This code must be run first, it connects to the database, drops and creates the empty staging and schema tables that will hold the data and are populated in the ETL.  

*sql_queries.py*  

Contains the SQL queries to drop, create and insert data into the staging and schema tables.  

*etl.py*  

The main ETL script that executes the queries to drop, create and insert the data.  

*launch_delete_clusters.py*    

Enables launching and deleting of basic clusters to execute the ETL from the command line.  

*test_create_and_copy.ipynb*  

Contains some code to test the table drop and create queries, the copy commands on single JSON files and the insert queries to populate the schema tables.    

*Test_ETL_Run.ipynb*  

Notebook to test the ETL run, connects to the database and selects the top 10 rows from each of the staging and schema tables.  