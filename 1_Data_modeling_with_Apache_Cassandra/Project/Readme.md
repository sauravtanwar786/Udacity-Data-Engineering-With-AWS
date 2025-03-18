## Project: Data Modelling with Apache Cassandra

### Project Purpose

The purpose of the project is to create a NoSQL database and ETL pipeline for A startup called Sparkify that wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.

### Project Discription
In this project I designed and created a NoSQL database using apache cassandra and built an ETL pipeline using Python.

### Database
The purpose of the NoSQL database is to answer queries on song play data. The data model includes a table for each of the following queries:

Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

### ETL Process
The ETL process will read every file contained in the data folder, denormalize its data to fit the apache cassandra tables and put it into a new csv file event_datafile_new.cvs .

### Project Files
This project consists of the following files:

event_data - This is all the data collected on songs and user activity on Sparkfy new music streaming app.
Project_1B_ Project_Template.ipynb - This file contains the Database and ETL code.
event_datafile_new.csv - This csv file denormalised from event_data file that will be used to insert data into the Apache Cassandra tables.
images - A screenshot of the data in the event_datafile_new.csv.

### How To Run
Just run Project_1B_ Project_Template.ipynb to run validation and example queries.

