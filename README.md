# Sparkify Data Warehouse

This data warehouse aims to allow the analytics team to understand what songs users are listening to.


## Database Schema

The data model consists of a **star schema** that has one fact table and 4 dimension tables. The fact table is the songplays table that has the records in log data associated with song plays. The dimension tables are the users, songs, artists, and time tables. The users table has the information of users in the app. The songs table has the songs in music database. The artists table has the artist in music database. The time table has the timestamps of records in songplays broken down into specific units. This model was chosen because of some key required benefits for this project like fast aggregations, simplified queries, and denormalization. Also, two staging tables are used to create the previously explained tables.

## ETL Pipeline

The ETL pipeline starts with the create_tables.py. It drops all tables if exist. Later, it creates all tables for the star schema again with all fields and constraints required, so the analytics team can query them efficiently. The sort and dist keys were chosen based on the joins used for this ETL and very likely usage of the analytics team.

The second script is the etl.py. It copies all log and song data from the S3 bucket to the staging tables. Then, the data from these staging tables are transformed to insert all data into the dimensional and fact tables. The  *staging_songs* staging table is used to insert the data into the song and artists tables. The *staging_events* staging table is used to insert the data into the users and time tables. Finally, the data is inserted into the songplays fact table using most of the tables. I suggest automatizing this second script in a daily batch mode in non-working hours so that the Postgress database is always up to date and the ETL does not consume resources nor compete with queries executed by the analytics team during working hours.

## Database and analytical goals

The main goal is to empower the analytics team will be able to query current data in a fast and optimized manner, letting them gather all the information required to make powerful decisions based on data. For example, they can decide to invest more in a certain type of music in a specific season of the year because they know what songs users are listening to by that time.

## Usage

Most of the scripts have comments for a better understanding of what they are doing. You can execute the *Project.ipynb* that drops and creates the tables, and execute the ETL pipeline. Also, the Test.ipynb script tests the tables and some analytic queries so the performance and efficiency are successfully demonstrated.