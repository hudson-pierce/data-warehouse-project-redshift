# AWS Data Warehouse Project

## Scenario
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

## Database and ETL Design

I designed the database with the star schema pattern shown in the diagram below. Where `songplays` is the fact table and `users`, `songs`, `artists`, and `time` are the dimension tables. All of these `CREATE TABLE` statements are run in [create_tables.py](./create_tables.py). 

In order to properly load the S3 data into these tables, I created 2 intermediary tables called `staging_events` and `staging_songs`. I loaded data from the [s3://udacity-dend/log_data](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend?region=us-west-2&prefix=log-data/&showversions=false) bucket into the `staging_events` table and loaded data from the [s3://udacity-dend/song_data](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend?region=us-west-2&prefix=song-data/&showversions=false) bucket into the `staging_songs` table. Then, once the data was loaded into the staging tables, the ETL job proceeds to insert the data from the staging tables into the `songplays`, `users`, `songs`, `artists`, and `time` tables. This ETL process is run via the [etl.py](./etl.py) script. Once you run this script, the data in the tables will be ready for analysis. 

![schema design](schema_design.png)

## Sample Queries

Some sample queries for analyzing the data are provided in [query_runner.py](./query_runner.py). The results of these queries are shown below.
```
------------------------------------
Question: What is the most played song?
Answer: ("You're The One", 37)
------------------------------------
Question: Who is the most played artist?
Answer: ('Muse', 42)
------------------------------------
Question: When is the highest usage time of day by hour for songs?
Answer: (16, 542)
```

## How to Run the Project:
Install requirements:

```pip install -r requirements.txt```

Create tables:

```python3 create_tables.py```

Run ETL script:

```python3 etl.py```

Run analysis queries:

```python3 query_runner.py```
