import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, year, month, dayofmonth, hour,
    weekofyear, date_format, dayofweek, max, monotonically_increasing_id
from pyspark.sql.types import TimestampType
import logging
import boto3
from botocore.exceptions import ClientError
import sys

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Create a spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Process song data files and write the results to S3
    
    Parameters:
        spark: spark cursor
        input_data: S3 folder where song and event log files are
        output_data: S3 folder where results will be saved
        
    Returns: None
    '''    
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/A/*.json"
#     song_data = "data/song_data/*/*/*/*.json"
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data")
    
    # extract columns to create songs table
    songs_table = spark.sql('''
        SELECT DISTINCT 
            song_id
            , title
            , artist_id
            , year
            , duration 
        FROM song_data 
        ''')
    print("     finished processing song table")
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs.parquet",mode='overwrite',partitionBy=('year','artist_id'))

    # extract columns to create artists table
    artists_table = spark.sql('''
        SELECT DISTINCT artist_id
            , artist_name as name
            , artist_location as location
            , artist_latitude as latitude
            , artist_longitude as longitude
        FROM song_data
        ''')
    print("     finished processing artist table")
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists.parquet',mode='overwrite')


def process_log_data(spark, input_data, output_data):
    '''
    Process log files and write the results to S3 output folder
        
    Parameters:
        spark: spark curson
        input_data: S3 folder where song and event log files are
        output_data: S3 folder where results will be saved
        
    Returns: None
    '''
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
#     log_data = "data/log-data/*.json"
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df.createOrReplaceTempView("log_data")
    df = spark.sql('''SELECT * 
        FROM log_data 
        WHERE page = 'NextSong'
        ''')

    df.createOrReplaceTempView("log_data")
    # extract columns for users table    
    users_table = spark.sql('''
        SELECT DISTINCT 
            userId as user_id
            , firstName as first_name
            , lastName as last_name
            , gender
            , level
        FROM log_data
    ''')
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users.parquet',mode='overwrite')
    print("     finished processing user table")
    
    # create timestamp column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000).replace(microsecond=0),TimestampType())
    
    df = df.withColumn("start_time", get_datetime("ts"))
    df = df.withColumn("weekday", dayofweek("start_time"))
    
    df.createOrReplaceTempView("log_data")    
    
    # extract columns to create time table
    time_table = spark.sql('''
        SELECT 
            start_time
            , extract(hour from start_time) hour
            , extract(day from start_time) day
            , extract(week from start_time) week
            , extract(month from start_time) month
            , extract(year from start_time) year
            , weekday
        FROM log_data
        ''')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time.parquet',mode='overwrite',partitionBy=('year','month'))
    print("     finished processing time table")
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs.parquet")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
        SELECT 
            A.start_time
            , A.userId as user_id
            , A.level
            , B.song_id
            , B.artist_id
            , A.sessionId as session_id
            , A.location
            , A.userAgent as user_agent
            , extract(month from A.start_time) month
            , extract(year from A.start_time) year 
        FROM log_data A
        JOIN song_data B 
            ON A.artist = B.artist_name 
                AND A.length = B.duration 
                AND A.song = B.title
    ''')
    songplays_table = songplays_table.withColumn('songplay_id',monotonically_increasing_id())

    songplays_table.write.parquet(output_data + 'songplays.parquet',mode='overwrite',partitionBy=('year','month'))
    print("     finished processing fact songplays table")
    
def main():
    '''
    Create a spark session and process the song files and log files
    '''    
    
    spark = create_spark_session()
    input_data =  "s3a://udacity-dend/"
    output_data = "s3a://sparkify-datalake-mma/"
    
    print("Process song data...")
    process_song_data(spark, input_data, output_data)
    
    print("Process log data...")
    process_log_data(spark, input_data, output_data)
    print("All data processed !")

if __name__ == "__main__":
    main()
