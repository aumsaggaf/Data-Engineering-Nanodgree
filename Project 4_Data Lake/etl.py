import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create the Spark Session
    
    Create the Spark Session

    Returns:
    spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process songs data and extract songs and artists tables
    
    Process the songs data from the json files specified from the input_data from S3 and processes it by extracting the songs and artists tables and then loaded back to S3.
    
    Parameters: 
    spark (create_spark_session()): Create spark session
    input_data ("s3a://udacity-dend/"): the data location that will be process
    output_data ("s3a://dend-4th-project/"): the table location after extracting
    """    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data+'songs_table/')

    # extract columns to create artists table
    artists_table = df.select("artist_id", col("artist_name").alias("name"), col("artist_location").alias("location"), col("artist_latitude").alias("latitude"), col("artist_longitude").alias("longitude")).distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
    Process log data and extract users, time, and songplays tables
    
    Process the logs data from the json files specified from the input_data from S3 and processes it by extracting the users, time, and songplays tables and then loaded back to S3.
    
    Parameters: 
    spark (create_spark_session()): Create spark session
    input_data ("s3a://udacity-dend/"): the data location that will be process
    output_data ("s3a://dend-4th-project/"): the table location after extracting
    """    
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # extract columns for users table
    users_table = df.select(col("userId").alias("user_id"), col("firstName").alias("first_name"), col("lastName").alias("last_name"), "gender", "level").distinct()
    
    # write users table to parquet files
    users_table.write.parquet(output_data+'users_table/')

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = df.withColumn("ts_timestamp", F.to_timestamp(F.from_unixtime((col("ts") / 1000) , 'yyyy-MM-dd HH:mm:ss.SSS')).cast("Timestamp"))
    
    # extract columns to create time table
    time_table = (df.withColumn("hour", hour(col("ts_timestamp")))
                  .withColumn("day", dayofmonth(col("ts_timestamp")))
                  .withColumn("week", weekofyear(col("ts_timestamp")))
                  .withColumn("month", month(col("ts_timestamp")))
                  .withColumn("year", year(col("ts_timestamp")))
                 ).select(col("ts_timestamp").alias("start_time"), col("hour"), col("day"), col("week"), col("month"), col("year"), date_format("ts_timestamp", "EEEE").alias("weekday"))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data+'time_table/')
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs_table/')
    
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.withColumn("monthPlay", month(col("ts_timestamp"))).withColumn("yearPlay", F.monotonically_increasing_id()).join(song_df, song_df.title == df.song).select("songplay_id", col("ts_timestamp").alias("start_time"), col("monthPlay"), col("yearPlay"), col("userId").alias("user_id"), "level", "song_id", "artist_id", col("sessionId").alias("session_id"), "location", col("userAgent").alias("user_agent"))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("yearPlay", "monthPlay").parquet(output_data+'songplays_table/')


def main():
    """
    Run the previous functions and define their parameters.

    Define the parameters needed for the previous functions and run them to process the data from S3 (input_data) and loaded back to S3 (output_data).
    
    Usage: python etl.py
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-4th-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
