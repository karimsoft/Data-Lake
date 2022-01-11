import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, weekofyear,dayofmonth, dayofweek, date_format, hour
import pyspark.sql.types as T

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data' + '/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id',
                            'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet('{output_data}/song_table',
                              mode='overwrite',
                              partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name',
                              'artist_location', 'artist_latitude',
                              'artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet('{output_data}/artist_table',
                                mode='overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =input_data + './data/log_data/*.json'

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df['page'] == 'Next Song')
 
    ##############################################
    ## users
    # extract columns for users table
    user_table = log_df.select( 'userId', 
                            'firstName',
                            'lastName', 
                            'gender', 
                            'level').dropDuplicates()
    # write users table to parquet files
    user_table.write.parquet('{output_data}/user_table', 
                             mode='overwrite')
    
    ##############################################
    ## time
    # create timestamp column from original timestamp column
    to_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), T.TimestampType())
    
    log_df = log_df.withColumn("start_time",to_timestamp(col("ts")))
    
    # create datetime column from original timestamp column                   
    log_df = log_df.withColumn("time_id",col('ts'))
    
    # extract columns to create time table
    time_table =log_df.select(col('ts').alias('time_id'),
                   col('start_time').alias('start_time'),
                   hour('start_time').alias('hour'), 
                   dayofmonth('start_time').alias('day'), 
                   weekofyear('start_time').alias('week'),
                   month('start_time').alias('month'),
                   year('start_time').alias('year'),
                   dayofweek('start_time').alias('weekday')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet('{output_data}/time_table', 
                             mode='overwrite',
                             partitionBy=['year', 'month'])
    
    ##############################################
    ## songplays
    
    # get filepath to song data file
    song_data_path = input_data + 'song_data' + '/*/*/*/*.json'
    
    # read in song data to use for songplays table
    song_df = spark.read.json(song_data_path)

    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_df.join(song_df, \
                                    log_df.artist == song_df.artist_name) \
                              .select('year', 'artist_id',\
                                      'ts', 'userId', 'song_id', \
                                      'artist_id', 'level', 'sessionId',\
                                      'location', 'userAgent') \
                              .dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songplays_table.write.parquet('{output_data}/songplays_table',
                              mode='overwrite',
                              partitionBy=['year', 'artist_id'])
    

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    
    # I have created a bucket named SparkifyDataLakeOutputto save out but
    output_data = "s3n://SparkifyDataLakeOutput"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
