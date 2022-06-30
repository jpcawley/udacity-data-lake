import configparser
import datetime
import os
import time
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import udf, col, year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType
import pyspark.sql.functions as F
from pyspark.sql.window import *
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Double, StringType as String, IntegerType as Integer, DateType as Date, LongType as Long


def get_config_variables():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
    
    input_data =  = config['PATH']['INPUT_DATA']
    output_data = config['PATH']['OUTPUT_DATA']
    
    return input_data, output_data


def create_spark_session():
    '''
    Function that creates or retrieves an existing SparkSession.
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def read_song_data(spark, input_data):
    '''
    Function that reads in song_data from S3.
    '''

    # adjust the song schema
    songSchema = R([
        Fld('artist_id', String()),
        Fld('artist_latitude', Double()),
        Fld('artist_location', String()),
        Fld('artist_longitude', Double()),
        Fld('artist_name', String()),
        Fld('duration', Double()),
        Fld('num_songs', Integer()),
        Fld('song_id', String()),
        Fld('title', String()),
        Fld('year', Integer()),
    ])

    # get filepath to song data file
    song_data = spark.read.json(os.path.join(
        input_data, 'song_data', '*', '*', '*', '*.json'), schema=songSchema)

    return song_data


def process_song_data(spark, input_data, output_data, song_df):
    '''
    Function that processes song data using PySpark.
    Read from s3, transform data, write to parquet, and write back to s3.

    Parameters:
        spark: the SparkSession
        input_data: s3 bucket path prefix
        output_data: s3 bucket path prefix
    '''

    # extract columns to create songs table
    songs_table = song_df.select(song_df.song_id, song_df.title,
                                 song_df.artist_id, song_df.year, song_df.duration).dropDuplicates()
    # songs_table.show()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').mode(
        'overwrite').parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = song_df.select(song_df.artist_id, song_df['artist_name'].alias('name'), song_df['artist_location'].alias(
        'location'), song_df['artist_latitude'].alias('latitude'), song_df['artist_longitude'].alias('longitude')).dropDuplicates()

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(
        os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data, song_df):
    '''
    Function that processes log data using PySpark.
    Read from s3, transform data, write to parquet, and write back to s3.

    Parameters:
        spark: the SparkSession
        input_data: s3 bucket path prefix
        output_data: s3 bucket path prefix
    '''
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data', '*', '*', '*.json')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    log_df = df.filter((df.page == 'NextSong') & (df.userId != ''))

    # adjust datatypes and column names
    log_df = log_df.selectExpr('cast(artist as string) artist',
                               'cast(auth as string) auth',
                               'cast(firstName as string) first_name',
                               'cast(gender as string) gender',
                               'cast(itemInSession as integer) itemInSession',
                               'cast(lastName as string) last_name',
                               'cast(length as double) length',
                               'cast(level as string) level',
                               'cast(location as string) location',
                               'cast(method  as string) method',
                               'cast(page as string) page',
                               'cast(registration as long) registration',
                               'cast(sessionId as integer) sessionId',
                               'cast(song as string) song',
                               'cast(status as integer) status',
                               'cast(ts as long) ts',
                               'cast(userAgent as string) userAgent',
                               'cast(userId as integer) userId')

    # extract columns for users table
    users_table = log_df.select(log_df.userId, log_df.first_name,
                                log_df.last_name, log_df.gender, log_df.level).dropDuplicates()

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(
        os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(
        x / 1000), TimestampType())
    log_df = log_df.withColumn('start_time', get_timestamp(log_df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(
        lambda x: datetime.datetime.fromtimestamp(x / 1000), DateType())
    spark.udf.register('get_datetime', get_datetime)
    log_df = log_df.withColumn('date_time', get_datetime(log_df.ts))

    # extract columns to create time table
    time_table = log_df.select(log_df.start_time).withColumn('hour', F.hour(log_df.start_time)) \
        .withColumn('day', F.dayofmonth(log_df.start_time)).withColumn('week', F.weekofyear(log_df.start_time)) \
        .withColumn('month', F.month(log_df.start_time)).withColumn('year', F.year(log_df.start_time)) \
        .withColumn('weekday', F.dayofweek(log_df.start_time)).dropDuplicates(['start_time'])

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode(
        'overwrite').parquet(os.path.join(output_data, 'time'))

    # extract columns from joined song and log datasets to create songplays table
    conditions = [log_df.artist == song_df.artist_name,
                  log_df.song == song_df.title, log_df.length == song_df.duration]
    songplays_table = log_df.join(song_df, conditions, 'inner') \
        .select(F.row_number().over(Window.partitionBy().orderBy(log_df.sessionId))
                .alias('songplay_id'), get_timestamp(log_df.ts)
                .alias('start_time'), F.year(log_df.start_time).alias('year'),
                F.month(log_df.start_time).alias(
                    'month'), log_df.userId, log_df.level,
                song_df.song_id, song_df.artist_id, log_df.sessionId, log_df.location, log_df.userAgent) \
        .dropna(how="any", subset=["userId", "sessionId"]).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode(
        'overwrite').parquet(os.path.join(output_data, 'songplays.parquet'))


def main():
    '''
    Main function to perform ETL.
    '''
    start_time = time.time()

    input_data, output_data = get_config_variables()
    spark = create_spark_session()

    # task parallelism: https://knowledge.udacity.com/questions/73278
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "mapreduce.fileoutputcommitter.algorithm.version", "2")
    
    song_df = read_song_data(spark, input_data)

    process_song_data(spark, input_data, output_data, song_df)
    process_log_data(spark, input_data, output_data, song_df)

    end_time = time.time()

    elapsed_time = (end_time - start_time) / 60

    print("Elapsed time: ~{:.2f} seconds or ~{:.2f} minutes.").format(
        (end_time - start_time), elapsed_time)


if __name__ == "__main__":
    main()
