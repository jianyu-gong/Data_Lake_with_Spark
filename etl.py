import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType, TimestampType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CREDENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function is used to create Spark session. Input Spark config into function.
    :param N/A
    :param N/A
    :return: N/A
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function is used to extract data from song json file and select necessary columns into song table and artists table. 
    Write and save tables in S3 as parquet.
    :param spark: Spark Session
    :param input_data: input data file path - in this case S3 path
    :param output_data: output data file path - in this case S3 path
    :return: N/A
    """ 
    # get filepath to song data file
    song_data = input_data + "song-data/*/*/*/*.json"
    
    # define table schema
    songdata_schema = StructType([
        StructField("artist_id", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_longitude", DoubleType(), True),
        StructField("artist_name", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("num_songs", IntegerType(), True),     
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("year", IntegerType(), True), 
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=songdata_schema)
    
    # create a temporary table
    df.createOrReplaceTempView("songs")

    # extract columns to create songs table
    songs_table = spark.sql(''' SELECT song_id, artist_id, year, duration
                                FROM songs
                            ''')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = spark.sql(''' SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                                  FROM songs
                              ''')
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """
    This function is used to extract data from logs json file and select necessary columns into user table, 
    time table and songplay table. Write and save tables in S3 as parquet.
    :param spark: Spark Session
    :param input_data: input data file path - in this case S3 path
    :param output_data: output data file path - in this case S3 path
    :return: N/A
    """ 
    # get filepath to log data file
    log_data = input_data + "log-data"
    
    # define table schema
    logdata_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", LongType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", LongType(), True),
        StructField("song", StringType(), True),
        StructField("status", LongType(), True),
        StructField("ts", TimestampType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", IntegerType(), True),
    ])

    # read log data file
    df = spark.read.json(log_data, schema=logdata_schema)
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # create a temporary table
    df.createOrReplaceTempView("logs")
    
    # extract columns for users table    
    users_table = spark.sql(''' SELECT distinct userId as user_id, firstName as first_name,\
                                       lastName as last_name, gender, level
                                FROM logs 
                            ''')
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users")
    
    # extract columns to create time table
    time_table = spark.sql('''
                          SELECT distinct datetime as start_time, hour(datetime) as hour, day(datetime) as day, 
                          weekofyear(datetime) as week, month(datetime) as month, year(datetime) as year
                          FROM logs
                          '''
                          )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month ').parquet(output_data + "time")

    # # read in song data to use for songplays table
    # song_df = spark.sql(''' SELECT song_id, artist_id, year, duration
    #                         FROM songs
    #                     ''')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
                                SELECT l.datetime as start_time, l.userId as user_id, l.level, qry.song_id, qry.artist_id, 
                                l.sessionId as session_id, l.location, l.userAgent as user_agent
                                FROM logs l
                                LEFT JOIN (SELECT artist_id, artist_name, song_id, title FROM songs) as qry
                                ON (l.song = qry.title AND l.artist = qry.artist_name)
                                WHERE l.page = 'NextSong'
                                ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dataengineerp3/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
