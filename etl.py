
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType as St, StructField as SSFld, DoubleType as dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Create or retrieve a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Description: This function loads song_data from S3 and processes it by extracting the songs and artist tables
        and then again loaded back to S3
        
        Parameters:
            spark       : Spark Session
            input_data  : location of song_data json files with the songs metadata
            output_data : S3 bucket were dimensional tables in parquet format will be stored
    """
    
    song_data = input_data + 'song_data/*/*/*/*.json'

    songSchema = St([
        SFld("artist_id",Str()),
        SFld("artist_latitude",dbl()),
        SFld("artist_location",Str()),
        SFld("artist_longitude",dbl()),
        SFld("artist_name",Str()),
        SFld("duration",dbl()),
        SFld("num_songs",Int()),
        SFld("title",Str()),
        SFld("year",Int()),
    ])
    
    df = spark.read.json(song_data, schema = songSchema)


    # Selecting song fields for songs_table
    song_fields = ["title", "artist_id","year", "duration"]

    # adding monotonically increasing id for song_id column in songs table
    songs_table = df.select(song_fields).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    
    # Writing table data to S3 paraquet file format
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs_table/', mode='overwrite')

    artists_fields = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]

    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_longitude', 'artist_latitude')
    artists_table = df.selectExpr(artists_fields)
                        .withColumnRenamed('artist_name', 'name').
                         withColumnRenamed('artist_location', 'location').
                         withColumnRenamed('artist_latitude', 'latitude').
                         withColumnRenamed('artist_longitude', 'longitude').dropDuplicates()


    artists_table.write.parquet(output_data + 'artists/', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
        Description: This function loads log_data from S3 and processes it by extracting the songs and artist tables
        and then again loaded back to S3. Also output from previous function is used in by spark.read.json command
        
        Parameters:
            spark       : Spark Session
            input_data  : location of log_data json files with the events data
            output_data : S3 bucket were dimensional tables in parquet format will be stored
            
    """
    
    log_data = input_data + 'log_data/*/*/*.json'

    df = spark.read.json(log_data)
    
    df = df.filter(df.page == 'NextSong')

    users_fields = ["userdId", "firstName", "lastName", "gender", "level"]
    users_table = df.selectExpr(users_fields).\
                        withColumnRenamed('userId', 'user_id').\
                        withColumnRenamed('firstName', 'first_name').\
                        withColumnRenamed('lastName', 'last_name').\
                        withColumnRenamed('gender', 'gender').\
                        withColumnRenamed('level', 'level').dropDuplicates()

    user_output_path = output_data+'users_table/'
    users_table.write.parquet(user_output_path)
    
    # create datetime column from original timestamp column
    get_datetime = udf( lambda ts: datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M%:%S"), Str())
    df = df.withColumn("start_time", get_datetime('ts'))

    time_table = df.select("start_time").dropDuplicates() \
        .withColumn("hour", hour(col("start_time")).withColumn("day", day(col("start_time")) \
        .withColumn("week", week(col("start_time")).withColumn("month", month(col("start_time")) \
        .withColumn("year", year(col("start_time")).withColumn("weekday", date_format(col("start_time"), 'E'))
                    
    songs_table.write.partitionBy("year", "month").parquet(output_data + 'time/')

    df_songs = spark.read.parquet(output_data + 'songs/*/*/*')

    df_artists = spark.read.parquet(output_data + 'artists/*')

    songs_logs = df.join(songs_df, (df.song == songs_df.title))
    artists_songs_logs = songs_logs.join(df_artists, (songs_logs.artist == df_artists.name))

    songplays = artists_songs_logs.join(
        time_table,
        artists_songs_logs.ts == time_table.start_time, 'left'
    ).drop(artists_songs_logs.year)

    songplays_table = songplays.select(
        col('start_time').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        col('month').alias('month'),
    ).repartition("year", "month")

    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')


def main():
    """
        Extract songs and events data from S3, Transform it into dimensional tables format, and Load it back to S3 in Parquet format
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()