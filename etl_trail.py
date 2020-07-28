import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
import re
from pyspark.sql.types import StructType as St, StructField as Sfld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType

from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
	spark = SparkSession \
		.builder \
		.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
		.getOrCreate()
	return spark


def process_song_data(spark, input_data, output_data):
	# get filepath to song data file
	song_data = input_data + '*/*/*/*.json'
	
	# creating schema fo our song_data
	
	songs_model = St([
		Sfld("num_songs", Int()),
		Sfld("artist_id", Str()),
		Sfld("artist_latitude", Dbl()),
		Sfld("artist_longitude", Dbl()),
		Sfld("artist_location", Str()),
		Sfld("artist_name", Str()),
		Sfld("song_id", Str()),
		Sfld("title", Str()),
		Sfld("duration", Dbl()),
		Sfld("year", Int())
	])
	
	# read song data file
	df = spark.read.json(song_data, schema=songs_model)

	# extract columns to create songs table
	songs_table = df.select(["title", "artist_id", "year", "duration"]).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
	
	# write songs table to parquet files partitioned by year and artist
	songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs_table/', mode= 'overwrite')

	# extract columns to create artists table
	artists_fields = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
	artists_table = df.select(artists_fields).withColumnRenamed('artist_name', 'name').withColumnRenamed('artist_location', 'location').withColumnRenamed('artist_latitude', 'latitude').withColumnRenamed('artist_longitude', 'longitude').dropDuplicates()

	# write artists table to parquet files
	artists_table.write.parquet(output_data+'artists_table/', mode = 'overwrite')


def process_log_data(spark, input_data, output_data):
	# get filepath to log data file
	log_data = input_data+'log-data/*.json'

	# read log data file
	df = spark.read.json(log_data)
	
	# filter by actions for song plays
	df = df.filter(df["page"] == "NextSong")
	print("printing log schema")
	df.printSchema()

	# extract columns for users table    
	users_fields = ["userdId", "firstName", "lastName", "gender", "level"]
	users_table = df.select(users_fields).withColumnRenamed('userId', 'user_id').withColumnRenamed('firstName', 'first_name').withColumnRenamed('lastName', 'last_name').withColumnRenamed('gender', 'gender').withColumnRenamed('level', 'level').dropDuplicates()

	# write users table to parquet files
	users_table.write.parquet(output_data + 'users_table/')

	# create timestamp column from original timestamp column
	get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
	df = df.withColumn("start_time", get_timestamp(col("ts")))
	
	# create datetime column from original timestamp column
#     get_datetime = udf(lambda x: datetime.fromtimestamp(int(x)/1000), StringType())
#     df = df.withColumn("datatime", get_datetime(col("datetime")))
	
	# extract columns to create time table
	time_table = df.withColumn("hour", hour("start_time")).withColumn("day", dayofmonth("start_time")).withColumn("week", weekofyear("start_time")).withColumn("month", month("start_time")).withColumn("year", year("start_time")).withColumn("weekday", dayofweek("start_time")).select("start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
	
	# write time table to parquet files partitioned by year and month
	time_table.write.partitionBy(["year","month"]).paraquet(output_data+'time_table/')

	# read in song data to use for songplays table
	song_df = spark.read.parquet(output_data+'songs_table/*/*/*/*.json')
	
	artists_df = spark.read.parquet(output_data+'artists_table/*')

	# extract columns from joined song and log datasets to create songplays table 
	print("joining songs table and logs table")
	songs_logs_table_df = df.join(song_df , (df.song == song_df.title))
	print("Resultant Schema")
	songs_logs_table_df.printSchema()
	print("finished joining songs and logs table")
	
	print("joining artists_table and with previous songs_log table")
	artists_songs_log_table_df = songs_logs_table_df.join(artists_df, (songs_logs_table_df.artist == artists_df.name))
	artists_songs_log_table_df.withColumn("songplay_id", monotonically_increasing_id())
	print("Resultant Schema")
	artists_songs_log_table_df.printSchema()
	print("finished joining artists table")
		
	songplays_table = artists_songs_time_log_table.select(["songplay_id", "start_time", "user_id", "level", "song_id", "artist_id", "sessionId", "location", "userAgent"]).repartitionBy("year", "month")
	
	

	# write songplays table to parquet files partitioned by year and month
	songplays_table.write.partitionBy("year", "month").parquet(output_data+'songplay_table/')


def main():
	spark = create_spark_session()
	input_data = "s3a://udacity-dend/song-data/"
	output_data = "s3a://udacity-sparkify-project/output/"
	
	process_song_data(spark, input_data, output_data)    
	process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
	main()
