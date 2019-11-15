import configparser
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['EMR']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['EMR']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
	"""
	When running spark script in command line mode, we need to
	initiate spark session on our own. While, in jupyter notebook,
	it's launched in the background automatically.

	:return: spark session obj
	"""
	spark = SparkSession \
		.builder \
		.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
		.getOrCreate()
	return spark


def process_song_data(spark, input_data, output_data):
	"""
	Read in song data from json (with wild-card used),
	then select cols, drop duplicated rows and
	save tables (song_table, artist_table) in partitioned parquet.

	:param spark: spark session obj
	:param input_data: (str) s3 bucket path of data source
	:param output_data: (str) s3 bucket path of where result stored
	:return: None

	"""
	# get filepath to song data file
	stage_song_pth = f"{input_data}/song_data/*/*/*/*.json"
	# read song data file
	df = spark.read.json(stage_song_pth)
	print("Success: Read data from S3.")

	# extract columns to create songs table
	songs_table = df.select(
		"song_id", "title", "artist_id",
		"year", "duration").dropDuplicates()

	# write songs table to parquet files partitioned by year and artist
	# https://spark.apache.org/docs/2.3.2/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter.parquet
	# https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery
	# https://spark.apache.org/docs/latest/api/R/write.parquet.html
	# In the doc above, it clearly states that PATH is a Directory, not the file path.
	songs_table.write.parquet(
		path=f"{output_data}/song_table",
		mode="overwrite",
		partitionBy=["year", "artist_id"]
	)

	# extract columns to create artists table
	artists_table = df.select(
		"artist_id", "artist_name", "artist_location",
		"artist_latitude", "artist_longitude").dropDuplicates()

	# write artists table to parquet files
	artists_table.write.parquet(
		path=f"{output_data}/artist_table",
		mode="overwrite"
	)


def process_log_data(spark, input_data, output_data):
	"""
	Read in log data from json (with wild-card used),
	then select cols, filter rows and drop duplicated rows.
	Pre-process timestamps and read in pre-processed dimension table
	(song_table, artist_table).
	Finally save tables (user_table, time_table, songplay_table) in partitioned parquet.

	:param spark: spark session obj
	:param input_data: (str) s3 bucket path of data source
	:param output_data: (str) s3 bucket path of where result stored
	:return: None
	"""
	# get filepath to log data file
	stage_log_data_pth = f"{input_data}/log_data/*.json"

	# read log data file
	df = spark.read.json(stage_log_data_pth)

	# filter by actions for song plays
	# only select actions of "NextSong"
	df = df.filter(df["page"] == "NextSong")

	# extract columns for users table
	users_table = df.select(
		"userId", "firstName", "lastName",
		"gender", "level").dropDuplicates()

	# write users table to parquet files
	users_table.write.parquet(
		path=f"{output_data}/user_table",
		mode="overwrite"
	)

	# create timestamp column from original timestamp column
	df = df.withColumn("start_time", F.from_unixtime(F.col("ts")/1000))

	# extract columns to create time table
	time_table = df.select("ts", "start_time") \
		.withColumn("hour", F.hour("start_time")) \
		.withColumn("day", F.dayofmonth("start_time")) \
		.withColumn("week", F.weekofyear("start_time")) \
		.withColumn("month", F.month("start_time")) \
		.withColumn("year", F.year("start_time")) \
		.withColumn("weekday", F.dayofweek("start_time")) \
		.dropDuplicates()

	# write time table to parquet files partitioned by year and month
	time_table.write.parquet(
		path=f"{output_data}/time_table",
		mode="overwrite",
		partitionBy=["year", "month"]
	)

	# read in song data to use for songplays table
	# https://stackoverflow.com/questions/55542708/spark-unable-to-read-parquet-file-from-partitioned-s3-bucket
	# NOTE: read in partitioned parquet file.
	# https://spark.apache.org/docs/latest/api/R/read.parquet
	song_table_pth = f"{output_data}/song_table/*/*/*"
	song_table = spark.read.parquet(song_table_pth)

	# read in artist_table
	artist_table_pth = f"{output_data}/artist_table/*"
	artists_table = spark.read.parquet(artist_table_pth)

	# create temp view
	song_table.createOrReplaceTempView("song_table")
	time_table.createOrReplaceTempView("time_table")
	users_table.createOrReplaceTempView("user_table")
	artists_table.createOrReplaceTempView("artist_table")
	df.createOrReplaceTempView("stage_events_table")

	# extract columns from joined song and log data sets to create songplays table
	songplays_table = spark.sql("""
		select distinct
			t.start_time,
			t.year,
			t.month,
			u.userId,
			e.level,
			s.song_id,
			a.artist_id,
			e.sessionId,
			e.userAgent
		from
			stage_events_table as e
		inner join time_table t on t.ts = e.ts 
		inner join user_table u on e.userId = u.userId
		inner join song_table s on s.title = e.song
		inner join artist_table a on a.artist_name = e.artist
	""")

	# write songplays table to parquet files partitioned by year and month
	songplays_table.write.parquet(
		path=f"{output_data}/songplay_table",
		mode="overwrite",
		partitionBy=["year", "month"]
	)


def main():
	spark = create_spark_session()
	input_data = "s3a://tom-dend-bucket-1"  # s3 bucket of data source
	output_data = "s3a://tom-dend-bucket-2"  # s3 bucket of data destination

	process_song_data(spark, input_data, output_data)
	process_log_data(spark, input_data, output_data)
	spark.stop()


if __name__ == "__main__":
	main()
