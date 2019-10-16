import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = """
	DROP TABLE IF EXISTS staging_events_table;
"""

staging_songs_table_drop = """
	DROP TABLE IF EXISTS staging_songs_table;
"""

songplay_table_drop = """
	DROP TABLE IF EXISTS songplay_table;
"""

user_table_drop = """
	DROP TABLE IF EXISTS user_table;
"""

song_table_drop = """
	DROP TABLE IF EXISTS song_table;
"""

artist_table_drop = """
	DROP TABLE IF EXISTS artist_table;
"""

time_table_drop = """
	DROP TABLE IF EXISTS time_table;
"""

# CREATE TABLES
# Footnote:
# `status` -> should be a number, it's http status
# `length` -> time played
# unix_timestamp(aks epoch) is defaulted second unix (10 digits)
# alternative version is in millisecond (13 digits)
# --> https://www.freeformatter.com/epoch-timestamp-to-date-converter.html
staging_events_table_create= ("""
	CREATE TABLE IF NOT EXISTS staging_events_table (
		eventId			integer			IDENTITY(0,1),
		artist			varchar(500),
		auth			varchar(500),
		firstName		varchar(500),
		gender			varchar(500),
		itemInSession	integer,
		lastName		varchar(500),
		length			double precision,
		level			varchar(500),
		location		varchar(500), 
		method			varchar(500),
		page			varchar(500),
		registration	double precision,
		sessionId		integer,
		song			varchar(500),
		status			integer,
		ts				bigint,
		userAgent		varchar(500),
		userId			integer
	);
""")

staging_songs_table_create = ("""
	CREATE TABLE IF NOT EXISTS staging_songs_table (
		num_songs			integer,
		artist_id			varchar(500),
		artist_latitude		double precision,
		artist_longitude	double precision,
		artist_location		varchar(500),
		artist_name			varchar(500),
		song_id				varchar(500),
		title				varchar(500),
		duration			double precision,
		year				integer
	);
""")

songplay_table_create = ("""
	CREATE TABLE IF NOT EXISTS songplay_table (
		songplay_id		bigint 			IDENTITY(0,1) PRIMARY KEY,
		start_time		timestamp		REFERENCES time_table(start_time),
		user_id			integer			REFERENCES user_table(user_id),
		level			varchar(500),
		song_id			varchar(500)	REFERENCES song_table(song_id),
		artist_id		varchar(500)	REFERENCES artist_table(artist_id),
		session_id		integer			NOT NULL,
		location		varchar(500),
		user_agent		varchar(500)
	)
	SORTKEY (start_time, session_id)
	;
""")

user_table_create = ("""
	CREATE TABLE IF NOT EXISTS user_table (
		user_id			integer			PRIMARY KEY,
		first_name		varchar(500),
		last_name		varchar(500),
		gender			varchar(500),
		level			varchar(500)
	);
""")

song_table_create = ("""
	CREATE TABLE IF NOT EXISTS song_table (
		song_id			varchar(500)		PRIMARY KEY,
		title			varchar(500),
		artist_id 		varchar(500),
		year			integer,
		duration		double precision
	)
	DISTKEY (artist_id)
	SORTKEY (year)
	;
""")

artist_table_create = ("""
	CREATE TABLE IF NOT EXISTS artist_table (
		artist_id		varchar(500)		PRIMARY KEY,
		name			varchar(500),
		location		varchar(500),
		latitude		double precision,
		longitude		double precision
	);
""")

# Redshift data type - time.
# http://dwgeek.com/amazon-redshift-date-format-conversion-examples.html
# https://www.fernandomc.com/posts/redshift-epochs-and-timestamps/
time_table_create = ("""
	CREATE TABLE IF NOT EXISTS time_table (
		start_time		timestamp		PRIMARY KEY,
		hour			integer,
		day				integer,
		week			integer,
		month			integer,
		year			integer,
		weekday			integer
	)
	SORTKEY (start_time)
	;
""")

# STAGING TABLES

# When using `jasonpaths`, it's best to provide column list of target table in COPY command,
# and the order in `jasonpaths` must map the order of that column list.

# https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html
# Each JSONPath expression in the jsonpaths array corresponds to one column
# in the Amazon Redshift target table. The order of the jsonpaths array elements
# must match the order of the columns in the target table or the column list,
# if a column list is used.
# --> https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-column-mapping.html#copy-column-list
# setting COMPUPDATE, STATUPDATE to speed up COPY
staging_events_copy = ("""
	COPY staging_events_table
	FROM '{}'
	credentials 'aws_iam_role={}'
	region 'us-west-2'
	json '{}'
	COMPUPDATE OFF STATUPDATE OFF
	;
""").format(
	config.get('S3', 'LOG_DATA'),
	config.get('IAM_ROLE', 'ARN'),
	config.get('S3', 'LOG_JSONPATH')
)

# When using `auto` for json loading, it directly maps the column name of target table
# to the key name of source json, the order of keys from source json doesn't matter.

# https://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html#r_COPY_command_examples-copy-from-json
# To load from JSON data using the 'auto' argument,
# the JSON data must consist of a set of objects.
# The key names must match the column names,
# but in this case, order does not matter.
staging_songs_copy = ("""
	COPY staging_songs_table
	FROM '{}'
	credentials 'aws_iam_role={}'
	region 'us-west-2'
	json 'auto'
	COMPUPDATE OFF STATUPDATE OFF
	TRUNCATECOLUMNS
	;
""").format(
	config.get('S3', 'SONG_DATA'),
	config.get('IAM_ROLE', 'ARN')
)

# FINAL TABLES

songplay_table_insert = ("""
	INSERT INTO songplay_table (
		start_time,
		user_id,
		level,
		song_id,
		artist_id,
		session_id,
		location,
		user_agent	
	)
	
	SELECT
		timestamp 'epoch' + (e.ts/1000 * interval '1 second') as start_time,
		u.user_id,
		e.level,
		s.song_id,
		a.artist_id,
		e.sessionId,
		e.location,
		e.userAgent
	FROM
		staging_events_table e 
	INNER JOIN user_table u ON e.userId = u.user_id
	INNER JOIN song_table s ON s.title = e.song
	INNER JOIN artist_table a ON a.artist_id = s.artist_id
		AND a.name = e.artist  
	WHERE e.page = 'NextSong'
	;
""")

user_table_insert = ("""
	INSERT INTO user_table (
		user_id,
		first_name,
		last_name,
		gender,
		level
	)
	
	SELECT 
		distinct
		userId,
		firstName,
		lastName,
		gender,
		level
	FROM
		staging_events_table
	WHERE
		page = 'NextSong'
	;
""")

song_table_insert = ("""
	INSERT INTO song_table (
		song_id,
		title,
		artist_id,
		year,
		duration
	)

	SELECT
		distinct
		song_id,
		title,
		artist_id,
		year,
		duration
	FROM
		staging_songs_table
	;
""")

artist_table_insert = ("""
	INSERT INTO artist_table (
		artist_id,
		name,
		location,
		latitude,
		longitude
	)
	
	SELECT
		distinct
		artist_id,
		artist_name,
		artist_location,
		artist_latitude,
		artist_longitude
	FROM
		staging_songs_table
	;
""")

time_table_insert = ("""
	INSERT INTO time_table (
		start_time,
		hour,
		day,
		week,
		month,
		year,
		weekday
	)

	SELECT
		distinct
		timestamp 'epoch' + (ts/1000 * interval '1 second') as start_time,
		extract(hour from start_time),
		extract(day from start_time),
		extract(week from start_time),
		extract(month from start_time),
		extract(year from start_time),
		extract(dayofweek from start_time)
	FROM
		staging_events_table
	WHERE
		page = 'NextSong'
	;	
""")

# QUERY LISTS
drop_table_queries = [
	staging_events_table_drop,
	staging_songs_table_drop,
	songplay_table_drop,
	user_table_drop,
	song_table_drop,
	artist_table_drop,
	time_table_drop
]

create_table_queries = [
	staging_events_table_create,
	staging_songs_table_create,
	user_table_create,
	song_table_create,
	artist_table_create,
	time_table_create,
	songplay_table_create
]

copy_table_queries = [
	staging_events_copy,
	staging_songs_copy
]

insert_table_queries = [
	user_table_insert,
	song_table_insert,
	artist_table_insert,
	time_table_insert,
	songplay_table_insert
]

example_query = """
select
	s.songplay_id
	,s.session_id
	,s.start_time
	,t.week
	,t.weekday
	,s.location
	,s.user_agent
	,u.first_name as user_name
	,u.gender as user_gender
	,u.level
	,g.title as song_title
	,a.name as artist_name
from
	songplay_table s
inner join time_table t
	on s.start_time = t.start_time
inner join user_table u 
	on s.user_id = u.user_id
inner join song_table g
	on s.song_id = g.song_id
	and s.artist_id = g.artist_id
inner join artist_table a
	on s.artist_id = a.artist_id
limit 
	10;
"""
