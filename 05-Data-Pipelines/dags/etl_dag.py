from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries

# `depends_on_past` (boolean) when set to True,
# keeps a task from getting triggered
# if the previous schedule for the task hasn’t succeeded.

# `catchup`, to disable back-filling for a DAG.

default_args = {
	'owner': 'AJ',
	'start_date': datetime.utcnow(),
	'email_on_retry': False,
	'retries': 3,
	'retry_delay': timedelta(minutes=5),
	'catchup': False,
	'depends_on_past': False
}

dag = DAG(
	dag_id='my_own_etl_dag',
	description='Loading and data transformation on redshift via airflow.',
	default_args=default_args,
	schedule_interval='@hourly'
)

start_operator = DummyOperator(
	task_id='start_execution',
	dag=dag
)

load_stage_events_to_redshift = StageToRedshiftOperator(
	task_id='load_stage_events',
	dag=dag,
	aws_cred_conn='aws_credentials',
	redshift_conn='redshift',
	table='staging_events',
	s3_bucket='udacity-dend',
	s3_key='log_data/',
	json_path='log_json_path.json'
)

load_stage_songs_to_redshift = StageToRedshiftOperator(
	task_id='load_stage_songs',
	dag=dag,
	aws_cred_conn='aws_credentials',
	redshift_conn='redshift',
	table='staging_songs',
	s3_bucket='udacity-dend',
	s3_key='song_data/',
	json_path='auto'
)

load_songplay_fact = LoadFactOperator(
	task_id='load_songplay_fact',
	dag=dag,
	redshift_conn='redshift',
	table='songplays',
	sql_command_to_run=SqlQueries.songplay_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
	task_id='load_song_dim',
	dag=dag,
	redshift_conn='redshift',
	table='songs',
	append=False,
	sql_command_to_run=SqlQueries.song_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
	task_id='load_user_dim',
	dag=dag,
	redshift_conn='redshift',
	table='users',
	append=False,
	sql_command_to_run=SqlQueries.user_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
	task_id='load_artist_dim',
	dag=dag,
	redshift_conn='redshift',
	table='artists',
	append=False,
	sql_command_to_run=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
	task_id='load_time_dim',
	dag=dag,
	redshift_conn='redshift',
	table='time',
	append=False,
	sql_command_to_run=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
	task_id='run_data_quality_checks',
	dag=dag,
	redshift_conn='redshift',
	table_list=['songs', 'users', 'artists', 'time']
)

end_operator = DummyOperator(
	task_id='end_execution',
	dag=dag
)

start_operator >> load_stage_events_to_redshift
start_operator >> load_stage_songs_to_redshift
load_stage_events_to_redshift >> load_songplay_fact
load_stage_songs_to_redshift >> load_songplay_fact
load_songplay_fact >> load_song_dimension_table
load_songplay_fact >> load_user_dimension_table
load_songplay_fact >> load_artist_dimension_table
load_songplay_fact >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
