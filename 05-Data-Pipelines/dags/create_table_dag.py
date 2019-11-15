from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
import table_queries

# `depends_on_past` (boolean) when set to True,
# keeps a task from getting triggered
# if the previous schedule for the task hasn’t succeeded.

# `catchup`, to disable back-filling for a DAG.

# The architecture of whole script is referred from https://github.com/FedericoSerini/DEND-Project-5-Data-Pipelines

default_args = {
	'owner':'AJ',
	'start_date': datetime.utcnow(),
	'email_on_retry': False,
	'retries': 3,
	'retry_delay': timedelta(minutes=5),
	'catchup': False,
	'depends_on_past': False
}

dag = DAG(
	dag_id='drop_create_table_dag',
	description='Drop and create tables that will be loaded in redshift.',
	default_args=default_args,
	schedule_interval=None
)


# `DummyOperator` Operator that does literally nothing. It can be used to group tasks in a DAG.
start_operator = DummyOperator(
	task_id='start_execution',
	dag=dag
)

# `redshift` connection has been defined in airflow connection section already.
drop_table_staging_events = PostgresOperator(
	task_id='drop_staging_events',
	postgres_conn_id='redshift',
	sql=table_queries.DROP_TABLE_STAGING_EVENTS,
	dag=dag
)

drop_table_staging_songs = PostgresOperator(
	task_id='drop_staging_songs',
	postgres_conn_id='redshift',
	sql=table_queries.DROP_TABLE_STAGING_SONGS,
	dag=dag
)

drop_table_songplay = PostgresOperator(
	task_id='drop_songplay',
	postgres_conn_id='redshift',
	sql=table_queries.DROP_TABLE_SONGPLAYS,
	dag=dag
)

drop_table_user = PostgresOperator(
	task_id='drop_user',
	postgres_conn_id='redshift',
	sql=table_queries.DROP_TABLE_USERS,
	dag=dag
)

drop_table_song = PostgresOperator(
	task_id='drop_song',
	postgres_conn_id='redshift',
	sql=table_queries.DROP_TABLE_SONGS,
	dag=dag
)

drop_table_artist = PostgresOperator(
	task_id='drop_artist',
	postgres_conn_id='redshift',
	sql=table_queries.DROP_TABLE_ARTISTS,
	dag=dag
)

drop_table_time = PostgresOperator(
	task_id='drop_time',
	postgres_conn_id='redshift',
	sql=table_queries.DROP_TABLE_TIME,
	dag=dag
)

create_table_staging_events = PostgresOperator(
	task_id='create_staging_events',
	postgres_conn_id='redshift',
	sql=table_queries.CREATE_TABLE_STAGING_EVENTS,
	dag=dag
)

create_table_staging_songs = PostgresOperator(
	task_id='create_staging_songs',
	postgres_conn_id='redshift',
	sql=table_queries.CREATE_TABLE_STAGING_SONGS,
	dag=dag
)

create_table_songplay = PostgresOperator(
	task_id='create_songplay',
	postgres_conn_id='redshift',
	sql=table_queries.CREATE_TABLE_SONGPLAYS,
	dag=dag
)

create_table_user = PostgresOperator(
	task_id='create_user',
	postgres_conn_id='redshift',
	sql=table_queries.CREATE_TABLE_USERS,
	dag=dag
)

create_table_song = PostgresOperator(
	task_id='create_song',
	postgres_conn_id='redshift',
	sql=table_queries.CREATE_TABLE_SONGS,
	dag=dag
)

create_table_artist = PostgresOperator(
	task_id='create_artist',
	postgres_conn_id='redshift',
	sql=table_queries.CREATE_TABLE_ARTISTS,
	dag=dag
)

create_table_time = PostgresOperator(
	task_id='create_time',
	postgres_conn_id='redshift',
	sql=table_queries.CREATE_TABLE_TIME,
	dag=dag
)

end_operator = DummyOperator(
	task_id='end_execution',
	dag=dag
)

start_operator >> drop_table_staging_events
drop_table_staging_events >> create_table_staging_events
create_table_staging_events >> end_operator

start_operator >> drop_table_staging_songs
drop_table_staging_songs >> create_table_staging_songs
create_table_staging_songs >> end_operator

start_operator >> drop_table_songplay
drop_table_songplay >> create_table_songplay
create_table_songplay >> end_operator

start_operator >> drop_table_user
drop_table_user >> create_table_user
create_table_user >> end_operator

start_operator >> drop_table_song
drop_table_song >> create_table_song
create_table_song >> end_operator

start_operator >> drop_table_artist
drop_table_artist >> create_table_artist
create_table_artist >> end_operator

start_operator >> drop_table_time
drop_table_time >> create_table_time
create_table_time >> end_operator
