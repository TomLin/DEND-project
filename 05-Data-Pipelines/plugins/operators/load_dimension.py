from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class LoadDimensionOperator(BaseOperator):
	ui_color = '#80BD9E'
	insert_sql = """
		INSERT INTO {}
		{}
		;
	"""

	@apply_defaults
	def __init__(
		self,
		# Define your operators params (with defaults) here
		# Example:
		# conn_id = your-connection-name
		redshift_conn,
		table,
		append,
		sql_command_to_run,
		*args,
		**kwargs
	):
		super(LoadDimensionOperator, self).__init__(*args, **kwargs)
		# Map params here
		# Example:
		# self.conn_id = conn_id
		self.redshift_conn = redshift_conn
		self.table = table
		self.append = append
		self.sql_command_to_run = sql_command_to_run

	def execute(self, context):
		self.log.info('Begin to implement LoadDimensionOperator.')
		self.log.info('Setting up redshift connection.')
		redshift = PostgresHook(postgres_conn_id=self.redshift_conn)  # return a class

		if not self.append:
			self.log.info('Delete data from dimension tables.')
			# redshift.run() is inherited from `DbApiHook`
			redshift.run(f"""
				DELETE FROM {self.table};
			""")

		formatted_sql = LoadDimensionOperator.insert_sql.format(
			self.table,
			self.sql_command_to_run
		)
		self.log.info('Inserting data to dimension tables.')
		self.log.info(f'SQL command to run:\n{formatted_sql}')
		redshift.run(formatted_sql)
