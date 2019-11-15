from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

	ui_color = '#F98866'
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
		sql_command_to_run,
		*args,
		**kwargs
	):

		super(LoadFactOperator, self).__init__(*args, **kwargs)
		# Map params here
		# Example:
		#  self.conn_id = conn_id
		self.redshift_conn = redshift_conn
		self.table = table
		self.sql_command_to_run = sql_command_to_run

	def execute(self, context):
		self.log.info('Begin to implement LoadFactOperator.')
		self.log.info('Setting up redshift connection.')
		redshift = PostgresHook(postgres_conn_id=self.redshift_conn)

		formatted_sql = LoadFactOperator.insert_sql.format(
			self.table,
			self.sql_command_to_run
		)
		self.log.info('Inserting data to fact table.')
		self.log.info(f'SQL command to run:\n{formatted_sql}')
		redshift.run(formatted_sql)
