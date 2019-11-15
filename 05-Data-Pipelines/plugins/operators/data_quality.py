from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
	ui_color = '#89DA59'

	@apply_defaults
	def __init__(
		self,
		# Define your operators params (with defaults) here
		# Example:
		# conn_id = your-connection-name
		redshift_conn,
		table_list,
		*args,
		**kwargs
	):

		super(DataQualityOperator, self).__init__(*args, **kwargs)
		# Map params here
		#  Example:
		#  self.conn_id = conn_id
		self.redshift_conn = redshift_conn
		self.table_list = table_list

	def execute(self, context):
		self.log.info('Begin to implement DataQualityOperator.')
		redshift = PostgresHook(postgres_conn_id=self.redshift_conn)
		# redshift.get_records() is inherited from `DbApiHook`

		for table in self.table_list:
			records = redshift.get_records(f"""
				SELECT COUNT(*) FROM {table};
			""")
			if len(records) < 1 or len(records[0]) < 1:
				raise ValueError(f"""
					Data quality check failed. {table} returned no result.
					""")
			num_records = records[0][0]
			if num_records < 1:
				raise ValueError(f"""
					Data quality check failed. {table} contained 0 rows.
				""")
			self.log.info(f"""
				Data quality check on {table} passed with {records[0][0]} records.
			""")
