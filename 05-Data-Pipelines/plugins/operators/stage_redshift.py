from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html
# COPY SYNTAX:
# COPY table_name [column - list]
# FROM data_source
# authorization
# [[FORMAT][AS] data_format]
# [parameter[argument][, ...]]


class StageToRedshiftOperator(BaseOperator):
	ui_color = '#358140'
	# template_fields = 's3_key'
	copy_sql = """
		COPY {}
		FROM '{}'
		ACCESS_KEY_ID '{}'
		SECRET_ACCESS_KEY '{}'
		JSON '{}'
		REGION 'us-west-2'
		;
	"""

	@apply_defaults
	def __init__(
			self,
			# Define your operators params (with defaults) here
			# Example:
			#  redshift_conn_id=your-connection-name
			aws_cred_conn,
			redshift_conn,
			table,
			s3_bucket,
			s3_key,
			json_path,
			*args, **kwargs
	):

		super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
		# Map params here
		# Example:
		#  self.conn_id = conn_id
		self.aws_cred_conn = aws_cred_conn
		self.redshift_conn = redshift_conn
		self.table = table
		self.s3_bucket = s3_bucket
		self.s3_key = s3_key
		self.json_path = json_path

	def execute(self, context):
		self.log.info('Begin to implement StageToRedshiftOperator.')
		self.log.info('Setting up aws connection.')
		aws_hook = AwsHook(self.aws_cred_conn)  # return aws_hook class
		credentials = aws_hook.get_credentials()  # return access key and secret key under that id

		self.log.info('Setting up redshift connection.')
		redshift = PostgresHook(postgres_conn_id=self.redshift_conn)  # return a class

		self.log.info('Clearing data from staging redshift table.')
		# redshift.run() is inherited from `DbApiHook`
		redshift.run('DELETE FROM {}'.format(self.table))

		self.log.info('Copying data from s3 to redshift.')
		# rendered_key = self.s3_key.format(**context)  # dynamically refer to different s3 key(table) from context
		rendered_key = self.s3_key
		s3_path = 's3://{}/{}'.format(self.s3_bucket, rendered_key)
		if self.json_path != 'auto':
			s3_jsonpath = 's3://{}/{}'.format(self.s3_bucket, self.json_path)
		else:
			s3_jsonpath = self.json_path
		formatted_sql = StageToRedshiftOperator.copy_sql.format(
			self.table,
			s3_path,
			credentials.access_key,
			credentials.secret_key,
			s3_jsonpath
		)
		redshift.run(formatted_sql)
