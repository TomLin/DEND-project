import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
	"""
	Load json dataset from s3 to staging redshift tables.

	:param cur: Cursory to connected db, used to execute SQL query.
	:param conn: Connection to db.
	"""

	for query in copy_table_queries:
		print('Execute query:\n{}'.format(query))
		try:
			cur.execute(query)
			conn.commit()
		except BaseException as e:
			print('Error on query:\n{}'.format(query))
			print(e)

	print('Load staging tables successfully.')


def insert_tables(cur, conn):
	"""
	Insert query result from select clause to analytical tables (star schema).
	Fact table --> songplay
	Dim table --> user, song, artist, time

	:param cur: Cursory to connected db, used to execute SQL query.
	:param conn: Connection to db.
	"""
	for query in insert_table_queries:
		print('Execute query:\n{}'.format(query))
		try:
			cur.execute(query)
			conn.commit()
		except BaseException as e:
			print('Error on query:\n{}'.format(query))
			print(e)

	print('Insert query result to analytical tables successfully.')


def main():
	"""

	:param
		host: Redshift cluster address -- ENDPOINT.
		dbname: DB name.
		user: DB user.
		password: User password.
		port: Port used to connect to db.
	"""
	config = configparser.ConfigParser()
	config.read('dwh.cfg')

	conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
	cur = conn.cursor()

	load_staging_tables(cur, conn)
	insert_tables(cur, conn)

	conn.close()


if __name__ == "__main__":
	main()
