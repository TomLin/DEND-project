import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
	"""
	Drop existing tables.

	:param cur: Cursory to connected db, used to execute SQL query.
	:param conn: Connection to db.
	"""
	for query in drop_table_queries:
		print('Execute query:\n{}'.format(query))
		try:
			cur.execute(query)
			conn.commit()
		except BaseException as e:
			print('Error on query:\n {}'.format(query))
			print(e)

	print('Drop tables successfully.')


def create_tables(cur, conn):
	"""
	Create table schema.

	:param cur: Cursory to connected db, used to execute SQL query.
	:param conn: Connection to db.
	"""
	for query in create_table_queries:
		print('Execute query:\n{}'.format(query))
		try:
			cur.execute(query)
			conn.commit()
		except BaseException as e:
			print('Error on query:\n{}'.format(query))
			print(e)

	print('Create table successfully.')


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

	drop_tables(cur, conn)
	create_tables(cur, conn)

	conn.close()


if __name__ == "__main__":
	main()
