from psycopg2 import connect
import os
from src import utils

config = os.path.join(os.getcwd(), '../config/config.yaml')
# Read parameters from yaml config
params = utils.read_yaml(config)

# Establish psql connection
psql_creds = {
    'dbname': params['psql_cfg']['db_name'],
    'user': params['psql_cfg']['db_user'],
    'password': params['psql_cfg']['db_password'],
    'host': params['psql_cfg']['db_host'],
    'port': params['psql_cfg']['db_port'],
    'sslmode': 'require',
}
psql_conn = connect(**psql_creds)
table_name = params['psql_cfg']['table_name']

cursor = psql_conn.cursor()

cursor.execute('DROP TABLE IF EXISTS test_t')
cursor.execute('CREATE TABLE IF NOT EXISTS test_t (resp_time int, error_code integer, pattern char(100), pattern_ok bool)')
cursor.execute("INSERT INTO test_t (resp_time, error_code, pattern, pattern_ok) VALUES (10, 0, 'xxx', False)")

cursor.execute(f"SELECT * FROM test_t")
print(cursor.fetchall())

cursor.close()
psql_conn.close()
