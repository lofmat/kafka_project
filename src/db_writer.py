from psycopg2 import connect


def query_exec(row, conn):
    cursor = conn.cursor()
    # TODO try cath
    cursor.execute(row)
    print(cursor.fetchall())
    cursor.close()


class DrWriter:
    def __init__(self, global_config):
        self.global_config = global_config

    def connect_to_db(self):
        psql_creds = {
            'dbname': self.global_config['psql_cfg']['db_name'],
            'user': self.global_config['psql_cfg']['db_user'],
            'password': self.global_config['psql_cfg']['db_password'],
            'host': self.global_config['psql_cfg']['db_host'],
            'port': self.global_config['psql_cfg']['db_port'],
            'sslmode': 'require',
        }
        # TODO try catch
        psql_connection = connect(**psql_creds)
        return psql_connection

    def convert_raw_data_to_queries(self, msg):
        table = self.global_config['psql_cfg']['table_name']
        columns = [k for k in msg.keys()]
        values = [msg[c] for c in columns]
        insert_str = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join(values)})"
        print('--->', insert_str)
        return insert_str
