from psycopg2 import connect, extensions


def query_exec(query, conn):
    with conn.cursor() as cursor:
        # TODO try cath
        print(f'Executing query: {query}')
        cursor.execute(query)
        conn.commit()


class DrWriter:
    def __init__(self, global_config, db_schema):
        self.global_config = global_config
        self.db_schema = db_schema

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
        table = self.db_schema['table_name']
        columns = [str(k) for k in msg.keys()]
        values = []
        for c in columns:
            if isinstance(msg[c], str):
                values.append(f"'{msg[c]}'")
            else:
                values.append(str(msg[c]))
        insert_str = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join(values)})"
        return insert_str
