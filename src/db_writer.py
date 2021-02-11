from psycopg2 import connect, DatabaseError, OperationalError
import logging
import sys

logging.getLogger().setLevel(logging.INFO)


def query_exec(query: str, conn) -> bool:
    query_ok = False
    with conn.cursor() as cursor:
        logging.info(f'Executing query: {query}')
        try:
            cursor.execute(query)
            conn.commit()
            query_ok = True
        except OperationalError as e:
            logging.exception(f'Query {query} cannot be executed!')
            conn.rollback()
    return query_ok


class DrWriter:
    def __init__(self, db_config, table_name):
        self.db_name = db_config['db_name']
        self.db_user = db_config['db_user']
        self.db_password = db_config['db_password']
        self.db_host = db_config['db_host']
        self.db_port = db_config['db_port']
        self.ssl_mode = db_config['ssl_mode']
        self.table = table_name

    def connect_to_db(self) -> None:
        psql_creds = {
            'dbname': self.db_name,
            'user': self.db_user,
            'password': self.db_password,
            'host': self.db_host,
            'port': self.db_port,
            'sslmode': self.ssl_mode,
        }
        try:
            psql_connection = connect(**psql_creds)
            return psql_connection
        except DatabaseError:
            logging.exception(f"Connection to DB can't be established. Stopping consumer...")
            sys.exit(1)


    def convert_raw_data_to_queries(self, msg: dict) -> str:
        """
        Prepare INSERT query from the dict
        :param msg: dict
        :return: insert string
        """
        columns = [str(k) for k in msg.keys()]
        values = []
        for c in columns:
            if isinstance(msg[c], str):
                values.append(f"'{msg[c]}'")
            else:
                values.append(str(msg[c]))
        insert_str = f"INSERT INTO {self.table} ({','.join(columns)}) VALUES ({','.join(values)})"
        return insert_str
