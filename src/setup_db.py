from db_writer import DrWriter, query_exec
import logging
from utils import get_yaml_configs

logging.getLogger().setLevel(logging.INFO)

# Configs
schema, params = get_yaml_configs()


d = DrWriter(params['psql_cfg'], schema['table_name'])


def build_sql_query_from_yaml_schema(db_schema: dict) -> str:
    """
    Build CREATE TABLE query
    :param table_name: DB table name
    :param db_schema: table field names and their types
    :return:
    """
    val = []
    table_name = db_schema['table_name']
    for field_name, field_value in db_schema['schema'].items():
        if isinstance(field_value, int):
            val.append(f"{field_name} int")
        elif isinstance(field_value, float):
            val.append(f"{field_name} float")
        elif isinstance(field_value, str):
            val.append(f"{field_name} varchar(255)")
        else:
            logging.warning(f'Type for value {field_value} is not supported')

    return f"CREATE TABLE {table_name} ({','.join(val)});"


def create_table(tb_schema):
    if tb_schema.get('table_name') and tb_schema.get('schema'):
        query = build_sql_query_from_yaml_schema(tb_schema)
        logging.info(f"Prepared query: {query}")
        with d.connect_to_db() as db_connection:
            r = query_exec(query, db_connection)
            if not r[0]:
                logging.error(f"Unable to create table {tb_schema.get('table_name')}. Please check DB connection.")
    else:
        logging.error(f"Could not find 'table_name' or 'schema' in config file ../config/db_schema.yaml")


if __name__ == '__main__':
    create_table(schema)
