from db_writer import DrWriter, query_exec
import utils
import os
import logging
logging.getLogger().setLevel(logging.INFO)

# Configs
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/config.yaml')
cfg_params = utils.read_yaml(config_path)
schema_cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/db_schema.yaml')
schema = utils.read_yaml(schema_cfg_path)

d = DrWriter(cfg_params['psql_cfg'], schema['table_name'])


def build_sql_query_from_yaml_schema(table_name: str, db_schema: dict) -> str:
    """
    Build CREATE TABLE query
    :param table_name: DB table name
    :param db_schema: table field names and their types
    :return:
    """
    val = []
    for field_name, field_value in db_schema.items():
        if isinstance(field_value, int):
            val.append(f"{field_name} int")
        elif isinstance(field_value, float):
            val.append(f"{field_name} float")
        elif isinstance(field_value, str):
            val.append(f"{field_name} varchar(255)")
        else:
            logging.warning(f'Type for value {field_value} is not supported')

    return f"CREATE TABLE {table_name} ({','.join(val)});"


if schema.get('table_name') and schema.get('schema'):
    query = build_sql_query_from_yaml_schema(schema.get('table_name'), schema.get('schema'))

    with d.connect_to_db() as db_connection:
        r = query_exec(query, db_connection)
        if not r[0]:
            logging.error(f"Unable to create table {schema.get('table_name')}. Please check DB connection.")
else:
    logging.error(f"Could not find 'table_name' or 'schema' in config file {schema_cfg_path}")
