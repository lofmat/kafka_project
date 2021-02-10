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

d = DrWriter(cfg_params, schema)
val = []
# Build create table query
for k in schema['schema'].keys():
    if isinstance(schema['schema'][k], int):
        val.append(f"{k} int")
    elif isinstance(schema['schema'][k], float):
        val.append(f"{k} float")
    elif isinstance(schema['schema'][k], str):
        val.append(f"{k} varchar(255)")

with d.connect_to_db() as db_connection:
    q = f"CREATE TABLE {schema['table_name']} ({','.join(val)});"
    query_exec(q, db_connection)
