from src.setup_db import build_sql_query_from_yaml_schema
from utils import get_yaml_configs
from db_writer import DrWriter

schema, params = get_yaml_configs()

s = {'table_name': 'test_table',
     'schema': {
               'pattern': 'footer',
               'pattern_matched': 1,
               'response_time': 0.11978,
               'response_code': 200,
               'url': 'http://www.info.com'
           }
        }

expected_crate_table_q = f"CREATE TABLE {s['table_name']} (pattern varchar(255),pattern_matched int,response_time float," \
                         f"response_code int,url varchar(255));"
expected_insert = f"INSERT INTO {schema['table_name']} (pattern,pattern_matched,response_time,response_code,url) " \
                  f"VALUES ('footer',1,0.11978,200,'http://www.info.com')"


def test_convert_schema_to_create_table_query():
    query = build_sql_query_from_yaml_schema(s)
    assert query == expected_crate_table_q


def test_create_insert_query():
    dbw = DrWriter(params['psql_cfg'], schema['table_name'])
    insert_query = dbw.convert_raw_data_to_queries(s['schema'])
    assert insert_query == expected_insert