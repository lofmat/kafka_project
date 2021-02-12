import pytest

import json
from src.kafka_consumer import Consumer
from src.kafka_producer import Producer
from src.db_writer import DrWriter, query_exec
from src.utils import get_yaml_configs
from src.setup_db import create_table

schema, params = get_yaml_configs()
test_table = 'T2'
schema['table_name'] = test_table


@pytest.fixture(scope='function')
def consumer():
    schema['table_name'] = test_table
    cons = Consumer(params, schema)
    yield cons
    del cons


@pytest.fixture(scope='function')
def producer():
    prod = Producer(params)
    yield prod
    del prod


@pytest.fixture(scope='function')
def connection_to_db():
    dbw = DrWriter(params['psql_cfg'], test_table)
    db_conn = dbw.connect_to_db()
    create_table(schema)
    yield db_conn
    query_exec(f'DROP TABLE {test_table};', db_conn)
    db_conn.close()


def test_send_to_kafka_read_from_and_store(consumer, producer, connection_to_db):
    msg = {
        'url': 'http://www.site.com',
        'pattern': 'somepattern',
        'pattern_matched': 1,
        'response_time': 0.119738,
        'response_code': 200,
    }
    producer.send_msg_to_kafka(msg)
    data = consumer.get_data_from_kafka()
    is_ok = False
    for m in data:
        msg = json.loads(m.value.decode("utf-8"))
        msg['url'] = m.key.decode("utf-8")
        is_ok = consumer.store_data_to_db(msg)
        break
    assert is_ok[0]



