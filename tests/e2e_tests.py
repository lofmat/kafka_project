import os
import pytest
from src.kafka_consumer import Consumer
from src.kafka_producer import Producer
import utils
from src.db_writer import query_exec

# Configs pathes
config = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/config.yaml')
db_schema = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/db_schema.yaml')
# Read configs
schema = utils.read_yaml(db_schema)
params = utils.read_yaml(config)


@pytest.fixture(scope='function')
def consumer():
    # Code that will run before your test, for example:
    cons = Consumer(params, schema)
    # A test function will be run at this point
    yield cons
    del cons


@pytest.fixture(scope='function')
def producer():
    # Code that will run before your test, for example:
    prod = Producer(params)
    # A test function will be run at this point
    yield prod
    del prod


def test_send_to_kafka_read_from_and_store(consumer, producer):
    msg = {
        'url': 'http://www.site.com',
        'pattern': 'somepattern',
        'pattern_matched': 1,
        'response_time': 0.119738,
        'response_code': 200,
    }

    producer.send_msg_to_kafka(msg)
    consumer.get_and_store_data_to_db()
    res = query_exec(f"SELECT * FROM {schema['table_name']};")
    skipped_value = False
    for k in msg.keys():
        if msg[k] not in res[1]:
            skipped_value = True

    assert not skipped_value

