import os
import pytest
from src.kafka_consumer import Consumer
from src.kafka_producer import Producer
import utils
from unittest.mock import patch
import requests
from src.db_writer import query_exec

# Configs pathes
config = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/config.yaml')
db_schema = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/db_schema.yaml')
# Read configs
schema = utils.read_yaml(db_schema)
params = utils.read_yaml(config)


@pytest.fixture(scope='function')
def producer():
    # Code that will run before your test, for example:
    prod = Producer(params)
    # A test function will be run at this point
    yield prod
    del prod


@pytest.fixture(scope='function')
def consumer():
    # Code that will run before your test, for example:
    cons = Consumer(params, schema)
    # A test function will be run at this point
    yield cons
    del cons


def test_message_received_from_site(producer, consumer):
    msg = producer.get_data_from_source()
    assert consumer.validate_schema(msg)


def test_message_received_from_site_http_error(producer, consumer):
        with pytest.raises(Exception) as excinfo:
            create_key('localhost:8080', 'spam', 'eggs')
        assert excinfo.value.message == 'mocked error'