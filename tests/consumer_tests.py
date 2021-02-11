import os
import pytest
from src.kafka_consumer import Consumer
import utils

# Configs pathes
config = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/config.yaml')
db_schema = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/db_schema.yaml')
# Read configs
schema = utils.read_yaml(db_schema)
params = utils.read_yaml(config)
#cons = Consumer(params, schema)


@pytest.fixture(scope='function')
def consumer():
    # Code that will run before your test, for example:
    cons = Consumer(params, schema)
    # A test function will be run at this point
    yield
    del cons


def test_schema_validation_correct_msg_format(consumer):
    # correct message format
    msg = {
        'url': 'http://www.site.com',
        'pattern': 'somepattern',
        'pattern_matched': 0,
        'response_time': 0.119738,
        'response_code': 200,
    }
    assert consumer.validate_schema(msg)


def test_schema_validation_incorrect_msg_field_type(consumer):
    # pattern_matched has incorrect value type
    msg = {
        'url': 'http://www.site.com',
        'pattern': 'somepattern',
        'pattern_matched': 'text',
        'response_time': 0.119738,
        'response_code': 200,
    }
    assert not consumer.validate_schema(msg)


def test_schema_validation_incorrect_msg_field_name(consumer):
    # incorrect name -> new_pattern
    msg = {
        'url': 'http://www.site.com',
        'new_pattern': 'somepattern',
        'pattern_matched': 0,
        'response_time': 0.119738,
        'response_code': 200,
    }
    assert not consumer.validate_schema(msg)

