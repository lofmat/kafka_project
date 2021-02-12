import pytest

from src.kafka_consumer import Consumer
from src.utils import get_yaml_configs

schema, params = get_yaml_configs()

wrong_messages = [
    {
        'url': 'http://www.site.com',
        'pattern': 'somepattern',
        'pattern_matched': 'text',
        'response_time': 0.119738,
        'response_code': 200,
    },
    {
        'url': 'http://www.site.com',
        'new_pattern': 'somepattern',
        'pattern_matched': 0,
        'response_time': 0.119738,
        'response_code': 200,
    }

]


@pytest.fixture(scope='function')
def consumer():
    cons = Consumer(params, schema)
    yield cons
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


@pytest.mark.parametrize('msg', wrong_messages, ids=['incorrect value type', 'incorrect name -> new_pattern'])
def test_schema_validation_incorrect_msg_field_type(consumer, msg):
    assert not consumer.validate_schema(msg)


def test_store_msg_to_db(consumer):
    msg = {
        'url': 'http://www.site.com',
        'pattern': 'somepattern',
        'pattern_matched': 0,
        'response_time': 0.119738,
        'response_code': 200,
    }
    is_ok = consumer.store_data_to_db(msg)[0]
    assert is_ok
