import pytest

import logging
import requests
from requests.exceptions import HTTPError

from src.kafka_consumer import Consumer
from src.kafka_producer import Producer
from src.utils import get_yaml_configs

schema, params = get_yaml_configs()
logging.getLogger().setLevel(logging.INFO)


class MockConnectionError:
    def __init__(self, *args, **kwargs):
        raise requests.exceptions.ConnectionError


class MockHttpError:
    def __init__(self, *args, **kwargs):
        raise requests.exceptions.HTTPError


@pytest.fixture(scope='function')
def producer():
    prod = Producer(params)
    yield prod
    del prod


@pytest.fixture(scope='function')
def fake_producer():
    # Add wrong connection parameter
    f_params = params.copy()
    f_params['kafka_cfg']['kafka_bootstrap_server'] = 'kafka-000000-noname-b9d4.aivencloud.com:00000'
    fake_prod = Producer(f_params)
    yield fake_prod
    del fake_prod


@pytest.fixture(scope='function')
def consumer():
    cons = Consumer(params, schema)
    yield cons
    del cons


def test_message_received_from_site(producer, consumer):
    msg = producer.get_data_from_source()
    msg['url'] = schema['schema']['url']
    assert consumer.validate_schema(msg)


def test_message_received_from_site_connection_error(producer, monkeypatch):
    monkeypatch.setattr("requests.get", MockConnectionError)
    msg = producer.get_data_from_source()
    assert msg.get('response_code') == 0 and msg.get('response_time') == 0.0


def test_message_received_from_site_http_error(producer, monkeypatch):
    monkeypatch.setattr("requests.get", MockHttpError)
    msg = producer.get_data_from_source()
    assert msg.get('response_code') == 1 and msg.get('response_time') == 0.0


def test_unable_to_establish_kafka_connection(fake_producer):
    msg = {
        'url': 'http://www.site.com',
        'pattern': 'somepattern',
        'pattern_matched': 1,
        'response_time': 0.119738,
        'response_code': 200,
    }
    with pytest.raises(SystemExit) as exc:
        fake_producer.send_msg_to_kafka(msg)

    assert exc.value.code == 1
