from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
import json
import requests
from requests.exceptions import HTTPError, ConnectionError, RequestException
import utils
import os
import time
import re
import logging
import sys

logging.getLogger().setLevel(logging.INFO)

class Producer:
    def __init__(self, global_config):
        self.global_config = global_config

    def connect_to_kafka(self):
        producer = KafkaProducer(bootstrap_servers=self.global_config['kafka_cfg']['kafka_bootstrap_server'],
                                 security_protocol='SSL',
                                 ssl_check_hostname=True,
                                 ssl_cafile=self.global_config['kafka_cfg']['ssl_cafile'],
                                 ssl_certfile=self.global_config['kafka_cfg']['ssl_certfile'],
                                 ssl_keyfile=self.global_config['kafka_cfg']['ssl_keyfile'],
                                 api_version=(0, 10, 0), value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                 retries=3)
        return producer

    def get_data_from_source(self):
        url = self.global_config['source']['source_url']
        pattern = self.global_config['source']['pattern_to_check']
        msg = {'pattern': pattern, 'pattern_matched': 0}
        try:
            resp = requests.get(url)
            # Response time
            resp_time = resp.elapsed.total_seconds()
            msg['response_time'] = resp_time
            msg['response_code'] = resp.status_code
            r = re.search(pattern.encode('utf-8'), resp.text.encode('utf-8'))
            if r:
                # Is there pattern patches
                msg['pattern_matched'] = 1
            return msg
        except ConnectionError:
            msg['response_code'] = 0
            msg['response_time'] = 0
            msg['pattern_matched'] = 0
        except HTTPError as he:
            msg['response_code'] = he.response.status_code
            msg['response_time'] = 0
            msg['pattern_matched'] = 0
        except RequestException:
            msg['response_code'] = 1
            msg['response_time'] = 0
            msg['pattern_matched'] = 0

    def send_msg_to_kafka(self, msg):
        producer = self.connect_to_kafka()
        t = self.global_config['kafka_cfg']['topic_name']
        k = bytes(self.global_config['source']['source_url'], 'utf-8')
        # Send message to Kafka topic
        try:
            producer.send(topic=t, key=k, value=msg)
            producer.flush()
            producer.close(timeout=2)
        except KafkaTimeoutError as e:
            producer.close()
            logging.exception(f'{e}. Please check if config contains correct Kafka connection params or topic name')
            sys.exit(1)


if __name__ == '__main__':
    config = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/config.yaml')
    params = utils.read_yaml(config)
    prod = Producer(params)
    i = 1
    try:
        while True:
            json_message = prod.get_data_from_source()
            if not json_message:
                continue
            logging.info(f'Check # {i}')
            logging.info(f" Stats for {params['source']['source_url']} -> {json_message}")
            prod.send_msg_to_kafka(json_message)
            time.sleep(params['source']['check_period'])
            i += 1
    except KeyboardInterrupt:
        print('Stopping Kafka producer...')
