from kafka import KafkaProducer
import json
import requests
from requests.exceptions import HTTPError, ConnectionError, RequestException
import utils
import os
import time
import re


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
                                 api_version=(0, 10, 0), value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        return producer

    def get_data_from_source(self):
        url = self.global_config['source']['source_url']
        pattern = self.global_config['source']['pattern_to_check']
        # Prepared message that will be sent to Kafka
        msg = {'url': url, 'pattern': {pattern: False}}
        try:
            resp = requests.get(url)
            # Response time
            resp_time = resp.elapsed.total_seconds()
            msg['response_time'] = resp_time
            msg['response_code'] = resp.status_code
            #print('Text of the page ----> ', resp.text)
            r = re.search(pattern.encode('utf-8'), resp.text.encode('utf-8'))
            # Is there pattern patches
            msg['pattern'][pattern] = True if len(r.group(0)) > 0 else False
            return msg
        except ConnectionError:
            msg['response_code'] = 'CE'
        except HTTPError as he:
            msg['response_code'] = he.response.status_code
        except RequestException:
            msg['response_code'] = 'ORE'

    def send_msg_to_kafka(self, msg):
        producer = self.connect_to_kafka()
        topic = self.global_config['kafka_cfg']['topic_name']
        # Send message to Kafka topic
        producer.send(topic, msg)
        producer.flush()


if __name__ == '__main__':
    config = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/config.yaml')
    params = utils.read_yaml(config)
    prod_wrapper = Producer(params)
    print(f'Params {params}')
    i = 0
    while True:
        json_message = prod_wrapper.get_data_from_source()
        print(f'Current json msg -> {json_message}')
        prod_wrapper.send_msg_to_kafka(json_message)
        print(f"Time to sleep -> {params['source']['check_period']}")
        time.sleep(params['source']['check_period'])
        print(f'Check is {i}')
        i += 1

