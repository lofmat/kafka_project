import json
from kafka import KafkaProducer, conn
import kafka.errors as Errors
import logging
import os
import re
import requests
from requests.exceptions import HTTPError, ConnectionError
import sys
import time
from utils import read_yaml


logging.getLogger().setLevel(logging.INFO)


class Producer:
    def __init__(self, global_config):
        self.kafka_bootstrap_server = global_config['kafka_cfg']['kafka_bootstrap_server']
        self.ssl_cafile = global_config['kafka_cfg']['ssl_cafile']
        self.ssl_certfile = global_config['kafka_cfg']['ssl_certfile']
        self.ssl_keyfile = global_config['kafka_cfg']['ssl_keyfile']
        self.source_url = global_config['source']['source_url']
        self.pattern_to_check = global_config['source']['pattern_to_check']
        self.topic_name = global_config['kafka_cfg']['topic_name']

    def connect_to_kafka(self) -> KafkaProducer:
        producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_server,
                                 security_protocol='SSL',
                                 ssl_check_hostname=True,
                                 ssl_cafile=self.ssl_cafile,
                                 ssl_certfile=self.ssl_certfile,
                                 ssl_keyfile=self.ssl_keyfile,
                                 acks='all',
                                 api_version=(0, 10, 0), value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        return producer

    def get_data_from_source(self) -> dict:
        """
        Pattern_matched equal to 0 when the pattern wasn't found in page code and 1 otherwise
        Response code equal to 0 when raised not HTTPError exception
        Response time equal to 0 when raised some exception
        Data format:
        { pattern: str,
          pattern_matched: str,
          response_time: float,
          response_code: int }

        :return:
        """
        url = self.source_url
        pattern = self.pattern_to_check
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
            msg['response_time'] = 0.0
            return msg
        except HTTPError:
            msg['response_code'] = 1
            msg['response_time'] = 0.0
            return msg

    def send_msg_to_kafka(self, msg: dict) -> None:
        """
        Send message to Kafka.
        Message format:
        { pattern: str,
          pattern_matched: str,
          response_time: float,
          response_code: int,
          url: str }
        :param msg: dict
        :return: None
        """
        producer_kafka_connection = self.connect_to_kafka()
        kafka_topic = self.topic_name
        url_as_key = bytes(self.source_url, 'utf-8')
        # Send message to Kafka topic
        try:
            logging.info(f'Sending to Kafka message -> {msg}')
            kafka_host = str(self.kafka_bootstrap_server).split(':')[0]
            kafka_port = str(self.kafka_bootstrap_server).split(':')[1]
            if not conn.dns_lookup(kafka_host, int(kafka_port)):
                logging.error(f'Unable to connect to {self.kafka_bootstrap_server}.'
                              f' Please check if Kafka server is alive')
                sys.exit(1)
            meta = producer_kafka_connection.send(topic=kafka_topic, key=url_as_key, value=msg)
            # Make all messages in buffer ready to the sending
            producer_kafka_connection.flush()
        except Errors.BrokerNotAvailableError as e:
            producer_kafka_connection.close()
            logging.exception(f'{e}. Please check if config contains correct Kafka connection params or topic name')
            sys.exit(1)


if __name__ == '__main__':
    config = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/config.yaml')
    params = read_yaml(config)
    prod = Producer(params)
    check_count = 1
    try:
        while True:
            json_message = prod.get_data_from_source()
            if not json_message:
                continue
            logging.info(f'Check # {check_count}')
            prod.send_msg_to_kafka(json_message)
            time.sleep(params['source']['check_period'])
            check_count += 1
    # Catch Ctrl+C
    except KeyboardInterrupt:
        logging.warning('Stopping Kafka producer...')
