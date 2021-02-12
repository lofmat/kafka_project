from kafka import KafkaConsumer, TopicPartition
import os
from utils import read_yaml
import json
from db_writer import DrWriter, query_exec
import logging
import sys

logging.getLogger().setLevel(logging.INFO)


class Consumer:
    def __init__(self, global_config, db_sch):
        self.kafka_bootstrap_server = global_config['kafka_cfg']['kafka_bootstrap_server']
        self.topic_name = global_config['kafka_cfg']['topic_name']
        self.ssl_cafile = global_config['kafka_cfg']['ssl_cafile']
        self.ssl_certfile = global_config['kafka_cfg']['ssl_certfile']
        self.ssl_keyfile = global_config['kafka_cfg']['ssl_keyfile']
        self.psql_cfg = global_config['psql_cfg']
        self.db_schema = db_sch

    def connect_to_kafka(self) -> KafkaConsumer:
        """
        Create kafka consumer instance
        :return: KafkaConsumer
        """
        consumer = KafkaConsumer(bootstrap_servers=self.kafka_bootstrap_server,
                                 security_protocol='SSL',
                                 ssl_check_hostname=True,
                                 ssl_cafile=self.ssl_cafile,
                                 ssl_certfile=self.ssl_certfile,
                                 ssl_keyfile=self.ssl_keyfile,)
        return consumer

    def validate_schema(self, current_message) -> bool:
        """
        Check if message received from Kafka topic has correct format
        :param current_message: {'url': 'http://info.com','pattern': 'footer', 'pattern_matched': 1, 'response_time': 0.121656, 'response_code': 200}
        :return: True if format is correct and False otherwise
        """
        template = self.db_schema['schema']
        if len(template) != len(current_message):
            logging.error(f'Different length of template -> {template} and\n'
                          f' current message -> {current_message}')
            return False
        # Compare keys and values types
        for k in template.keys():
            if k in current_message.keys():
                if not isinstance(current_message[k], type(template[k])):
                    logging.error(f'Different types of value for key  -> {k}\n'
                                  f'Template val -> {template[k]}, current message key -> {current_message[k]}')
                    return False
            else:
                logging.error(f'No such key -> {k} in current message')
                return False
        return True

    def get_data_from_kafka(self) -> KafkaConsumer:
        kafka_consumer = self.connect_to_kafka()
        kafka_consumer.assign([TopicPartition(self.topic_name, 0)])
        kafka_consumer.seek_to_beginning(TopicPartition(self.topic_name, 0))
        return kafka_consumer

    def store_data_to_db(self, msg: dict) -> list:
        dbw = DrWriter(self.psql_cfg, self.db_schema['table_name'])
        with dbw.connect_to_db() as db_connection:
            insert_q = dbw.convert_raw_data_to_queries(msg)
            res = query_exec(insert_q, db_connection)
        return res

    def get_and_store_data_to_db(self) -> None:
        """
        Start Kafka connector and receive data from Kafka topic.
        Then store it the messages to PostgreSQL
        If will be reached 5 fails of message format checking or 5 fails of query execution.
        The consumer will be stopped.
        :return: None
        """
        message_format_fails = 0
        query_failed = 0
        try:
            kk = self.get_data_from_kafka()
            for m in kk:
                # If there were 5 errors, we believe that
                # the topic receives messages in the wrong format
                # or there is some DB related issue
                if message_format_fails == 5 or query_failed == 5:
                    logging.error(f'There are 2 possible reasons of such error:\n'
                                  f'1. {message_format_fails} format checks failed.\n'
                                  f'Please check db schema config that uses producer or topic name\n'
                                  f'2. {query_failed} query failed. Please check the consumer logs.')
                    sys.exit(1)
                current_message_dict = json.loads(m.value.decode("utf-8"))
                current_message_dict['url'] = m.key.decode("utf-8")
                logging.info(f'Received message -> {current_message_dict}')
                if self.validate_schema(current_message_dict):
                    res = self.store_data_to_db(current_message_dict)
                    if not res[0]:
                        query_failed += 1
                else:
                    message_format_fails += 1
        # Catch Ctrl+C
        except KeyboardInterrupt:
            logging.warning('Stopping Kafka consumer...')


if __name__ == '__main__':
    # Configs pathes
    config = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/config.yaml')
    db_schema = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/db_schema.yaml')
    # Read configs
    schema = read_yaml(db_schema)
    params = read_yaml(config)
    # Start consumer
    cons = Consumer(params, schema)
    # Get data from Kafka topic and store it to DB
    cons.get_and_store_data_to_db()









