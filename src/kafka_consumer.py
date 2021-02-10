from kafka import KafkaConsumer, TopicPartition
import os
import utils
import json
from db_writer import DrWriter, query_exec
import logging
import sys

logging.getLogger().setLevel(logging.INFO)


class Consumer:
    def __init__(self, global_config, db_sch):
        self.global_config = global_config
        self.db_schema = db_sch

    def connect_to_kafka(self) -> KafkaConsumer:
        """
        Create kafka consumer instance
        :return: KafkaConsumer
        """
        consumer = KafkaConsumer(bootstrap_servers=self.global_config['kafka_cfg']['kafka_bootstrap_server'],
                                 security_protocol='SSL',
                                 ssl_check_hostname=True,
                                 ssl_cafile=self.global_config['kafka_cfg']['ssl_cafile'],
                                 ssl_certfile=self.global_config['kafka_cfg']['ssl_certfile'],
                                 ssl_keyfile=self.global_config['kafka_cfg']['ssl_keyfile'],)
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

    def store_data_to_db(self):
        dbw = DrWriter(self.global_config, self.db_schema)
        fails = 0
        with dbw.connect_to_db() as db_connection:
            try:
                kafka_consumer = self.connect_to_kafka()
                kafka_consumer.assign([TopicPartition(self.global_config['kafka_cfg']['topic_name'], 0)])
                kafka_consumer.seek_to_beginning(TopicPartition(self.global_config['kafka_cfg']['topic_name'], 0))
                for m in kafka_consumer:
                    # If there were 5 errors, we believe that
                    # the topic receives messages in the wrong format
                    if fails == 5:
                        logging.error(f'{fails} format checks failed.\n'
                                      f'Please check db schema config that uses producer or topic name')
                        db_connection.close()
                        sys.exit(1)
                    current_message_dict = json.loads(m.value.decode("utf-8"))
                    current_message_dict['url'] = m.key.decode("utf-8")
                    logging.info(f'Received message -> {current_message_dict}')
                    if self.validate_schema(current_message_dict):
                        insert_q = dbw.convert_raw_data_to_queries(current_message_dict)
                        query_exec(insert_q, db_connection)
                    else:
                        fails += 1
            except KeyboardInterrupt:
                db_connection.close()


if __name__ == '__main__':
    config = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/config.yaml')
    db_schema = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/db_schema.yaml')
    schema = utils.read_yaml(db_schema)
    params = utils.read_yaml(config)

    Cx = Consumer(params, schema)
    msg = Cx.store_data_to_db()









