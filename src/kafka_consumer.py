from kafka import KafkaConsumer, TopicPartition
import os
import utils
import json
from db_writer import DrWriter, query_exec


class Consumer:
    def __init__(self, global_config):
        self.global_config = global_config

    def connect_to_kafka(self):
        # try catch some problem

        consumer = KafkaConsumer(bootstrap_servers=self.global_config['kafka_cfg']['kafka_bootstrap_server'],
                                 security_protocol='SSL',
                                 ssl_check_hostname=True,
                                 ssl_cafile=self.global_config['kafka_cfg']['ssl_cafile'],
                                 ssl_certfile=self.global_config['kafka_cfg']['ssl_certfile'],
                                 ssl_keyfile=self.global_config['kafka_cfg']['ssl_keyfile'],)
        return consumer

    def store_data_to_db(self):
        dbw = DrWriter(self.global_config)
        try:
            db_connection = dbw.connect_to_db()
            kafka_consumer = self.connect_to_kafka()
            kafka_consumer.assign([TopicPartition(self.global_config['kafka_cfg']['topic_name'], 0)])
            kafka_consumer.seek_to_beginning(TopicPartition(self.global_config['kafka_cfg']['topic_name'], 0))
            for m in kafka_consumer:
                msg_dict = json.loads(m.value.decode("utf-8"))
                msg_dict['url'] = m.key.decode("utf-8")
                print('DDD-> ', msg_dict)
                insert_q = dbw.convert_raw_data_to_queries(msg_dict)
                query_exec(insert_q, db_connection)
        except KeyboardInterrupt:
            db_connection.close()


if __name__ == '__main__':
    config = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/config.yaml')
    params = utils.read_yaml(config)
    Cx = Consumer(params)
    msg = Cx.store_data_to_db()







