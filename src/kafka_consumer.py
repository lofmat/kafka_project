from kafka import KafkaConsumer, TopicPartition
import os
import utils
# How to write json https://kb.objectrocket.com/postgresql/insert-json-data-into-postgresql-using-python-part-2-1248
# How to compare json files https://stackoverflow.com/questions/36635726/how-to-compare-2-jsons-in-python-only-with-keys

#SQL_INSERT_QUERY = """INSERT INTO table (
#                              x,
#                              json_list_one,
#                              json_list_two
#                            ) VALUES (%s, %s::json[], %s::json[])"""

# config = os.path.join(os.getcwd(), '../config/config.yaml')
# # Read parameters from yaml config
# params = utils.read_yaml(config)
# topic = params['kafka_cfg']['topic_name']


class Consumer:
    def __init__(self, global_config):
        self.global_config = global_config

    def connect_to_kafka(self):
        consumer = KafkaConsumer(bootstrap_servers=params['kafka_cfg']['kafka_bootstrap_server'],
                                 security_protocol='SSL',
                                 ssl_check_hostname=True,
                                 ssl_cafile=params['kafka_cfg']['ssl_cafile'],
                                 ssl_certfile=params['kafka_cfg']['ssl_certfile'],
                                 ssl_keyfile=params['kafka_cfg']['ssl_keyfile'],)
        return consumer

    def get_data_from_topic(self):
        c = self.connect_to_kafka()
        c.assign([TopicPartition(self.global_config['kafka_cfg']['topic_name'], 0)])
        c.seek_to_beginning(TopicPartition(self.global_config['kafka_cfg']['topic_name'], 0))
        j = 0
        for msg in c:
            print(f'Message N: {j}')
            print(msg)
            j += 1


if __name__ == '__main__':
    config = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../config/config.yaml')
    params = utils.read_yaml(config)
    Cx = Consumer(params)
    Cx.get_data_from_topic()
