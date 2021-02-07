from kafka import KafkaConsumer, TopicPartition
import os
from src import utils

config = os.path.join(os.getcwd(), '../config/config.yaml')
# Read parameters from yaml config
params = utils.read_yaml(config)
topic = params['kafka_cfg']['topic_name']

consumer = KafkaConsumer(bootstrap_servers=params['kafka_cfg']['kafka_bootstrap_server'],
                         security_protocol='SSL',
                         ssl_check_hostname=True,
                         ssl_cafile=params['kafka_cfg']['ssl_cafile'],
                         ssl_certfile=params['kafka_cfg']['ssl_certfile'],
                         ssl_keyfile=params['kafka_cfg']['ssl_keyfile'],)

consumer.assign([TopicPartition(topic, 0)])
consumer.seek_to_beginning(TopicPartition(topic, 0))
for msg in consumer:
    print(msg)
