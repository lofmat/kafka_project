from kafka import KafkaProducer
import json
import requests
from src import utils
import os
import time
import re

config = os.path.join(os.getcwd(), '../config/config.yaml')

# Read parameters from yaml config
params = utils.read_yaml(config)

producer = KafkaProducer(bootstrap_servers=params['kafka_cfg']['kafka_bootstrap_server'],
                         security_protocol='SSL',
                         ssl_check_hostname=True,
                         ssl_cafile=params['kafka_cfg']['ssl_cafile'],
                         ssl_certfile=params['kafka_cfg']['ssl_certfile'],
                         ssl_keyfile=params['kafka_cfg']['ssl_keyfile'],
                         api_version=(0, 10, 0), value_serializer=lambda v: json.dumps(v).encode('utf-8'))

url = params['source']['source_url']
pattern = params['source']['pattern_to_check']
time_to_sleep = params['source']['check_period']
topic = params['kafka_cfg']['topic_name']
msg = {url: {}}
while True:
    try:
        resp = requests.get(url)
        # Response time
        resp_time = resp.elapsed.total_seconds()
        msg[url] = {'response_time': resp_time}
        r = re.search(pattern, resp.text)
        # Is there pattern patches
        msg[url] = {pattern: True}
        msg[url][pattern] = True if len(r.group(0)) > 0 else False
    # ????????????????????????????????????????????????????????????
    except (requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
            requests.exceptions.Timeout,
            requests.exceptions.RequestException) as e:
        msg[url]['error'] = {str(e).split('.')[-1]}
        continue
    producer.send(topic, msg)
    producer.flush()
    time.sleep(params['source']['check_period'])

