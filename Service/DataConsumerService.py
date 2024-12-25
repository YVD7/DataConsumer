import json
import yaml
from redis import Redis
from kafka import KafkaConsumer

yml_file = "config.yml"
with open(yml_file, "r") as file:
    config = yaml.safe_load(file)
kafka_config = config['kafka_conf']

class DataConsumerService:
    def __init__(self):
        self.consumer_config = {
            'bootstrap_servers': kafka_config['bootstrap_servers']
        }
        self.host = config['redis']['host']
        self.port = config['redis']['port']


    def kafka_consumer(self):
        consumer = KafkaConsumer(
            "market_orders",
            group_id="market_orders_grp",
            bootstrap_servers=self.consumer_config['bootstrap_servers'],
            value_deserializer=lambda m: json.loads(m),
            auto_offset_reset="latest"

        )
        r = Redis(host=self.host, port=self.port, decode_responses="True", db=0)
        for message in consumer:
            msg = message.value
            print(msg)
            r.hset(msg['ticker'], mapping=msg)
            return f"key: {msg['ticker']}, values: {msg}"

