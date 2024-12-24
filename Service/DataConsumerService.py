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
        self.r = Redis(host=self.host, port=self.port, decode_responses=True, db=0)


    def kafka_consumer(self):
        consumer = KafkaConsumer(
            "stock_orders",
            group_id="stock_orders_group",
            bootstrap_servers=self.consumer_config['bootstrap_servers'],
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m),
        )

        r = Redis(host="localhost", port=6379, decode_responses="True", db=0)
        for message in consumer:
            msg = message.value
            r.hset(msg['ticker'], mapping=msg)
            return f"Data save sucessfully: {msg['ticker']}"

