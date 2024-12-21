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
            "stock-market-data",
            group_id="stock-market-group",
            bootstrap_servers=self.consumer_config['bootstrap_servers'],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m),
            enable_auto_commit=True

        )

        for message in consumer:
            key = message.key
            data = message.value
            return key, data

    # def save_to_redisdb(self, key, data):
    #     r = Redis(host=self.host, port=self.port, decode_responses='True', db=0)
    #     r.hset(key, mapping=data)

    def save_to_redisdb(self, key, data):
        try:
            r = Redis(host=self.host, port=self.port, decode_responses=True, db=0)
            if r.hset(key, mapping=data) == 1:  # Returns True if a new field was added
                print(f"Data saved successfully under key: {key}")
                return True
            else:
                print(f"No new fields were added for key: {key}. Data may already exist.")
                return False
        except Exception as e:
            print(f"Error saving to Redis: {e}")
            return False