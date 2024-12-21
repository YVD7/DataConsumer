from flask import Flask
from DataConsumer.Service.DataConsumerService import DataConsumerService

data_consumer_app = Flask(__name__)

@data_consumer_app.route("/DataConsumer", methods=['GET'])
def data_consumer():
    dcs = DataConsumerService()
    key, data = dcs.kafka_consumer()
    return dcs.save_to_redisdb(key, data)

# if __name__ == "__main__":
#     data_consumer_app.run(port=8000)