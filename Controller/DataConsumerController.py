from flask import Flask, request, jsonify
from DataConsumer.Service.DataConsumerService import DataConsumerService

data_consumer_app = Flask(__name__)
dcs = DataConsumerService()

def stop_data_consumer():
    func = request.environ.get("werkzeug.server.shutdown")
    if func is None:
        raise RuntimeError("Not running with the Werkzeug Server")
    func()

@data_consumer_app.route("/DataConsumer", methods=['GET'])
def data_consumer():
    try:
        # Assuming DataConsumerService().kafka_consumer() starts consuming Kafka messages
        dcs.kafka_consumer()
        return "Consumer is running..."
    except Exception as e:
        # Log the exception or handle it as needed
        print(f"Error occurred: {e}")
        return "An error occurred in the consumer."






# if __name__ == "__main__":
#     data_consumer_app.run(port=8000)