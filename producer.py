from kafka import KafkaProducer
import json
from faker import Faker
import time
from yahooquery import Ticker

class kafka_Producer():

    def send_data():
        symbols=['aapl', 'meta', 'msft', 'goog']
        maang = Ticker(symbols, asynchronous=True)
        def json_serializer(data):
            return json.dumps(data).encode('utf-8')
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=json_serializer)
        for i in range(0,10):
            summary_details = maang.summary_detail
            print(summary_details)
            try:
                producer.send("test-topic",summary_details)
            except Exception as e:
                print(e)
            time.sleep(3)

if __name__ == "__main__":
    kafka_producer=kafka_Producer()
    kafka_producer.send_data()