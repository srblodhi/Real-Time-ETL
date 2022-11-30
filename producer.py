from kafka import KafkaProducer
import json
from faker import Faker
import time
from yahooquery import Ticker
import pandas as pd
from collections.abc import MutableMapping

class kafka_Producer():

    def flatten_dict(d: MutableMapping, sep: str= '.') -> MutableMapping:
        [flat_dict] = pd.json_normalize(d, sep=sep).to_dict(orient='records')
        return flat_dict

    def send_data(self):
        symbols=['goog']
        maang = Ticker(symbols, asynchronous=True)
        def json_serializer(data):
            return json.dumps(data).encode('utf-8')
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=json_serializer)
        
        for i in range(0,1000):
            summary_details = maang.summary_detail
            flat_dict=self.flatten_dict(summary_details)
            print(flat_dict)
            try:
                producer.send("test-topic",flat_dict)
            except Exception as e:
                print("Producer not working: ",e)
            time.sleep(3)
    #         return flat_dict

if __name__ == "__main__":
    kafka_producer=kafka_Producer()
    kafka_producer.send_data()