import kafka
from kafka import KafkaConsumer
import json
from json import loads
import time

class kafka_Consumer():

    def consume_data():
        consumer = KafkaConsumer(
        'test-topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        enable_auto_commit=True)
        print("starting a consumer")
        #     return consumer
        msg_list=[]
        c=0
        for msg in consumer:
            print("Key", msg.key)
            print("Message: ", msg.value)
            time.sleep(3)

if __name__ == "__main__":
    kafka_consumer=kafka_Consumer()
    kafka_consumer.consume_data()
    