import configparser
import json
import time

from confluent_kafka import Producer

kafka_topic = 'confluent-kafka-python-example-topic'

config = configparser.ConfigParser()
config.read('/Users/radek/programs/kafka/config.properties')

conf = {
    'bootstrap.servers': 'bootstrap.mytestkafkacluster.europe-west3.managedkafka.bigdataworkshops.cloud.goog:9092',
    'security.protocol': config['default']['security.protocol'],
    'sasl.mechanism': config['default']['sasl.mechanism'],
    'sasl.username': config['default']['username'],
    'sasl.password': config['default']['password']
}

producer = Producer(conf)

for x in range(600000):
    message = {'klucz': f'wartość{x}'}
    print(f"Sending message: {message}")
    message_str = json.dumps(message).encode('utf-8')
    producer.produce(kafka_topic, value=message_str)
    time.sleep(1)

producer.flush()
