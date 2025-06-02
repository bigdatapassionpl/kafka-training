import configparser
import json

from confluent_kafka import Consumer

kafka_topic = 'confluent-kafka-python-example-topic'

config = configparser.ConfigParser()
config.read('/Users/radek/programs/kafka/config.properties')

conf = {
    'bootstrap.servers': 'bootstrap.mytestkafkacluster.europe-west3.managedkafka.bigdataworkshops.cloud.goog:9092',
    'group.id': 'moja-grupa',
    'auto.offset.reset': 'earliest',
    'security.protocol': config['default']['security.protocol'],
    'sasl.mechanism': config['default']['sasl.mechanism'],
    'sasl.username': config['default']['username'],
    'sasl.password': config['default']['password']
}

consumer = Consumer(conf)
consumer.subscribe([kafka_topic])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f'Błąd: {msg.error()}')
        continue

    message_str = msg.value().decode('utf-8')
    message_obj = json.loads(message_str)

    print(f'Receiving message: {message_obj}')
