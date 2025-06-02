from kafka import KafkaProducer
import json
import time
import configparser

kafka_topic = 'kafka-python-example-topic'

config = configparser.ConfigParser()
config.read('/Users/radek/programs/kafka/config.properties')

producer = KafkaProducer(
    bootstrap_servers=['bootstrap.mytestkafkacluster.europe-west3.managedkafka.bigdataworkshops.cloud.goog:9092'],

    value_serializer=lambda v: json.dumps(v).encode('utf-8'),

    security_protocol=config['default']['security.protocol'],
    sasl_mechanism=config['default']['sasl.mechanism'],
    sasl_plain_username=config['default']['username'],
    sasl_plain_password=config['default']['password']
)

for x in range(60):
    message = {'klucz': f'wartość{x}'}
    print(f"Sending message: {message}")
    producer.send(kafka_topic, message)
    time.sleep(1)

producer.flush()
producer.close()
