from kafka import KafkaConsumer
import json
import configparser

kafka_topic = 'kafka-python-example-topic'

config = configparser.ConfigParser()
config.read('/Users/radek/programs/kafka/config.properties')

consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=['bootstrap.mytestkafkacluster.europe-west3.managedkafka.bigdataworkshops.cloud.goog:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='moja-grupa',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),

    security_protocol=config['default']['security.protocol'],
    sasl_mechanism=config['default']['sasl.mechanism'],
    sasl_plain_username=config['default']['username'],
    sasl_plain_password=config['default']['password']
)

for message in consumer:
    print(f"Odebrano wiadomość: {message.value}")
