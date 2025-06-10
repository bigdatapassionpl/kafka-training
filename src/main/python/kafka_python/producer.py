import configparser
import json
import sys
import time

from kafka import KafkaProducer

print("All arguments:", sys.argv)
if len(sys.argv) > 2:
    configPath = sys.argv[1]
    configName = sys.argv[2]
    print(f"configPath: {configPath}, configName: {configName}")
else:
    print("Wrong number of arguments!")
    sys.exit(1)

kafka_topic = 'kafka-python-example-topic'

config = configparser.ConfigParser()
config.read(configPath)

producer = KafkaProducer(
    bootstrap_servers=[config[configName]['bootstrap.servers']],
    security_protocol=config[configName]['security.protocol'],
    sasl_mechanism=config[configName]['sasl.mechanism'],
    sasl_plain_username=config[configName]['username'],
    sasl_plain_password=config[configName]['password'],

    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

for x in range(600000):
    message = {'klucz': f'wartość{x}'}
    print(f"Sending message: {message}")
    producer.send(kafka_topic, message)
    time.sleep(1)

producer.flush()
producer.close()
