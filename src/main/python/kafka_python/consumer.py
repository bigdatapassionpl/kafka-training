import configparser
import json
import sys

from kafka import KafkaConsumer

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

consumer = KafkaConsumer(
    kafka_topic,

    bootstrap_servers=[config[configName]['bootstrap.servers']],
    security_protocol=config[configName]['security.protocol'],
    sasl_mechanism=config[configName]['sasl.mechanism'],
    sasl_plain_username=config[configName]['username'],
    sasl_plain_password=config[configName]['password'],

    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='moja-grupa',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
)

for message in consumer:
    print(f"Odebrano wiadomość: {message.value}")
