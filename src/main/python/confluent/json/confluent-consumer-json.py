import configparser
import json
import sys

from confluent_kafka import Consumer

print("All arguments:", sys.argv)
if len(sys.argv) > 2:
    configPath = sys.argv[1]
    configName = sys.argv[2]
    print(f"configPath: {configPath}, configName: {configName}")
else:
    print("Wrong number of arguments!")
    sys.exit(1)

# Kafka Client configuration
kafka_topic = 'confluent-kafka-python-example-topic'
conf = {
    'group.id': 'example-consumer-group',
    'auto.offset.reset': 'earliest',
}

config = configparser.ConfigParser()
config.read(configPath)

# Reading Kafka Client configuration
for key, value in config[configName].items():
    print(f"{key} = {value}")
    conf[key] = value

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
