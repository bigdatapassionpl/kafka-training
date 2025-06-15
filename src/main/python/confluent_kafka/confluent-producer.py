import configparser
import json
import sys
import time

from confluent_kafka import Producer

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
conf = {}

config = configparser.ConfigParser()
config.read(configPath)

# Reading Kafka Client configuration
for key, value in config[configName].items():
    print(f"{key} = {value}")
    conf[key] = value

producer = Producer(conf)

for x in range(600000):
    message = {'key': f'value{x}'}
    print(f"Sending message: {message}")
    message_str = json.dumps(message).encode('utf-8')
    producer.produce(kafka_topic, value=message_str)
    time.sleep(1)

producer.flush()
