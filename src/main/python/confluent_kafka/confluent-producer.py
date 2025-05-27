from confluent_kafka import Producer
import json
import time

kafka_topic = 'confluent-kafka-python-example-topic'

conf = {'bootstrap.servers': 'localhost:9092'}  # adres brokera Kafka
producer = Producer(conf)

for x in range(60):
    message = {'klucz': f'wartość{x}'}
    print(f"Sending message: {message}")
    message_str = json.dumps(message).encode('utf-8')
    producer.produce(kafka_topic, value=message_str)
    time.sleep(1)

producer.flush()
