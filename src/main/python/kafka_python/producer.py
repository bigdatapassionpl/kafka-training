from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

kafka_topic = 'kafka-python-example-topic'

for x in range(60):
    message = {'klucz': f'wartość{x}'}
    print(f"Sending message: {message}")
    producer.send(kafka_topic, message)
    time.sleep(1)

producer.flush()
producer.close()
