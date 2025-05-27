from kafka import KafkaConsumer
import json

kafka_topic = 'kafka-python-example-topic'

consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='moja-grupa',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    print(f"Odebrano wiadomość: {message.value}")
