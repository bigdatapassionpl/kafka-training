from confluent_kafka import Consumer
import json

kafka_topic = 'confluent-kafka-python-example-topic'

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'moja-grupa',
    'auto.offset.reset': 'earliest'
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

    print(f'Odebrano wiadomość: {message_obj}')
