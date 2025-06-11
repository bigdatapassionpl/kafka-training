import argparse

import confluent_kafka

from tokenprovider import TokenProvider

parser = argparse.ArgumentParser()
parser.add_argument('-b', '--bootstrap-servers', dest='bootstrap', type=str, required=True)
parser.add_argument('-t', '--topic-name', dest='topic_name', type=str, default='example-topic', required=False)
parser.add_argument('-n', '--num_messages', dest='num_messages', type=int, default=1, required=False)
args = parser.parse_args()

token_provider = TokenProvider()

config = {
    'bootstrap.servers': args.bootstrap,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': token_provider.get_token,
}

producer = confluent_kafka.Producer(config)

def callback(error, message):
    if error is not None:
        print(error)
        return
    print("Delivered a message to {}[{}]".format(message.topic(), message.partition()))

for i in range(args.num_messages):
    message = f"{i} hello world!".encode('utf-8')
    print(f"Message: {message}")
    producer.produce(args.topic_name, message, callback=callback)

producer.flush()