import configparser
import sys

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from google.auth import default
from google.auth.transport.requests import Request

kafka_topic = 'confluent-kafka-python-avro-example-topic'


class User(object):
    """
    User record

    Args:
        name (str): User's name

        favorite_number (int): User's favorite number

        favorite_color (str): User's favorite color
    """

    def __init__(self, name=None, favorite_number=None, favorite_color=None):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color


def dict_to_user(obj, ctx):
    """
    Converts object literal(dict) to a User instance.

    Args:
        obj (dict): Object literal(dict)

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """

    if obj is None:
        return None

    return User(name=obj['name'],
                favorite_number=obj['favorite_number'],
                favorite_color=obj['favorite_color'])


def bearer_auth_callback(parameter):
    credentials, project = default(
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )

    # Refresh the credentials to get a valid access token
    credentials.refresh(Request())

    token = credentials.token
    parameter['bearer.auth.token'] = token
    parameter['bearer.auth.identity.pool.id'] = ''
    parameter['bearer.auth.logical.cluster'] = ''
    return parameter


def main(configPath, configName):
    with open("/Users/radek/projects/bigdatapassion/kafka-training/src/main/resources/avro/user.avsc") as f:
        schema_str = f.read()

    config = configparser.ConfigParser()
    config.read(configPath)

    consumer_conf = {
        'group.id': 'example-consumer-group',
        'auto.offset.reset': "earliest"
    }
    # Reading Kafka Client configuration
    for key, value in config[configName].items():
        print(f"{key} = {value}")
        consumer_conf[key] = value

    schema_registry_conf = {
        'bearer.auth.credentials.source': 'CUSTOM',
        'bearer.auth.custom.provider.config': {},
        'bearer.auth.custom.provider.function': bearer_auth_callback,
    }
    for key, value in config['schema.registry'].items():
        print(f"{key} = {value}")
        schema_registry_conf[key] = value

    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str,
                                         dict_to_user)

    consumer = Consumer(consumer_conf)
    consumer.subscribe([kafka_topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            user = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if user is not None:
                print("User record {}: name: {}\n"
                      "\tfavorite_number: {}\n"
                      "\tfavorite_color: {}\n"
                      .format(msg.key(), user.name,
                              user.favorite_number,
                              user.favorite_color))
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':

    print("All arguments:", sys.argv)
    if len(sys.argv) > 2:
        configPath = sys.argv[1]
        configName = sys.argv[2]
        print(f"configPath: {configPath}, configName: {configName}")
    else:
        print("Wrong number of arguments!")
        sys.exit(1)

    main(configPath, configName)
