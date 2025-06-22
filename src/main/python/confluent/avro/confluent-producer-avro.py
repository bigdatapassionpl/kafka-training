import configparser
import os
import sys
import time
from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from google.auth import default
from google.auth.transport.requests import Request

kafka_topic = 'confluent-kafka-python-avro-example-topic'


class User(object):
    def __init__(self, name, address, favorite_number, favorite_color):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color


def user_to_dict(user, ctx):
    return dict(
        name=user.name,
        favorite_number=user.favorite_number,
        favorite_color=user.favorite_color
    )


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def bearer_auth_callback(parameter):
    credentials, project = default(
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )

    credentials.refresh(Request())

    token = credentials.token
    parameter['bearer.auth.token'] = token
    parameter['bearer.auth.identity.pool.id'] = ''
    parameter['bearer.auth.logical.cluster'] = ''
    return parameter


def main(configPath, configName, schemaRegistryConfigName):
    # Reading Schema file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, "user.avsc")
    with open(file_path) as f:
        schema_str = f.read()

    config = configparser.ConfigParser()
    config.read(configPath)

    producer_conf = {}
    # Reading Kafka Client configuration
    for key, value in config[configName].items():
        print(f"{key} = {value}")
        producer_conf[key] = value

    schema_registry_conf = {
        'bearer.auth.credentials.source': 'CUSTOM',
        'bearer.auth.custom.provider.config': {},
        'bearer.auth.custom.provider.function': bearer_auth_callback,
    }
    for key, value in config[schemaRegistryConfigName].items():
        print(f"{key} = {value}")
        schema_registry_conf[key] = value

    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     user_to_dict)

    string_serializer = StringSerializer('utf_8')

    producer = Producer(producer_conf)

    for x in range(600000):
        user = User(name=f"User name ${x}",
                    address="user_address_${x}",
                    favorite_color=f"user_favorite_color_${x}",
                    favorite_number=x)

        print(f"Sending message: {user}")
        print("User record: name: {}\n"
              "\tfavorite_number: {}\n"
              "\tfavorite_color: {}\n"
              .format(user.name,
                      user.favorite_number,
                      user.favorite_color))

        producer.produce(topic=kafka_topic,
                         key=string_serializer(str(uuid4())),
                         value=avro_serializer(user, SerializationContext(kafka_topic, MessageField.VALUE)),
                         on_delivery=delivery_report)
        time.sleep(1)

    producer.flush()


if __name__ == '__main__':

    print("All arguments:", sys.argv)
    if len(sys.argv) > 3:
        configPath = sys.argv[1]
        configName = sys.argv[2]
        schemaRegistryConfigName = sys.argv[3]
        print(f"configPath: {configPath}, configName: {configName}")
    else:
        print("Wrong number of arguments!")
        sys.exit(1)

    main(configPath, configName, schemaRegistryConfigName)
