import configparser
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
    """
    User record

    Args:
        name (str): User's name

        favorite_number (int): User's favorite number

        favorite_color (str): User's favorite color

        address(str): User's address; confidential
    """

    def __init__(self, name, address, favorite_number, favorite_color):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color
        # address should not be serialized, see user_to_dict()
        self._address = address

def user_to_dict(user, ctx):
    """
    Returns a dict representation of a User instance for serialization.

    Args:
        user (User): User instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return dict(name=user.name,
                favorite_number=user.favorite_number,
                favorite_color=user.favorite_color)

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


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

    producer_conf = {}
    # Reading Kafka Client configuration
    for key, value in config[configName].items():
        print(f"{key} = {value}")
        producer_conf[key] = value

    schema_registry_conf = {
        # 'bearer.auth.credentials.source': 'SASL_OAUTHBEARER_INHERIT',

        'bearer.auth.credentials.source': 'CUSTOM',
        'bearer.auth.custom.provider.config': {},
        'bearer.auth.custom.provider.function': bearer_auth_callback,

        # 'bearer.auth.credentials.source': 'OAUTHBEARER',
        # 'bearer.auth.logical.cluster': '',
        # 'bearer.auth.identity.pool.id': '',
        # 'bearer.auth.client.id': 'unused',
        # 'bearer.auth.client.secret': 'unused',
        # 'bearer.auth.scope': '',
        # 'bearer.auth.issuer.endpoint.url': 'http://localhost:14293/',
    }
    for key, value in config['schema.registry'].items():
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
    if len(sys.argv) > 2:
        configPath = sys.argv[1]
        configName = sys.argv[2]
        print(f"configPath: {configPath}, configName: {configName}")
    else:
        print("Wrong number of arguments!")
        sys.exit(1)

    main(configPath, configName)
