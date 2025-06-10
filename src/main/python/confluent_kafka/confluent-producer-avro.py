import configparser
import sys
import time
from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField


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


def main(configPath, configName):
    kafka_topic = 'confluent-kafka-python-avro-example-topic'

    with open("/Users/radek/projects/bigdatapassion/kafka-training/src/main/resources/avro/user.avsc") as f:
        schema_str = f.read()

    config = configparser.ConfigParser()
    config.read(configPath)

    username = config[configName]['username']
    token = config[configName]['password']

    producer_conf = {
        'bootstrap.servers': config[configName]['bootstrap.servers'],
        'security.protocol': config[configName]['security.protocol'],
        'sasl.mechanism': config[configName]['sasl.mechanism'],
        'sasl.username': config[configName]['username'],
        'sasl.password': config[configName]['password']
    }

    schema_registry_conf = {
        'url': config[configName]['schema.registry'],
        # 'basic.auth.credentials.source': 'USER_INFO',
        # 'basic.auth.user.info': f'{username}:{token}',
        # 'basic.auth.user.info': f'{username}:{token}',
        # 'bearer.auth.credentials.source': 'OAUTHBEARER',
        'bearer.auth.credentials.source': 'STATIC_TOKEN',
        'bearer.auth.logical.cluster': '',
        'bearer.auth.identity.pool.id': '',
        # 'bearer.auth.scope': '',
        # 'bearer.auth.issuer.endpoint.url': '',
        # 'bearer.auth.client.secret': '',
        # 'bearer.auth.client.id': '',
        'bearer.auth.token': token,
        # 'basic.auth.user.info': config[configName]['username'] + ':' + config[configName]['password']
        # 'http.headers': {
        #     'Authorization': f'Bearer {token}'
        # }
    }

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
