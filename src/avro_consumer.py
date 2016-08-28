from confluent.schemaregistry.client import CachedSchemaRegistryClient
from confluent.schemaregistry.serializers import MessageSerializer, Util
from KafkaConsumers import KafkaGenericConsumer
import sys
import os
from configobj import ConfigObj

schema_registry_url = os.environ['SCHEMA_REGISTRY_URL']
zookeeper_url = os.environ['ZOOKEEPER_URL']
kafka_broker_url = os.environ['KAFKA_BROKER_URL']


def parse_config():
    try:
        settings_file_name = sys.argv[1]
    except:
        raise ImportError(
            "Failed to import settings. Make sure to provide the settings file path as argument to avro_consumer.py")

    config = ConfigObj(os.path.join(os.path.dirname(__file__), '..', 'conf', settings_file_name))
    return config


if __name__ == '__main__':
    config = parse_config()

    # Unpack dictionaries from config file
    source_settings = config['SOURCE_SETTINGS']
    kafka_settings = config['KAFKA_SETTINGS']
    target_settings = config['TARGET_SETTINGS']

    # Initialize the schema registry client
    schema = Util.parse_schema_from_string(open(source_settings['AVRO_SCHEMA']).read())
    schema_description = source_settings['SCHEMA_DESCRIPTION']
    client = CachedSchemaRegistryClient(url=schema_registry_url)
    schema_id = client.register(schema_description, schema)
    serializer = MessageSerializer(client)

    # Unpack kafka settings
    topic = kafka_settings['TOPIC']
    offset_reset = kafka_settings['OFFSET_RESET']

    # Unpack target settings
    consumer_group = target_settings['CONSUMER_GROUP']

    worker = KafkaGenericConsumer(schema_id=schema_id, settings=target_settings, topic=topic, offset_reset=offset_reset,
                                  broker=kafka_broker_url, consumer_group=consumer_group, serializer=serializer)
    worker.process()



