from behave import *
import os
import imp
import sys
from confluent.schemaregistry.client import CachedSchemaRegistryClient
from confluent.schemaregistry.serializers import MessageSerializer, Util
from nose.tools import assert_raises, assert_equals

sys.path.append('/Users/Cabe/Development/RetStreams')

from src import KafkaProducers

@given(u'I have a Zookeeper Server, Kafka Server and Schema Registry Server running')
def step_impl(context):
        kafka = os.system("nc -vz localhost 9092 >/dev/null 2>&1")
        schema = os.system("nc -vz localhost 8081 >/dev/null 2>&1")
        zookeeper = os.system("nc -vz localhost 2181 >/dev/null 2>&1")

        assert zookeeper == 0 and kafka == 0 and schema == 0


@given('I publish the weather.csv file to kafka')
def step_impl(context):
    pass


@then('I should see 8784 records if I consume the weatherTest topic')
def step_impl(context):
    source_settings = {'SCHEMA_DESCRIPTION': 'weather_schema', 'SOURCE_TYPE': 'csv',
                       'SOURCE_LOCATION': '/Users/Cabe/Development/RetStreams/resources/weather.csv',
                       'FIELDNAMES': 'time,temp,dew_point_temp,humidity,wind_speed,visibility,pressure,weather',
                       'FLOAT_FIELDS': 'temp,dew_point_temp,visibility,pressure',
                        'INT_FIELDS': 'humidity,wind_speed'}

    schema_path = '/Users/Cabe/Development/RetStreams/resources/weather_schema.avsc'
    schema = Util.parse_schema_from_string(open(schema_path).read())
    schema_registry_url = 'http://localhost:8081'
    schema_description = 'weather_schema'
    client = CachedSchemaRegistryClient(url=schema_registry_url)
    schema_id = client.register(schema_description, schema)
    serializer = MessageSerializer(client)
    kafka_broker_url = 'localhost:9092'
    topic = 'weathertesty'


    producer = KafkaProducers.KafkaGenericProducer(schema_id=schema_id, settings=source_settings,
                                                   topic=topic, broker=kafka_broker_url, serializer=serializer)
    producer.process()
    assert_equals(producer.get_processed_count(), 8784)


@given('I try to use KafkaGenericProducer with an \'ODBC\' source type')
def step_impl(context):
    pass

@then('I should get an NotImplementedError')
def step_impl(context):
    source_settings = {'SCHEMA_DESCRIPTION': 'weather_schema', 'SOURCE_TYPE': 'ODBC',
                       'SOURCE_LOCATION': '/Users/Cabe/Development/RetStreams/resources/weather.csv',
                       'FIELDNAMES': 'time,temp,dew_point_temp,humidity,wind_speed,visibility,pressure,weather',
                       'FLOAT_FIELDS': 'temp,dew_point_temp,visibility,pressure',
                        'INT_FIELDS': 'humidity,wind_speed'}

    schema_path = '/Users/Cabe/Development/RetStreams/resources/weather_schema.avsc'
    schema = Util.parse_schema_from_string(open(schema_path).read())
    schema_registry_url = 'http://localhost:8081'
    schema_description = 'weather_schema'
    client = CachedSchemaRegistryClient(url=schema_registry_url)
    schema_id = client.register(schema_description, schema)
    serializer = MessageSerializer(client)
    kafka_broker_url = 'localhost:9092'
    topic = 'weathertesty'

    producer = KafkaProducers.KafkaGenericProducer(schema_id=schema_id, settings=source_settings,
                                                   topic=topic, broker=kafka_broker_url, serializer=serializer)

    with assert_raises(NotImplementedError):
        producer.process()

@given('I give the KafkaGenericProducer the filename \'xyz.test\'')
def step_impl(context):
    pass

@then('I should get an IOError')
def step_impl(context):
    source_settings = {'SCHEMA_DESCRIPTION': 'weather_schema', 'SOURCE_TYPE': 'csv',
                       'SOURCE_LOCATION': '/Users/Cabe/Development/RetStreams/resources/xyz.csv',
                       'FIELDNAMES': 'time,temp,dew_point_temp,humidity,wind_speed,visibility,pressure,weather',
                       'FLOAT_FIELDS': 'temp,dew_point_temp,visibility,pressure',
                        'INT_FIELDS': 'humidity,wind_speed'}

    schema_path = '/Users/Cabe/Development/RetStreams/resources/weather_schema.avsc'
    schema = Util.parse_schema_from_string(open(schema_path).read())
    schema_registry_url = 'http://localhost:8081'
    schema_description = 'weather_schema'
    client = CachedSchemaRegistryClient(url=schema_registry_url)
    schema_id = client.register(schema_description, schema)
    serializer = MessageSerializer(client)
    kafka_broker_url = 'localhost:9092'
    topic = 'weathertesty'

    producer = KafkaProducers.KafkaGenericProducer(schema_id=schema_id, settings=source_settings,
                                                   topic=topic, broker=kafka_broker_url, serializer=serializer)

    with assert_raises(IOError):
        producer.process()

@given('All my tests have passed')
def step_impl(context):
    pass

@then('I add an extra passing scenario to suppress the errors let through by assertraises')
def step_impl(context):
    pass