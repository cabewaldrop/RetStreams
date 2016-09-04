Feature: As a data engineer I wish to publish data to Apache Kafka.

  Background:
    Given I have a Zookeeper Server, Kafka Server and Schema Registry Server running

  Scenario: Publish data to kafka from the KafkaGenericProducer
    Given I publish the weather.csv file to kafka
    Then I should see 8784 records if I consume the weatherTest topic

  Scenario: Try to use the KafkaGenericProducer with an undefined source type
    Given I try to use KafkaGenericProducer with an 'ODBC' source type
    Then I should get an NotImplementedError

  Scenario: Try to use the KafkaGenericProducer with an incorrect filename
    Given I give the KafkaGenericProducer the filename 'xyz.test'
    Then I should get an IOError
