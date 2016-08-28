from confluent_kafka import Consumer, KafkaError


class KafkaGenericConsumer(object):
    def __init__(self, schema_id, settings, topic, offset_reset, broker, consumer_group, serializer):
        self.schema_id = schema_id
        self.topic = topic
        self.broker = broker
        self.serializer = serializer
        self.consumer_group = consumer_group
        self.offset_reset = offset_reset

        # Unpack settings dictionary
        self.target_type = settings['TARGET_TYPE']
        self.target_location = settings['TARGET_LOCATION']

        self.consumer = Consumer({'bootstrap.servers': self.broker, 'group.id': self.consumer_group,
                                  'default.topic.config': {'auto.offset.reset': self.offset_reset}})
        self.consumer.subscribe([self.topic])

    def parse_db_settings(self, db_settings):
        self.db_host = db_settings['host']
        self.db_user = db_settings['user']
        self.db_name = db_settings['db_name']
        self.db_port = db_settings['port']
        self.db_password = db_settings['password']

    def process(self):

        if self.target_type == 'sysout':
            self._process_to_sysout()
        # elif self.target_type == 'postgres':
        #     self.parse_db_settings(self.database_settings)
        #     self._process_to_postgres()
        else:
            raise NotImplementedError("Only console output has been implemented, set target_type to sysout in settings")

    def _process_to_sysout(self):
        running = True
        while running:
            msg = self.consumer.poll(timeout=0.5)
            print "message polling"
            if msg is None:
                continue
            elif not msg.error():
                decoded_object = self.serializer.decode_message(msg.value())
                print decoded_object
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                running = False
        self.consumer.close()

    # def _process_to_postgres(self):
    #     conn = psycopg2.connect(host=self.db_host, user=self.db_user, database=self.db_name,
    #                             password=self.db_password, port=self.db_port)
    #     running = True
    #     while running:
    #         msg = self.consumer.poll(timeout=0.5)
    #         print "message polling"
    #         if msg is None:
    #             continue
    #         elif not msg.error():
    #             decoded_object = self.serializer.decode_message(msg.value())
