from confluent_kafka import Producer, KafkaError
import csv


class KafkaGenericProducer(object):
    def __init__(self, schema_id, settings, topic, broker, serializer, debug=False):
        self.schema_id = schema_id
        self.topic = topic
        self.broker = broker
        self.serializer = serializer

        # Unpack the settings dictionary
        self.source_type = settings['SOURCE_TYPE']
        self.source_location = settings['SOURCE_LOCATION']
        self.fieldnames = settings['FIELDNAMES'].split(',')
        self.float_fields = settings['FLOAT_FIELDS'].split(',')
        self.int_fields = settings['INT_FIELDS'].split(',')
        self.debug = debug
        self.process_count = 0

    def process(self):
        if self.source_type == 'csv':
            self._process_csv_file()
        else:
            raise NotImplementedError('Only csv sources have been implemented so far')

    def _process_csv_file(self):
        producer = Producer({'bootstrap.servers': self.broker})

        try:
            csvfile = open(self.source_location)
        except IOError:
            print "Could not read file: ", self.source_location
            raise IOError

        with csvfile:
            fieldnames = self.fieldnames
            reader = csv.DictReader(csvfile, fieldnames=fieldnames, delimiter=',')
            next(reader, None)  # Skipping the header

            # Main loop -- Prints a status every 1000 rows
            for row in reader:
                if self.process_count % 1000 == 0:
                    print "Processed {0} rows".format(self.process_count)
                for field in self.int_fields:
                    row[field] = int(row[field])
                for field in self.float_fields:
                    row[field] = float(row[field])
                encoded = self.serializer.encode_record_with_schema_id(self.schema_id, row)
                producer.produce(self.topic, encoded)
                self.process_count += 1
            producer.flush()

    def get_processed_count(self):
        return self.process_count