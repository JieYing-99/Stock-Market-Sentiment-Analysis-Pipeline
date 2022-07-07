from kafka import KafkaProducer
import json
from common import KAFKA_BOOTSTRAP_SERVER

class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    def send(self, topic, record):
        self.producer.send(topic, record)

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()