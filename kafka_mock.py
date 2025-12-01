import queue
import time
import json

# Global in-memory message queues
# Key: topic name, Value: queue.Queue
_queues = {}

def get_queue(topic):
    if topic not in _queues:
        _queues[topic] = queue.Queue()
    return _queues[topic]

class KafkaProducer:
    def __init__(self, bootstrap_servers=None, value_serializer=None):
        self.value_serializer = value_serializer

    def send(self, topic, value):
        if self.value_serializer:
            value = self.value_serializer(value)
        
        q = get_queue(topic)
        q.put(value)
        print(f"[IN-MEMORY KAFKA] Produced to topic {topic}")

class KafkaConsumer:
    def __init__(self, topic, bootstrap_servers=None, value_deserializer=None, auto_offset_reset=None):
        self.topic = topic
        self.value_deserializer = value_deserializer
        self.q = get_queue(topic)

    def __iter__(self):
        return self

    def __next__(self):
        # Blocking get
        data = self.q.get()
        
        if self.value_deserializer:
            data = self.value_deserializer(data)
        
        class MockMessage:
            def __init__(self, val):
                self.value = val
        
        return MockMessage(data)
