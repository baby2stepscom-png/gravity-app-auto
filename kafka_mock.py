import json
import os
import time
import glob
import redis

class KafkaProducer:
    def __init__(self, bootstrap_servers=None, value_serializer=None):
        self.value_serializer = value_serializer
        self.redis_url = os.getenv("REDIS_URL")
        
        if self.redis_url:
            self.redis_client = redis.from_url(self.redis_url)
            print(f"[MOCK KAFKA] Connected to Redis at {self.redis_url}")
        else:
            self.redis_client = None
            self.queue_dir = "kafka_mock_data"
            if not os.path.exists(self.queue_dir):
                os.makedirs(self.queue_dir)

    def send(self, topic, value):
        if self.value_serializer:
            value = self.value_serializer(value)
        
        if self.redis_client:
            # Use Redis List as a queue
            self.redis_client.lpush(topic, value)
            print(f"[MOCK KAFKA] Produced to Redis topic {topic}")
        else:
            # Write to a file (Local fallback)
            filename = f"{self.queue_dir}/{topic}_{int(time.time() * 1000)}.json"
            with open(filename, "wb") as f:
                f.write(value)
            print(f"[MOCK KAFKA] Produced to {topic}: {filename}")

class KafkaConsumer:
    def __init__(self, topic, bootstrap_servers=None, value_deserializer=None, auto_offset_reset=None):
        self.topic = topic
        self.value_deserializer = value_deserializer
        self.redis_url = os.getenv("REDIS_URL")
        
        if self.redis_url:
            self.redis_client = redis.from_url(self.redis_url)
            print(f"[MOCK KAFKA] Connected to Redis at {self.redis_url}")
        else:
            self.redis_client = None
            self.queue_dir = "kafka_mock_data"
            if not os.path.exists(self.queue_dir):
                os.makedirs(self.queue_dir)
            self.processed_files = set()

    def __iter__(self):
        return self

    def __next__(self):
        if self.redis_client:
            # Blocking pop from right (FIFO)
            # brpop returns a tuple (key, value)
            # We use a timeout to allow the loop to check for exit conditions if needed, 
            # but here we can block indefinitely or for a short time.
            # Using 1 second timeout to keep it responsive to signals if needed.
            result = self.redis_client.brpop(self.topic, timeout=1)
            
            if result:
                _, data = result
                if self.value_deserializer:
                    data = self.value_deserializer(data)
                
                class MockMessage:
                    def __init__(self, val):
                        self.value = val
                
                return MockMessage(data)
            else:
                # No message, sleep briefly to avoid tight loop if we were not blocking
                # But brpop blocks, so we just return to loop.
                # However, __next__ must return a value or raise StopIteration.
                # Since we want to block until message, we can loop here.
                # But to follow the iterator pattern properly in a blocking way:
                return self.__next__()

        else:
            # Simple polling from files
            while True:
                files = sorted(glob.glob(f"{self.queue_dir}/{self.topic}_*.json"))
                for file in files:
                    if file not in self.processed_files:
                        self.processed_files.add(file)
                        with open(file, "rb") as f:
                            data = f.read()
                            if self.value_deserializer:
                                data = self.value_deserializer(data)
                            
                            class MockMessage:
                                def __init__(self, val):
                                    self.value = val
                            
                            return MockMessage(data)
                
                time.sleep(1) # Poll interval
