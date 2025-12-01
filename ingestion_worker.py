import time
import uuid
import json
import logging
import os
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

import requests
try:
    from kafka import KafkaProducer
except ImportError:
    from kafka_mock import KafkaProducer

# Configuration
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ingestion.raw")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("IngestionWorker")

class IngestionWorker:
    def __init__(self):
        self.producer = self._setup_producer()
        # In a real app, these sources would come from the database (Monitors)
        self.sources = [
            {"type": "search_engine", "query": "remote work software"},
            {"type": "social_mock", "query": "remote work"}
        ]

    def _setup_producer(self):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except Exception as e:
            logger.warning(f"Failed to connect to real Kafka ({e}). Using Mock Kafka.")
            from kafka_mock import KafkaProducer as MockProducer
            return MockProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def fetch_from_search_engine(self, query: str) -> List[Dict[str, Any]]:
        """
        Fetches results from Google Custom Search API.
        Requires GOOGLE_API_KEY and GOOGLE_CX (Custom Search Engine ID) in environment variables.
        """
        api_key = os.getenv("GOOGLE_API_KEY")
        cx = os.getenv("GOOGLE_CX")
        
        if not api_key or not cx:
            logger.warning("Missing Google API keys. Falling back to mock data.")
            return [
                {
                    "title": "[MOCK] Top 10 Remote Work Tools 2024",
                    "link": "https://example.com/remote-tools",
                    "snippet": "Check out these amazing tools for remote teams...",
                    "source": "google_mock"
                }
            ]

        logger.info(f"Fetching from Google Custom Search for: {query}")
        url = "https://www.googleapis.com/customsearch/v1"
        params = {
            "key": api_key,
            "cx": cx,
            "q": query,
            "num": 5 # Limit to 5 results to save quota
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            results = []
            for item in data.get("items", []):
                results.append({
                    "title": item.get("title"),
                    "link": item.get("link"),
                    "snippet": item.get("snippet"),
                    "source": "google"
                })
            return results
        except Exception as e:
            logger.error(f"Error calling Google API: {e}")
            return []

    def fetch_from_social(self, query: str) -> List[Dict[str, Any]]:
        """
        Simulates fetching from a social media API.
        """
        logger.info(f"Fetching from Social Media for: {query}")
        return [
            {
                "text": "Just launched our new remote collaboration app! #remote #startup",
                "url": "https://social.com/post/123",
                "author": "startup_founder",
                "source": "twitter_mock"
            }
        ]

    def process_and_publish(self, raw_data: List[Dict[str, Any]], monitor_id: str):
        for item in raw_data:
            event = {
                "event_id": str(uuid.uuid4()),
                "monitor_id": monitor_id,
                "timestamp": time.time(),
                "payload": item
            }
            self.producer.send(KAFKA_TOPIC, event)
            logger.info(f"Published event: {event['event_id']}")

    def run(self):
        """
        Main loop to poll sources.
        """
        logger.info("Starting Ingestion Worker...")
        while True:
            for source in self.sources:
                try:
                    if source["type"] == "search_engine":
                        data = self.fetch_from_search_engine(source["query"])
                    elif source["type"] == "social_mock":
                        data = self.fetch_from_social(source["query"])
                    else:
                        continue
                    
                    # In a real app, we would look up the monitor_id associated with the query
                    mock_monitor_id = str(uuid.uuid4())
                    self.process_and_publish(data, mock_monitor_id)
                    
                except Exception as e:
                    logger.error(f"Error fetching from {source['type']}: {e}")
            
            logger.info("Sleeping for 60 seconds...")
            time.sleep(60)

if __name__ == "__main__":
    worker = IngestionWorker()
    worker.run()
