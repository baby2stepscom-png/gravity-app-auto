import hashlib
import hashlib
import logging
import json
import os
from typing import Dict, Any
from dotenv import load_dotenv
try:
    from kafka import KafkaConsumer
except ImportError:
    from kafka_mock import KafkaConsumer
from sqlalchemy import text
from database import SessionLocal

# Load environment variables
load_dotenv()

# Configuration
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ingestion.raw")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ScoringEngine")

class ScoringEngine:
    def __init__(self):
        self.db = SessionLocal()
        self.keywords_positive = ["hiring", "looking for", "budget", "urgent"]
        self.keywords_negative = ["student", "internship", "free", "tutorial"]
        self.consumer = self._setup_consumer()

    def _setup_consumer(self):
        try:
            return KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest'
            )
        except Exception as e:
            logger.warning(f"Failed to connect to real Kafka ({e}). Using Mock Kafka.")
            from kafka_mock import KafkaConsumer as MockConsumer
            return MockConsumer(
                KAFKA_TOPIC,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )

    def generate_fingerprint(self, item: Dict[str, Any]) -> str:
        """
        Creates a unique hash for the item to prevent duplicates.
        Uses URL or a combination of title + snippet.
        """
        if "url" in item:
            raw = item["url"]
        else:
            raw = item.get("title", "") + item.get("snippet", "")
        
        return hashlib.md5(raw.encode('utf-8')).hexdigest()

    def calculate_score(self, item: Dict[str, Any]) -> float:
        """
        Heuristic scoring based on keywords.
        Range: 0.0 to 1.0
        """
        text = (item.get("title", "") + " " + item.get("snippet", "")).lower()
        score = 0.5 # Base score

        # Boost for positive keywords
        for word in self.keywords_positive:
            if word in text:
                score += 0.1
        
        # Penalty for negative keywords
        for word in self.keywords_negative:
            if word in text:
                score -= 0.2

        # Normalize
        return max(0.0, min(1.0, score))

    def process_event(self, event: Dict[str, Any]):
        """
        Main entry point for processing a raw ingestion event.
        """
        payload = event.get("payload", {})
        
        # 1. Deduplication
        fingerprint = self.generate_fingerprint(payload)
        
        # Check if exists in DB
        # Note: In a high-volume system, use Redis for this check.
        # Here we check Postgres for simplicity.
        existing = self.db.execute(text("SELECT 1 FROM leads WHERE fingerprint = :fp"), {"fp": fingerprint}).fetchone()
        if existing:
            logger.info(f"Duplicate detected: {fingerprint}. Skipping.")
            return

        # 2. Scoring
        score = self.calculate_score(payload)
        
        # 3. Enrichment (Mock)
        # In a real app, we might call Clearbit or similar here
        contact_info = {}
        if "email" in payload.get("snippet", ""):
            # Very naive extraction
            contact_info["email_detected"] = True

        # 4. Construct Lead Object
        # We need to map this to the DB schema
        lead_data = {
            "id": event.get("event_id"),
            "monitor_id": event.get("monitor_id"),
            "source": payload.get("source"),
            "url": payload.get("link") or payload.get("url"),
            "snippet": payload.get("snippet") or payload.get("text"),
            "score": score,
            "fingerprint": fingerprint,
            "status": "new"
        }

        # 5. Save
        try:
            self.db.execute(
                text("""
                    INSERT INTO leads (id, monitor_id, source, url, snippet, score, fingerprint, status, created_at)
                    VALUES (:id, :monitor_id, :source, :url, :snippet, :score, :fingerprint, :status, NOW())
                """),
                lead_data
            )
            self.db.commit()
            logger.info(f"Saved lead: {lead_data['id']} (Score: {score})")
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error saving lead: {e}")
        
        # 6. Notify (e.g., Push to WebSocket / Webhook)
        if score > 0.7:
            self.notify_high_value_lead(lead_data)

    def notify_high_value_lead(self, lead: Dict[str, Any]):
        print(f"!!! HIGH VALUE LEAD DETECTED !!! {lead['url']}")

    def run(self):
        logger.info("Starting Scoring Engine...")
        for message in self.consumer:
            try:
                event = message.value
                self.process_event(event)
            except Exception as e:
                logger.error(f"Error processing message: {e}")

if __name__ == "__main__":
    engine = ScoringEngine()
    engine.run()
