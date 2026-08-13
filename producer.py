"""
producer.py
============
Publishes "new image" events to Kafka. Used two ways:
  1. Imported by listener.py, which calls publish_event() automatically
     whenever it detects a new/unprocessed Firestore document.
  2. Run directly from the command line to manually publish a test event
     without needing a real Firestore write -- useful for testing
     consumer.py in isolation.

Standalone usage:
  python3 producer.py --doc-id "..." --file-path "..." --milestone-id "..." --company-id "..." --project-id "..."
"""

import os
import json
import argparse

from dotenv import load_dotenv
load_dotenv()

from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
NEW_IMAGE_TOPIC = os.getenv("KAFKA_NEW_IMAGE_TOPIC", "new-image-events")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")

_producer = None


def _kafka_connection_kwargs() -> dict:
    kwargs = {"bootstrap_servers": KAFKA_BROKER, "value_serializer": lambda v: json.dumps(v).encode("utf-8")}
    if KAFKA_SASL_USERNAME:
        kwargs.update({
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": KAFKA_SASL_USERNAME,
            "sasl_plain_password": KAFKA_SASL_PASSWORD,
        })
    return kwargs


def get_producer():
    global _producer
    if _producer is None:
        _producer = KafkaProducer(**_kafka_connection_kwargs())
    return _producer


def publish_event(doc_id: str, file_path: str, milestone_id: str, company_id: str, project_id: str):
    """Publishes one 'new image' event to Kafka. Called by listener.py."""
    message = {
        "doc_id": doc_id,
        "file_path": file_path,
        "milestone_id": milestone_id,
        "company_id": company_id,
        "project_id": project_id,
    }
    producer = get_producer()
    producer.send(NEW_IMAGE_TOPIC, message)
    producer.flush()
    return message


def main():
    parser = argparse.ArgumentParser(description="Manually publish a test 'new image' event to Kafka.")
    parser.add_argument("--doc-id", required=True)
    parser.add_argument("--file-path", required=True)
    parser.add_argument("--milestone-id", required=True)
    parser.add_argument("--company-id", required=True)
    parser.add_argument("--project-id", required=True)
    args = parser.parse_args()

    print(f"Publishing to topic '{NEW_IMAGE_TOPIC}' on {KAFKA_BROKER} ...")
    message = publish_event(args.doc_id, args.file_path, args.milestone_id, args.company_id, args.project_id)
    print(json.dumps(message, indent=2))
    print("\nSent. Check consumer.py's terminal -- it should pick this up within a second or two.")


if __name__ == "__main__":
    main()
