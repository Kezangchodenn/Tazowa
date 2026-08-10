"""
listener.py
=============
Watches Firestore's threeSixtyImages collection directly (using Firestore's
own real-time listener, same idea as your on_snapshot script) and, for
every new or still-unprocessed image, publishes an event via producer.py.

This script does NOT call Gemini and does NOT write anything back to
Firestore -- its only job is "notice something new, tell Kafka about it".
All the actual detection work happens in consumer.py, which is a
completely separate process (possibly on a completely separate machine).

Run with:  python3 listener.py

Must run permanently (same requirement as consumer.py). Locally: leave
the terminal open. In production: deploy as its own Cloud Run service
with --no-cpu-throttling (see README_KAFKA_GCE.md).

NOTE ON STARTUP BEHAVIOR: since this watches processedByAI == False, on
first startup every EXISTING unprocessed image publishes an event
immediately, not just future new uploads -- this doubles as an automatic
backlog catch-up. After that initial burst, it only reacts to genuinely
new/changed documents.
"""

import os
import time

from dotenv import load_dotenv
load_dotenv()

import producer

FIREBASE_SERVICE_ACCOUNT_KEY_PATH = os.getenv("FIREBASE_SERVICE_ACCOUNT_KEY_PATH", "serviceAccountKey.json")
FIREBASE_STORAGE_BUCKET = os.getenv("FIREBASE_STORAGE_BUCKET", "")
COMPANY_ID = os.getenv("FIREBASE_COMPANY_ID", "")
PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID", "")


def validate_config():
    problems = []
    if not FIREBASE_STORAGE_BUCKET:
        problems.append("FIREBASE_STORAGE_BUCKET is not set in your .env file.")
    if not COMPANY_ID:
        problems.append("FIREBASE_COMPANY_ID is not set in your .env file.")
    if not PROJECT_ID:
        problems.append("FIREBASE_PROJECT_ID is not set in your .env file.")
    if problems:
        print("Cannot start -- please fix the following:\n")
        for p in problems:
            print(f"  - {p}")
        raise SystemExit(1)


import firebase_admin
from firebase_admin import credentials, firestore


def init_firebase():
    if not firebase_admin._apps:
        if os.path.exists(FIREBASE_SERVICE_ACCOUNT_KEY_PATH):
            cred = credentials.Certificate(FIREBASE_SERVICE_ACCOUNT_KEY_PATH)
            firebase_admin.initialize_app(cred, {"storageBucket": FIREBASE_STORAGE_BUCKET})
        else:
            # Cloud Run / GCP: use the service's own identity instead of a key file.
            firebase_admin.initialize_app(options={"storageBucket": FIREBASE_STORAGE_BUCKET})
    return firestore.client()


def main():
    validate_config()

    print("Connecting to Firebase...")
    db = init_firebase()

    collection_ref = (
        db.collection("companies").document(COMPANY_ID)
          .collection("projects").document(PROJECT_ID)
          .collection("threeSixtyImages")
          .where("processedByAI", "==", False)
    )

    def on_snapshot(col_snapshot, changes, read_time):
        for change in changes:
            if change.type.name == "ADDED":
                doc_id = change.document.id
                data = change.document.to_dict()
                file_path = data.get("filePath")
                milestone_id = data.get("milestoneId")

                if not file_path or not milestone_id:
                    print(f"  skipping {doc_id}: missing filePath or milestoneId")
                    continue

                print(f"New/unprocessed document detected: {doc_id} -- publishing to Kafka...")
                producer.publish_event(doc_id, file_path, milestone_id, COMPANY_ID, PROJECT_ID)
                print(f"  published.")

    watch = collection_ref.on_snapshot(on_snapshot)

    print("Listening for new/unprocessed documents in threeSixtyImages... (Ctrl+C to stop)\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping listener...")
        watch.unsubscribe()


if __name__ == "__main__":
    main()