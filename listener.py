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

NOTE ON STARTUP BEHAVIOR: this watches the WHOLE collection (not filtered
by Firestore's own query) and checks processedByAI in Python instead.
This is deliberate: if a real upload creates a document WITHOUT the
processedByAI field at all (rather than explicitly setting it to false),
a Firestore-side .where("processedByAI", "==", False) filter would NOT
match it -- a missing field and a field set to false are different things
to Firestore's query engine, even though they mean the same thing to a
human. Checking in Python (data.get("processedByAI", False)) treats a
missing field the same as false, so both cases get caught correctly.

On first startup, every EXISTING document in the collection is delivered
once (Firestore's snapshot semantics), and this script silently skips
the ones already processed -- so you'll see a brief burst of internal
checking, but only genuinely unprocessed images actually get published.
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

# When true, this ONLY tests the Firestore fetching/detection logic --
# it prints what it WOULD publish, but never actually calls Kafka. Use
# this to confirm fetching itself works, completely independent of
# whether Kafka/consumer.py are even running.
DRY_RUN = os.getenv("LISTENER_DRY_RUN", "True").lower() == "true"


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
    # NOTE: no .where(...) filter here anymore -- watching the whole
    # collection and filtering in Python inside on_snapshot instead.
    collection_ref = (
        db.collection("companies").document(COMPANY_ID)
          .collection("projects").document(PROJECT_ID)
          .collection("threeSixtyImages")
    )

    seen_already_published = set()  # avoid re-publishing on every MODIFIED event

    def on_snapshot(col_snapshot, changes, read_time):
        for change in changes:
            if change.type.name not in ("ADDED", "MODIFIED"):
                continue

            doc_id = change.document.id
            data = change.document.to_dict()

            # Treat a MISSING processedByAI field the same as False --
            # this is the actual fix. Real uploads that never set this
            # field at all now get caught correctly.
            already_processed = data.get("processedByAI", False)
            if already_processed:
                continue

            if doc_id in seen_already_published:
                continue

            file_path = data.get("filePath")
            milestone_id = data.get("milestoneId")

            if not file_path or not milestone_id:
                print(f"  skipping {doc_id}: missing filePath or milestoneId")
                continue

            print(f"New/unprocessed document detected: {doc_id}" + (" (DRY RUN - not publishing)" if DRY_RUN else " -- publishing to Kafka..."))
            if not DRY_RUN:
                producer.publish_event(doc_id, file_path, milestone_id, COMPANY_ID, PROJECT_ID)
                print(f"  published.")
            seen_already_published.add(doc_id)

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
