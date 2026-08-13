"""
consumer.py
=============
Always-running service. Subscribes to Kafka, and for every event
published by listener.py:
  1. Confirms the image's milestone is currently active (status == 1).
  2. Confirms processedByAI is still false (in case of a duplicate/
     replayed message).
  3. Assigns/reuses a MilestoneCodeN via the shared persistent mapping
     file (MilestoneCodeMapper, in defect_pipeline.py).
  4. Resolves that code to a detection rule-set (MilestoneCode1 -> MS001, ...).
  5. Downloads the image, resizes, dedups, calls Gemini, converts boxes
     to pixel + longitude/latitude, draws them, saves the annotated image.
  6. Writes processedByAI = true AND the hotspotAI companion doc in one
     atomic Firestore batch -- ONLY after full success.

Run with:  python3 consumer.py

Must run permanently. In production: deploy as its own Cloud Run service
with --no-cpu-throttling (see README_KAFKA_GCE.md).
"""

import os
import io
import json
import traceback
import warnings

warnings.filterwarnings("ignore")

from dotenv import load_dotenv
load_dotenv()

from PIL import Image
from kafka import KafkaConsumer

import defect_pipeline as dp

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
NEW_IMAGE_TOPIC = os.getenv("KAFKA_NEW_IMAGE_TOPIC", "new-image-events")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")

SERVICE_ACCOUNT_KEY_PATH = os.getenv("FIREBASE_SERVICE_ACCOUNT_KEY_PATH", "serviceAccountKey.json")
STORAGE_BUCKET = os.getenv("FIREBASE_STORAGE_BUCKET", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

ACTIVE_STATUS_VALUE = 1
MILESTONE_CODE_MAP_PATH = os.getenv("MILESTONE_CODE_MAP_PATH", "milestone_code_map.json")
OUTPUT_DIR = "batch_defect_outputs"


def kafka_connection_kwargs() -> dict:
    kwargs = {"bootstrap_servers": KAFKA_BROKER}
    if KAFKA_SASL_USERNAME:
        kwargs.update({
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": KAFKA_SASL_USERNAME,
            "sasl_plain_password": KAFKA_SASL_PASSWORD,
        })
    return kwargs


def validate_config():
    problems = []
    if not STORAGE_BUCKET:
        problems.append("FIREBASE_STORAGE_BUCKET is not set in your .env file.")
    if not GEMINI_API_KEY:
        problems.append("GEMINI_API_KEY is not set in your .env file.")
    if not os.path.exists(SERVICE_ACCOUNT_KEY_PATH):
        problems.append(f"Service account key not found at '{SERVICE_ACCOUNT_KEY_PATH}'.")
    if problems:
        print("Cannot start -- please fix the following:\n")
        for p in problems:
            print(f"  - {p}")
        raise SystemExit(1)


import firebase_admin
from firebase_admin import credentials, firestore, storage


def init_firebase():
    if not firebase_admin._apps:
        if os.path.exists(SERVICE_ACCOUNT_KEY_PATH):
            cred = credentials.Certificate(SERVICE_ACCOUNT_KEY_PATH)
            firebase_admin.initialize_app(cred, {"storageBucket": STORAGE_BUCKET})
        else:
            firebase_admin.initialize_app(options={"storageBucket": STORAGE_BUCKET})
    return firestore.client(), storage.bucket()


def download_image_bytes(bucket, file_path: str) -> bytes:
    blob = bucket.blob(file_path)
    return blob.download_as_bytes()


def get_image_doc_ref(db, company_id, project_id, doc_id):
    return (
        db.collection("companies").document(company_id)
          .collection("projects").document(project_id)
          .collection("threeSixtyImages").document(doc_id)
    )


def get_milestone_doc_ref(db, company_id, project_id, milestone_id):
    return (
        db.collection("companies").document(company_id)
          .collection("projects").document(project_id)
          .collection("milestones").document(milestone_id)
    )


def is_milestone_active(db, company_id, project_id, milestone_id) -> bool:
    snap = get_milestone_doc_ref(db, company_id, project_id, milestone_id).get()
    if not snap.exists:
        return False
    return snap.to_dict().get("status") == ACTIVE_STATUS_VALUE


def process_message(db, bucket, mapper, fingerprint_cache, msg: dict):
    doc_id = msg.get("doc_id")
    file_path = msg.get("file_path")
    milestone_id = msg.get("milestone_id")
    company_id = msg.get("company_id")
    project_id = msg.get("project_id")

    if not all([doc_id, file_path, milestone_id, company_id, project_id]):
        print(f"  skipping message, missing required field(s): {msg}")
        return

    print(f"  [1/5] checking milestone active status...", flush=True)
    if not is_milestone_active(db, company_id, project_id, milestone_id):
        print(f"  skipping {doc_id}: milestone '{milestone_id}' is not currently active (status != {ACTIVE_STATUS_VALUE})")
        return
    print(f"  [1/5] done -- milestone is active", flush=True)

    print(f"  [2/5] resolving milestone code / rule...", flush=True)
    mapper.reload()
    milestone_code = mapper.get_or_assign(milestone_id)
    rule_id = dp.resolve_rule_id(milestone_code)
    if rule_id is None:
        print(f"  skipping {doc_id}: milestone '{milestone_id}' -> {milestone_code} has no MS0xx rule defined yet")
        return
    print(f"  [2/5] done -- {milestone_code} -> {rule_id}", flush=True)

    print(f"  [3/5] checking if already processed...", flush=True)
    image_ref = get_image_doc_ref(db, company_id, project_id, doc_id)
    current = image_ref.get()
    if current.exists and current.to_dict().get("processedByAI"):
        print(f"  skipping {doc_id}: already processedByAI = true")
        return
    print(f"  [3/5] done -- not yet processed", flush=True)

    print(f"  [4/5] downloading image from Storage...", flush=True)
    print(f"  processing {doc_id} ({file_path}) -> {milestone_code} -> {rule_id} ...", end=" ", flush=True)
    try:
        content = download_image_bytes(bucket, file_path)
        print(f"\n  [4/5] done -- downloaded {len(content)} bytes", flush=True)
        orig_image = Image.open(io.BytesIO(content)).convert("RGB")
        orig_w, orig_h = orig_image.size

        small = dp.resize_small(orig_image)
        fp = dp.dedup_fingerprint(small)

        cache_key = (rule_id, fp)
        if cache_key in fingerprint_cache:
            source_doc_id, normalized = fingerprint_cache[cache_key]
            duplicate_of = source_doc_id
        else:
            print(f"  [5/5] calling Gemini API...", flush=True)
            normalized = dp.get_defects_normalized(small, rule_id, GEMINI_API_KEY)
            print(f"  [5/5] done -- Gemini responded", flush=True)
            fingerprint_cache[cache_key] = (doc_id, normalized)
            duplicate_of = None

        result = dp.scale_boxes(normalized, orig_w, orig_h)
        annotated = dp.draw_defects(orig_image, result["defects"])

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        annotated.save(os.path.join(OUTPUT_DIR, f"annotated_{rule_id}_{doc_id}.jpg"))

        dp.mark_processed_and_store_hotspot(
            db, image_ref, company_id, project_id,
            milestone_id, milestone_code, rule_id, file_path, result,
        )

        n_defects = len(result["defects"])
        print(f"done ({n_defects} defect(s)), processedByAI=true" + (f" [dup of {duplicate_of}]" if duplicate_of else ""))

    except Exception as e:
        print(f"FAILED: {e}")
        print(traceback.format_exc())


def main():
    validate_config()

    print(f"Milestone code mapping file: {os.path.abspath(MILESTONE_CODE_MAP_PATH)}")
    mapper = dp.MilestoneCodeMapper(MILESTONE_CODE_MAP_PATH)

    print(f"Connecting to Kafka broker at {KAFKA_BROKER} ...")
    consumer = KafkaConsumer(
        NEW_IMAGE_TOPIC,
        group_id="defect-detector-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        **kafka_connection_kwargs(),
    )

    print("Connecting to Firebase...")
    db, bucket = init_firebase()

    print(f"Listening on topic '{NEW_IMAGE_TOPIC}'. Waiting for messages... (Ctrl+C to stop)\n")

    fingerprint_cache = {}

    for message in consumer:
        print(f"Received message: {message.value}")
        process_message(db, bucket, mapper, fingerprint_cache, message.value)
        print()


if __name__ == "__main__":
    main()
