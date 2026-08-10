
"""
kafka_consumer.py
===================
Always-running service. Subscribes to the Kafka topic that receives one
message per newly-created threeSixtyImages doc (published by
cloud_function/main.py). For each message:
  1. Confirms the image's milestone is currently active (status == 1) --
     same rule as the batch script.
  2. Confirms processedByAI is still false (belt-and-suspenders, in case
     of a duplicate/replayed Kafka message).
  3. Assigns/reuses a MilestoneCodeN for this milestone via the SAME
     persistent mapping file the batch script uses (MilestoneCodeMapper,
     shared in defect_pipeline.py) -- NOT a manually-maintained dict.
     Whichever script (this one, or the batch script) sees a given
     milestone ID first assigns it the next code; the other reuses it.
  4. Resolves that code to a detection rule-set (MilestoneCode1 -> MS001,
     MilestoneCode2 -> MS002, ...). Skips (with a clear log line) if no
     matching rule is defined yet.
  5. Downloads the image, resizes, dedups (persists for the life of this
     process, scoped per rule), calls Gemini, converts boxes to pixel +
     longitude/latitude, draws them, saves the annotated image.
  6. Writes processedByAI = true AND the hotspotAI companion doc in a
     single atomic Firestore batch (mark_processed_and_store_hotspot,
     shared with the batch script) -- ONLY after full success.
  7. Appends to a running JSON report, and optionally publishes the
     result onto a second Kafka topic.

Run with:  python3 kafka_consumer.py

This process needs to stay running permanently. For production run it
under systemd, Docker with --restart unless-stopped, or Cloud Run with
min-instances=1 (NOT a Cloud Function).
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
from kafka import KafkaConsumer, KafkaProducer

import defect_pipeline as dp

# ============================================================
# CONFIG - loaded from .env
# ============================================================

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
NEW_IMAGE_TOPIC = os.getenv("KAFKA_NEW_IMAGE_TOPIC", "new-image-events")
RESULTS_TOPIC = os.getenv("KAFKA_RESULTS_TOPIC", "defect-results")
PUBLISH_RESULTS = os.getenv("KAFKA_PUBLISH_RESULTS", "false").lower() == "true"

KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")

SERVICE_ACCOUNT_KEY_PATH = os.getenv("FIREBASE_SERVICE_ACCOUNT_KEY_PATH", "serviceAccountKey.json")
STORAGE_BUCKET = os.getenv("FIREBASE_STORAGE_BUCKET", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

ACTIVE_STATUS_VALUE = 1

# SAME default filename as the batch script -- as long as both scripts run
# from the same working directory, they share one mapping file and stay
# in sync automatically. Override via env var if you keep them in
# different folders.
MILESTONE_CODE_MAP_PATH = os.getenv("MILESTONE_CODE_MAP_PATH", "milestone_code_map.json")

OUTPUT_DIR = "batch_defect_outputs"
RUNNING_REPORT_PATH = os.path.join(OUTPUT_DIR, "streaming_report.json")


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


# ============================================================
# Firebase (Firestore + Storage)
# ============================================================

import firebase_admin
from firebase_admin import credentials, firestore, storage


def init_firebase():
    if not firebase_admin._apps:
        if os.path.exists(SERVICE_ACCOUNT_KEY_PATH):
            cred = credentials.Certificate(SERVICE_ACCOUNT_KEY_PATH)
            firebase_admin.initialize_app(cred, {"storageBucket": STORAGE_BUCKET})
        else:
            # Cloud Run / GCP: use the service's own identity instead of a
            # key file baked into the container.
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


# ============================================================
# Running JSON report (appended to as messages are processed)
# ============================================================

def append_to_report(doc_id: str, file_path: str, milestone_id: str, milestone_code: str, rule_id: str, result: dict):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if os.path.exists(RUNNING_REPORT_PATH):
        with open(RUNNING_REPORT_PATH, "r") as f:
            report = json.load(f)
    else:
        report = {}
    report[doc_id] = {
        "file_path": file_path,
        "milestone_id": milestone_id,
        "milestone_code": milestone_code,
        "rule_id": rule_id,
        "result": result,
    }
    with open(RUNNING_REPORT_PATH, "w") as f:
        json.dump(report, f, indent=2)


# ============================================================
# Main consume loop
# ============================================================

def process_message(db, bucket, producer, mapper, fingerprint_cache, msg: dict):
    doc_id = msg.get("doc_id")
    file_path = msg.get("file_path")
    milestone_id = msg.get("milestone_id")
    company_id = msg.get("company_id")
    project_id = msg.get("project_id")

    if not all([doc_id, file_path, milestone_id, company_id, project_id]):
        print(f"  skipping message, missing required field(s): {msg}")
        return

    if not is_milestone_active(db, company_id, project_id, milestone_id):
        print(f"  skipping {doc_id}: milestone '{milestone_id}' is not currently active (status != {ACTIVE_STATUS_VALUE})")
        return

    # Reload before each use: picks up any codes assigned by the batch
    # script (or another consumer instance) since this process started.
    mapper.reload()
    milestone_code = mapper.get_or_assign(milestone_id)
    rule_id = dp.resolve_rule_id(milestone_code)
    if rule_id is None:
        print(f"  skipping {doc_id}: milestone '{milestone_id}' -> {milestone_code} has no MS0xx rule defined yet")
        return

    image_ref = get_image_doc_ref(db, company_id, project_id, doc_id)
    current = image_ref.get()
    if current.exists and current.to_dict().get("processedByAI"):
        print(f"  skipping {doc_id}: already processedByAI = true")
        return

    print(f"  processing {doc_id} ({file_path}) -> {milestone_code} -> {rule_id} ...", end=" ", flush=True)
    try:
        content = download_image_bytes(bucket, file_path)
        orig_image = Image.open(io.BytesIO(content)).convert("RGB")
        orig_w, orig_h = orig_image.size

        small = dp.resize_small(orig_image)
        fp = dp.dedup_fingerprint(small)

        cache_key = (rule_id, fp)
        if cache_key in fingerprint_cache:
            source_doc_id, normalized = fingerprint_cache[cache_key]
            duplicate_of = source_doc_id
        else:
            normalized = dp.get_defects_normalized(small, rule_id, GEMINI_API_KEY)
            fingerprint_cache[cache_key] = (doc_id, normalized)
            duplicate_of = None

        result = dp.scale_boxes(normalized, orig_w, orig_h)
        annotated = dp.draw_defects(orig_image, result["defects"])

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out_img_path = os.path.join(OUTPUT_DIR, f"annotated_{rule_id}_{doc_id}.jpg")
        annotated.save(out_img_path)

        # Atomic write: processedByAI=true + hotspotAI doc together, or
        # neither -- same shared function the batch script uses.
        dp.mark_processed_and_store_hotspot(
            db, image_ref, company_id, project_id,
            milestone_id, milestone_code, rule_id, file_path, result,
        )

        n_defects = len(result["defects"])
        print(f"done ({n_defects} defect(s)), processedByAI=true" + (f" [dup of {duplicate_of}]" if duplicate_of else ""))

        append_to_report(doc_id, file_path, milestone_id, milestone_code, rule_id, result)

        if PUBLISH_RESULTS and producer is not None:
            producer.send(RESULTS_TOPIC, {
                "doc_id": doc_id,
                "file_path": file_path,
                "milestone_id": milestone_id,
                "milestone_code": milestone_code,
                "rule_id": rule_id,
                "defects": result["defects"],
                "summary": result["summary"],
                "duplicate_of": duplicate_of,
            })
            producer.flush()

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

    producer = None
    if PUBLISH_RESULTS:
        producer = KafkaProducer(
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            **kafka_connection_kwargs(),
        )

    print("Connecting to Firebase...")
    db, bucket = init_firebase()

    print(f"Listening on topic '{NEW_IMAGE_TOPIC}'. Waiting for messages... (Ctrl+C to stop)\n")

    fingerprint_cache = {}  # (rule_id, fingerprint) -> (doc_id, normalized_result)

    for message in consumer:
        print(f"Received message: {message.value}")
        process_message(db, bucket, producer, mapper, fingerprint_cache, message.value)
        print()


if __name__ == "__main__":
    main()