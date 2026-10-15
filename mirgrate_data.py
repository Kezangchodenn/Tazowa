import io
import json
import os
import sys
from datetime import datetime, timezone

import firebase_admin
from firebase_admin import firestore, storage
from PIL import Image

# ======================= settings you edit =======================
task_id = "TAS-MS-PRE-010"        # Work ID these photos are checked against (written to "taskId")
PHOTOS = ["1p2", "1p3", "1p4"]    # photo names inside IMAGE_FOLDER (extension optional). [] = every picture in the folder
IMAGE_FOLDER = r"C:\Users\G15\Desktop\test image"

STORAGE_BUCKET = "tazowa-4d7ac.firebasestorage.app"   # VERIFY: Firebase Console > Build > Storage, shown as gs://<name>

DRY_RUN = True            # True = only show what would happen; set to False to really upload
SEND_TO_KAFKA = True      # True = also tell consumer.py to process each photo with the AI
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "new-image-events"
# ==================================================================

# ---- project values (same as before) ----
company_id = "7rHlOfofXfP18gXoZUs1"
project_id = "L7sboZReNKzieaFKnbvr"
milestone_id = "L7sboZReNKzieaFKnbvr_mlstn-bRmRikZ8oND0tXKt7wBZ"
floor_plan_id = "bmZvrWJEZuS5y1O1Kwtn"
POSE = (0, 0, 0, 1, 0, 0, 0)      # qx qy qz qw tx ty tz  (no rotation, position 0,0,0)
FPS = 24.0                        # in your data: timestamp(ms) = frameId / 24 * 1000

# where the Firestore documents go:  /companies/<company>/projects/<project>/threeSixtyImages
NEW_PATH = f"/companies/{company_id}/projects/{project_id}/threeSixtyImages"
IMAGE_EXTS = (".png", ".jpg", ".jpeg", ".webp", ".bmp")

firebase_admin.initialize_app(options={"projectId": "tazowa-4d7ac", "storageBucket": STORAGE_BUCKET})
db = firestore.client()
bucket = storage.bucket()


def findPhotos():
    """Turn the names in PHOTOS into real file paths inside IMAGE_FOLDER."""
    if not os.path.isdir(IMAGE_FOLDER):
        sys.exit(f"ERROR: folder not found: {IMAGE_FOLDER}")
    if not PHOTOS:
        return sorted(os.path.join(IMAGE_FOLDER, f) for f in os.listdir(IMAGE_FOLDER)
                      if f.lower().endswith(IMAGE_EXTS)), []
    found, missing = [], []
    for name in PHOTOS:
        candidates = [name] if name.lower().endswith(IMAGE_EXTS) else [name + ext for ext in IMAGE_EXTS]
        match = next((os.path.join(IMAGE_FOLDER, c) for c in candidates
                      if os.path.isfile(os.path.join(IMAGE_FOLDER, c))), None)
        if match is None:                                   # case-insensitive fallback (e.g. 1P2.PNG)
            wanted = {c.lower() for c in candidates}
            match = next((os.path.join(IMAGE_FOLDER, f) for f in os.listdir(IMAGE_FOLDER)
                          if f.lower() in wanted), None)
        (found if match else missing).append(match or name)
    return found, missing


def checkBucket():
    """Stop early (also in a dry run) if the Storage bucket name is wrong."""
    try:
        ok = bucket.exists()
    except Exception as e:      # no permission to look it up -> cannot tell, carry on
        print(f"(note: could not verify bucket '{STORAGE_BUCKET}': {type(e).__name__})")
        return
    if not ok:
        sys.exit(f"ERROR: Storage bucket '{STORAGE_BUCKET}' does not exist.\n"
                 f"  Copy the exact name from Firebase Console > Build > Storage (gs://<name>).\n"
                 f"  New projects usually use  tazowa-4d7ac.firebasestorage.app , older ones  tazowa-4d7ac.appspot.com")


def checkTask():
    """Warn if the task id is unknown or still locked (the consumer silently skips locked tasks)."""
    try:
        import defect_pipeline as dp
    except Exception as e:
        print(f"(note: task not validated, defect_pipeline not importable: {e})")
        return
    m_id, phase = dp.resolve_task_id(task_id)
    if m_id is None:
        sys.exit(f"ERROR: '{task_id}' is not a known task / Work ID. Example: TAS-MS-PRE-010")
    print(f"Task   : {task_id} -> phase '{phase['phase_name']}', milestone {m_id}")
    if not dp.is_task_ready(task_id):
        print(f"WARNING: {task_id} is still LOCKED (an earlier phase is not marked complete).")
        print("         Photos upload fine, but consumer.py will skip them without calling the AI.")


def toPng(local_path):
    with Image.open(local_path) as im:
        buf = io.BytesIO()
        im.convert("RGB").save(buf, format="PNG")
        return buf.getvalue()


def makeDoc(file_path, frame_id, date_str, now):
    qx, qy, qz, qw, tx, ty, tz = POSE
    return {
        "filePath": file_path,
        "frameId": frame_id,
        "floorPlanId": floor_plan_id,
        "milestoneId": milestone_id,
        "group": milestone_id,
        "groupKey": f"{floor_plan_id}_{date_str}",
        "taskId": task_id,                           # <-- the taskId field
        "processedByAI": False,
        "timestamp": frame_id * 1000.0 / FPS,
        "createdAt": now,
        "updatedAt": now,
        "qx": qx, "qy": qy, "qz": qz, "qw": qw,
        "tx": tx, "ty": ty, "tz": tz,
    }


# ------------------------------- main -------------------------------
photos, missing = findPhotos()
if missing:
    sys.exit(f"ERROR: photo(s) not found in {IMAGE_FOLDER}: {', '.join(missing)}\n"
             f"  Files in that folder: {sorted(os.listdir(IMAGE_FOLDER))}")
if not photos:
    sys.exit(f"ERROR: no pictures found in {IMAGE_FOLDER}")

print(f"Photos : {len(photos)} from {IMAGE_FOLDER}")
print(f"Target : {NEW_PATH}")
print(f"Bucket : {STORAGE_BUCKET}")
checkBucket()
checkTask()

# next free frame number = highest existing frameId + 1  (so nothing existing is ever overwritten)
col_ref = db.collection(NEW_PATH.strip("/"))
existing_paths, max_frame = set(), -1
for d in col_ref.stream():
    data = d.to_dict() or {}
    existing_paths.add(data.get("filePath"))
    if isinstance(data.get("frameId"), int):
        max_frame = max(max_frame, data["frameId"])

date_str = datetime.now().strftime("%d-%m-%Y")
now = datetime.now(timezone.utc)
plan = []
for i, local in enumerate(photos, start=1):
    frame_id = max_frame + i
    file_path = f"companies/{company_id}/projects/{project_id}/threeSixtyImages/{date_str}/{frame_id:05d}.png"
    if file_path in existing_paths or bucket.blob(file_path).exists():
        sys.exit(f"ERROR: {file_path} already exists -- refusing to overwrite it.")
    plan.append((local, file_path, makeDoc(file_path, frame_id, date_str, now)))

print()
for local, file_path, doc in plan:
    print(f"  {os.path.basename(local)}  ->  frame {doc['frameId']}  ->  {file_path}")

if DRY_RUN:
    print("\nFirst document that would be created:")
    print(json.dumps({k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in plan[0][2].items()}, indent=2))
    print("\nDRY RUN -- nothing was uploaded, written or sent.  Set DRY_RUN = False at the top to do it for real.")
    sys.exit(0)

producer = None
if SEND_TO_KAFKA:
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                                 value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    except Exception as e:
        print(f"WARNING: cannot connect to Kafka at {KAFKA_BROKER} ({type(e).__name__}). "
              f"Photos will be uploaded but NOT sent to the AI.")

print()
sent = 0
for n, (local, file_path, doc) in enumerate(plan, start=1):
    bucket.blob(file_path).upload_from_string(toPng(local), content_type="image/png")   # 1) the picture -> Storage
    doc_ref = col_ref.document()                                                          # 2) the record  -> Firestore
    doc_ref.set(doc)
    line = f"[{n}/{len(plan)}] {os.path.basename(local)}: uploaded, document {doc_ref.id}"
    if producer:                                                                          # 3) tell the AI
        producer.send(KAFKA_TOPIC, {
            "doc_id": doc_ref.id,
            "file_path": file_path,
            "milestone_id": task_id,          # consumer.py calls the Work ID "milestone_id"
            "company_id": company_id,
            "project_id": project_id,
        })
        sent += 1
        line += ", sent to Kafka"
    print(line)

if producer:
    producer.flush()
    producer.close()
print(f"\nDone. {len(plan)} photo(s) uploaded" + (f", {sent} message(s) sent to '{KAFKA_TOPIC}'." if producer else "."))