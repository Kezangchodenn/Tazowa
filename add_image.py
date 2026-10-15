"""
add_image.py -- add new image(s) to Firebase, tagged with a task id and a phase id, and have the AI process them.

For every image it does the same 3 things that happen for your existing images:

  1. STORAGE   upload to  companies/<c>/projects/<p>/threeSixtyImages/<dd-mm-yyyy>/<frame 5 digits>.png
  2. FIRESTORE create the image document (filePath, frameId, floorPlanId, milestoneId, group, groupKey,
               taskId, phaseId, processedByAI=False, timestamps, camera pose)
  3. KAFKA     send the message to 'new-image-events' so consumer.py processes it

Usage (PowerShell, venv active, project folder; consumer.py running in another window):

    python add_image.py photo.jpg --task-id TAS-MS-PRE-010 --phase-id 0oaeno4fUBCLBvpyWAdG --bucket YOUR-BUCKET --dry-run
    python add_image.py photo.jpg --task-id TAS-MS-PRE-010 --phase-id 0oaeno4fUBCLBvpyWAdG --bucket YOUR-BUCKET

    # several files, or a whole folder of images:
    python add_image.py a.png b.png --task-id ... --phase-id ... --bucket ...
    python add_image.py C:\\photos\\site1 --task-id ... --phase-id ... --bucket ...

--task-id   the Work ID the image is checked against, e.g. TAS-MS-PRE-010      (required)
--phase-id  the Firestore phase document id, e.g. 0oaeno4fUBCLBvpyWAdG. When given, the image documents
            are created under  .../milestones/<milestone>/phases/<phase-id>/threeSixtyImages
            (the "new" location from mirgrate_data.py) and also carry a phaseId field.
--bucket    Storage bucket name: Firebase Console -> Build -> Storage, shown as gs://<name>
            (use just <name>), or set env FIREBASE_BUCKET.

Optional:
  --milestone-id ID     Firestore milestone document id (default: the one in mirgrate_data.py)
  --frame-id N          first frame number (default: highest existing frameId + 1; +1 for each further image)
  --date DD-MM-YYYY     folder date in Storage (default: today)
  --floor-plan-id ID    default: the one used by all your existing images
  --collection-path P   override where the image documents are created. Must be the SAME collection
                        consumer.py reads.
  --pose qx qy qz qw tx ty tz   camera pose (default: no rotation, position 0 0 0)
  --skip-kafka          upload + create docs, but do not trigger the AI
  --yes                 required when sending more than 10 images (each one costs a Gemini call)
  --dry-run             show everything, change nothing
"""
import argparse
import io
import json
import os
import sys
from datetime import datetime, timezone

# ---- values from mirgrate_data.py / your existing data ---------------------------------------
FIREBASE_PROJECT_ID = "tazowa-4d7ac"
company_id = "7rHlOfofXfP18gXoZUs1"
project_id = "L7sboZReNKzieaFKnbvr"
DEFAULT_MILESTONE_ID = "L7sboZReNKzieaFKnbvr_mlstn-bRmRikZ8oND0tXKt7wBZ"
DEFAULT_FLOOR_PLAN_ID = "bmZvrWJEZuS5y1O1Kwtn"
FPS = 24.0   # in your data: timestamp(ms) = frameId / 24 * 1000
DEFAULT_TOPIC = "new-image-events"
DEFAULT_BROKER = "localhost:9092"
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".bmp"}
MAX_WITHOUT_CONFIRM = 10


def frame_timestamp_ms(frame_id):
    return frame_id * 1000.0 / FPS


def storage_path(date_str, frame_id):
    return f"companies/{company_id}/projects/{project_id}/threeSixtyImages/{date_str}/{frame_id:05d}.png"


def nested_collection(milestone_id, phase_id):
    return (f"companies/{company_id}/projects/{project_id}"
            f"/milestones/{milestone_id}/phases/{phase_id}/threeSixtyImages")


def build_doc(file_path, frame_id, floor_plan_id, date_str, pose, now, milestone_id, task_id, phase_id=None):
    qx, qy, qz, qw, tx, ty, tz = pose
    doc = {
        "filePath": file_path,
        "frameId": frame_id,
        "floorPlanId": floor_plan_id,
        "milestoneId": milestone_id,
        "group": milestone_id,
        "groupKey": f"{floor_plan_id}_{date_str}",
        "taskId": task_id,
        "processedByAI": False,
        "timestamp": frame_timestamp_ms(frame_id),
        "createdAt": now,
        "updatedAt": now,
        "qx": qx, "qy": qy, "qz": qz, "qw": qw,
        "tx": tx, "ty": ty, "tz": tz,
    }
    if phase_id:
        doc["phaseId"] = phase_id
    return doc


def build_event(doc_id, file_path, task_id):
    return {
        "doc_id": doc_id,
        "file_path": file_path,
        "milestone_id": task_id,      # consumer.py calls the Work ID "milestone_id"
        "company_id": company_id,
        "project_id": project_id,
    }


def collect_images(paths):
    found = []
    for p in paths:
        if os.path.isdir(p):
            found += sorted(os.path.join(p, f) for f in os.listdir(p)
                            if os.path.splitext(f)[1].lower() in IMAGE_EXTS)
        elif os.path.isfile(p):
            found.append(p)
        else:
            sys.exit(f"ERROR: not found: {p}")
    if not found:
        sys.exit("ERROR: no image files found.")
    return found


def to_png_bytes(local_path):
    """Read the local image and return PNG bytes (your existing images are all .png)."""
    from PIL import Image
    with Image.open(local_path) as im:
        buf = io.BytesIO()
        im.convert("RGB").save(buf, format="PNG")
        return buf.getvalue()


def get_clients(bucket_name):
    import firebase_admin
    from firebase_admin import firestore, storage
    if not firebase_admin._apps:
        firebase_admin.initialize_app(options={"projectId": FIREBASE_PROJECT_ID, "storageBucket": bucket_name})
    return firestore.client(), storage.bucket()


def check_bucket(bucket, name):
    """Fail early (also on --dry-run) when the Storage bucket name is wrong."""
    try:
        exists = bucket.exists()
    except Exception as e:   # e.g. no permission to look the bucket up -> cannot tell, carry on
        print(f"(note: could not verify bucket '{name}': {type(e).__name__})")
        return
    if not exists:
        sys.exit(
            f"ERROR: Storage bucket '{name}' does not exist.\n"
            f"  Copy the exact name from Firebase Console -> Build -> Storage (shown as gs://<name>).\n"
            f"  New Firebase projects usually use:  {FIREBASE_PROJECT_ID}.firebasestorage.app\n"
            f"  older ones use:                     {FIREBASE_PROJECT_ID}.appspot.com"
        )


def check_task_id(task_id):
    try:
        import defect_pipeline as dp
    except Exception as e:
        print(f"(note: task id not validated, pipeline not importable: {e})")
        return
    m_id, phase = dp.resolve_task_id(task_id)
    if m_id is None:
        sys.exit(f"ERROR: '{task_id}' is not a known task / Work ID. Example: TAS-MS-PRE-010")
    print(f"Task {task_id} -> phase '{phase['phase_name']}', milestone {m_id}")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("images", nargs="+", help="image file(s) or a folder")
    ap.add_argument("--task-id", "--work-id", dest="task_id", required=True)
    ap.add_argument("--phase-id", default=None)
    ap.add_argument("--milestone-id", default=DEFAULT_MILESTONE_ID)
    ap.add_argument("--bucket", default=os.getenv("FIREBASE_BUCKET"))
    ap.add_argument("--frame-id", type=int, default=None)
    ap.add_argument("--date", default=datetime.now().strftime("%d-%m-%Y"))
    ap.add_argument("--floor-plan-id", default=DEFAULT_FLOOR_PLAN_ID)
    ap.add_argument("--collection-path", default=None)
    ap.add_argument("--pose", type=float, nargs=7, default=[0, 0, 0, 1, 0, 0, 0],
                    metavar=("qx", "qy", "qz", "qw", "tx", "ty", "tz"))
    ap.add_argument("--broker", default=DEFAULT_BROKER)
    ap.add_argument("--topic", default=DEFAULT_TOPIC)
    ap.add_argument("--skip-kafka", action="store_true")
    ap.add_argument("--yes", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    images = collect_images(args.images)
    if not args.bucket:
        sys.exit("ERROR: give --bucket (Firebase Console -> Storage, e.g. tazowa-4d7ac.appspot.com) or set FIREBASE_BUCKET")
    try:
        datetime.strptime(args.date, "%d-%m-%Y")
    except ValueError:
        sys.exit("ERROR: --date must look like 08-05-2026 (DD-MM-YYYY)")
    if len(images) > MAX_WITHOUT_CONFIRM and not args.dry_run and not args.yes:
        sys.exit(f"ERROR: {len(images)} images = {len(images)} Gemini calls. Re-run with --yes to confirm.")

    collection = args.collection_path or (
        nested_collection(args.milestone_id, args.phase_id) if args.phase_id
        else f"companies/{company_id}/projects/{project_id}/threeSixtyImages")

    check_task_id(args.task_id)
    db, bucket = get_clients(args.bucket)
    check_bucket(bucket, args.bucket)

    if args.phase_id:
        phase_doc = f"companies/{company_id}/projects/{project_id}/milestones/{args.milestone_id}/phases/{args.phase_id}"
        if not db.document(phase_doc).get().exists:
            print(f"WARNING: no phase document at .../phases/{args.phase_id} (fine if it only holds sub-collections, "
                  f"but double-check the phase id for typos)")

    # Existing docs: pick the next free frame id and make sure we never overwrite anything
    existing_paths, max_frame = set(), -1
    for d in db.collection(collection).stream():
        data = d.to_dict() or {}
        existing_paths.add(data.get("filePath"))
        if isinstance(data.get("frameId"), int):
            max_frame = max(max_frame, data["frameId"])
    next_frame = args.frame_id if args.frame_id is not None else max_frame + 1

    # ---- plan everything first (so a problem stops the run BEFORE anything is uploaded) ----
    plan, now = [], datetime.now(timezone.utc)
    for local in images:
        frame_id = next_frame
        next_frame += 1
        file_path = storage_path(args.date, frame_id)
        if file_path in existing_paths:
            sys.exit(f"ERROR: an image doc for {file_path} already exists. Use another --frame-id or --date.")
        if bucket.blob(file_path).exists():
            sys.exit(f"ERROR: {file_path} already exists in Storage -- refusing to overwrite it.")
        existing_paths.add(file_path)
        doc = build_doc(file_path, frame_id, args.floor_plan_id, args.date, args.pose, now,
                        args.milestone_id, args.task_id, args.phase_id)
        plan.append((local, file_path, doc))

    print(f"\nFirestore collection: {collection}")
    print(f"Task id: {args.task_id}" + (f" | Phase id: {args.phase_id}" if args.phase_id else " | (no phase id)"))
    print(f"{len(plan)} image(s):")
    for local, file_path, doc in plan:
        print(f"  {os.path.basename(local)}  ->  frame {doc['frameId']}  ->  {file_path}")
    first = plan[0][2]
    print("First document:\n" + json.dumps({k: (v.isoformat() if isinstance(v, datetime) else v)
                                            for k, v in first.items()}, indent=2))

    if args.dry_run:
        print("\nDRY RUN -- nothing was uploaded, written or sent.")
        return [p[2] for p in plan]

    producer = None
    if not args.skip_kafka:
        from kafka import KafkaProducer
        producer = KafkaProducer(bootstrap_servers=args.broker,
                                 value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    events = []
    for n, (local, file_path, doc) in enumerate(plan, 1):
        bucket.blob(file_path).upload_from_string(to_png_bytes(local), content_type="image/png")
        ref = db.collection(collection).document()
        ref.set(doc)
        event = build_event(ref.id, file_path, args.task_id)
        events.append(event)
        if producer:
            producer.send(args.topic, event)
        print(f"[{n}/{len(plan)}] {os.path.basename(local)}: uploaded, doc {ref.id}" +
              (", sent to Kafka" if producer else ", Kafka skipped"))
    if producer:
        producer.flush()
        producer.close()
        print(f"\nSent {len(events)} message(s) to '{args.topic}'. Watch the consumer.py window.")
    return events


if __name__ == "__main__":
    main()