"""
defect_pipeline.py
===================
Shared detection logic used by BOTH the batch script (firebase_defect_detector.py)
and the streaming Kafka consumer (kafka_consumer.py). Matches the current
project logic: equirectangular longitude/latitude output (not raw box_2d),
no date-folder guessing -- milestone identity comes from Firestore's
milestoneId + status fields.
"""

import os
import io
import json
import base64
import hashlib

from PIL import Image, ImageDraw, ImageFont
from firebase_admin import firestore

HAS_NEW_GENAI = False
try:
    from google import genai
    from google.genai import types
    HAS_NEW_GENAI = True
except Exception:
    HAS_NEW_GENAI = False

import urllib.request
import urllib.error

# ============================================================
# Detection rule-sets
# ============================================================

MILESTONE_RULES = {
    "MS001": {
        "name": "Milestone 1: Water Leakage & Dampness Control",
        "targets": "Water Leakage, Water Damage, Damp Area, Puddle",
        "instructions": "CRITICAL: Look ONLY for active water leaks, wet damp areas, and liquid puddles on surfaces. ABSOLUTELY DO NOT detect wall cracks, concrete damage, or electrical sockets as water leakage. If there is no water leakage or water damage, return an EMPTY list of defects: []"
    },
    "MS002": {
        "name": "Milestone 2: Electrical Safety Compliance",
        "targets": "Exposed Wire, Broken Switch, Open Fusebox, Spark Hazard",
        "instructions": "CRITICAL: Look ONLY for active electrical hazards like loose cables, broken wall outlets, exposed junction/fuse boxes. ABSOLUTELY DO NOT detect water stains or concrete cracks as electrical hazards. If no electrical hazard exists, return an EMPTY list of defects: []"
    },
    "MS003": {
        "name": "Milestone 3: Machinery & Safety Gear Audit",
        "targets": "Person, Safety Helmet, High-Visibility Vest, Crane, Excavator, Concrete Truck",
        "instructions": "CRITICAL: Look ONLY for workers (Persons), safety helmets, high-visibility safety vests, or heavy machinery. Do NOT detect minor cracks, water, or wiring. If no personnel or heavy vehicles are visible, return an EMPTY list of defects: []"
    },
    "MS004": {
        "name": "Milestone 4: Structural Integrity Inspection",
        "targets": "Wall Crack, Exposed Rebar, Concrete Spalling, Sinking Foundation",
        "instructions": "CRITICAL: Look ONLY for concrete degradation and structural defects (e.g. wall fissures, concrete spalling, exposed rebar skeletons). Do NOT detect worker helmets, cables, or clean surfaces. If no structural defects are found, return an EMPTY list of defects: []"
    }
}

RESIZE_MAX_W = 1024


def build_prompt(milestone_id: str) -> str:
    rule = MILESTONE_RULES.get(milestone_id, MILESTONE_RULES["MS001"])
    return f"""
    Milestone ID: {milestone_id} ({rule['name']})
    Task: Detect ONLY these targets: [{rule['targets']}].
    Instructions: {rule['instructions']}

    STRICT ACCURACY RULES:
    - Do not hallucinate or make up detections to satisfy the prompt.
    - If you do not see any of the targets above, return an empty array for "defects" (e.g. "defects": []).
    - Do NOT label unrelated elements (such as wall cracks, sockets, or structural issues) as water leakage/water damage unless there is visible dampness or water.

    Provide bounding boxes in [ymin, xmin, ymax, xmax] format using normalized coordinates (0 to 1000).
    Return ONLY a JSON object with keys "defects" (array of objects with label, box_2d) and "summary" (string).
    """


def resize_small(image: Image.Image, max_w: int = RESIZE_MAX_W) -> Image.Image:
    w, h = image.size
    if w <= max_w:
        return image.copy()
    scale = max_w / float(w)
    return image.resize((max_w, int(h * scale)))


def dedup_fingerprint(small_image: Image.Image) -> str:
    thumb = small_image.convert("L").resize((32, 32))
    return hashlib.md5(thumb.tobytes()).hexdigest()


def _call_gemini_new_sdk(image_bytes: bytes, prompt: str, api_key: str, model: str = "gemini-2.5-flash"):
    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(
        model=model,
        contents=[prompt, types.Part.from_bytes(data=image_bytes, mime_type="image/jpeg")],
        config=types.GenerateContentConfig(response_mime_type="application/json"),
    )
    raw_json = response.text.replace("```json", "").replace("```", "").strip()
    parsed = json.loads(raw_json)
    if isinstance(parsed, list):
        return {"defects": parsed, "summary": ""}
    return parsed


def _call_gemini_http(image_bytes: bytes, prompt: str, api_key: str):
    file_data = base64.b64encode(image_bytes).decode("utf-8")
    payload = {
        "contents": [{"parts": [{"text": prompt}, {"inline_data": {"mime_type": "image/jpeg", "data": file_data}}]}],
        "generationConfig": {"response_mime_type": "application/json"},
    }
    headers = {"Content-Type": "application/json"}

    def call_api(model):
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
        req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"), headers=headers, method="POST")
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))

    try:
        res_data = call_api("gemini-2.5-flash")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            res_data = call_api("gemini-1.5-flash")
        else:
            raise

    if "candidates" in res_data and res_data["candidates"]:
        candidate = res_data["candidates"][0]
        if "content" in candidate and "parts" in candidate["content"]:
            text = candidate["content"]["parts"][0]["text"]
            return json.loads(text)
        elif "finishReason" in candidate:
            raise Exception(f"Model failed to generate content. Reason: {candidate['finishReason']}")
    raise Exception(f"Unexpected API response structure: {json.dumps(res_data)}")


def get_defects_normalized(small_image: Image.Image, milestone_id: str, api_key: str):
    buf = io.BytesIO()
    small_image.convert("RGB").save(buf, format="JPEG", quality=90)
    image_bytes = buf.getvalue()
    prompt = build_prompt(milestone_id)

    if HAS_NEW_GENAI:
        try:
            return _call_gemini_new_sdk(image_bytes, prompt, api_key)
        except Exception:
            return _call_gemini_http(image_bytes, prompt, api_key)
    return _call_gemini_http(image_bytes, prompt, api_key)


def pixel_to_lonlat(x: float, y: float, img_w: int, img_h: int) -> tuple:
    """
    Convert pixel coordinates from a 360-degree equirectangular panorama
    into longitude and latitude coordinates.
    """
    longitude = (x / img_w) * 360.0 - 180.0
    latitude = 90.0 - (y / img_h) * 180.0
    return round(longitude, 3), round(latitude, 3)


def scale_boxes(normalized_result: dict, orig_w: int, orig_h: int) -> dict:
    """
    Convert Gemini normalized bounding boxes (0-1000 scale) into original
    image pixel coordinates and calculate longitude/latitude position.
    """
    final_boxes = []
    for det in normalized_result.get("defects", []):
        ymin, xmin, ymax, xmax = det["box_2d"]
        box_px = [
            int(ymin * orig_h / 1000),
            int(xmin * orig_w / 1000),
            int(ymax * orig_h / 1000),
            int(xmax * orig_w / 1000)
        ]
        center_x = (box_px[1] + box_px[3]) / 2
        center_y = (box_px[0] + box_px[2]) / 2
        longitude, latitude = pixel_to_lonlat(center_x, center_y, orig_w, orig_h)
        final_boxes.append({
            "label": det["label"],
            "box_px": box_px,
            "longitude": longitude,
            "latitude": latitude
        })
    return {"defects": final_boxes, "summary": normalized_result.get("summary", "")}


_PALETTE = ["#FF3B30", "#FFCC00", "#34C759", "#007AFF", "#AF52DE", "#FF9500", "#5AC8FA"]


def draw_defects(image: Image.Image, defects: list) -> Image.Image:
    annotated = image.convert("RGB").copy()
    draw = ImageDraw.Draw(annotated)
    try:
        font = ImageFont.truetype("DejaVuSans-Bold.ttf", size=max(14, annotated.width // 90))
    except Exception:
        font = ImageFont.load_default()

    label_colors = {}
    for det in defects:
        label = det["label"]
        color = label_colors.setdefault(label, _PALETTE[len(label_colors) % len(_PALETTE)])
        ymin, xmin, ymax, xmax = det["box_px"]
        width = max(2, annotated.width // 400)
        draw.rectangle([xmin, ymin, xmax, ymax], outline=color, width=width)
        text = label
        text_bbox = draw.textbbox((0, 0), text, font=font)
        tw, th = text_bbox[2] - text_bbox[0], text_bbox[3] - text_bbox[1]
        draw.rectangle([xmin, max(0, ymin - th - 6), xmin + tw + 8, ymin], fill=color)
        draw.text((xmin + 4, max(0, ymin - th - 5)), text, fill="white", font=font)
    return annotated


def pil_to_base64(image: Image.Image, max_w: int = 320) -> str:
    thumb = image.copy()
    if thumb.width > max_w:
        ratio = max_w / thumb.width
        thumb = thumb.resize((max_w, int(thumb.height * ratio)))
    buf = io.BytesIO()
    thumb.save(buf, format="JPEG", quality=85)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


# ============================================================
# Dynamic milestone code mapping (SHARED between the batch script and
# the Kafka consumer -- both read/write the SAME persisted JSON file,
# so a milestone gets the same MilestoneCodeN no matter which entrypoint
# saw it first.)
# ============================================================

class MilestoneCodeMapper:
    """
    Maintains a persistent, append-only mapping of online milestone IDs
    (arbitrary/dynamic strings from the API) to stable internal codes:
    MilestoneCode1, MilestoneCode2, MilestoneCode3, ...

    Rules:
      - The first time a milestone ID is passed to get_or_assign(), it is
        given the next sequential code.
      - If that same milestone ID is passed again (now, later in this
        run, or in a completely different process/run), the SAME code
        is returned -- never a new one.
      - A different, never-before-seen milestone ID always gets the next
        available code; previously assigned codes are never reused or
        renumbered.
      - The mapping is persisted to disk after every new assignment.
      - call reload() before get_or_assign() in any LONG-RUNNING process
        (like the Kafka consumer) to pick up codes assigned by some
        OTHER process (like the batch script) since this object was
        created -- both must point at the same file path to stay in sync.
    """

    def __init__(self, path: str):
        self.path = path
        self._id_to_code = {}
        self._next_n = 1
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as f:
                    data = json.load(f)
                self._id_to_code = data.get("id_to_code", {})
                self._next_n = data.get("next_n", 1)
            except Exception as e:
                print(f"  warning: could not read {self.path} ({e}); starting a fresh milestone code mapping")
                self._id_to_code = {}
                self._next_n = 1

    def reload(self):
        """Re-reads the file from disk, picking up codes assigned by another process."""
        self._load()

    def _save(self):
        tmp_path = self.path + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump({"id_to_code": self._id_to_code, "next_n": self._next_n}, f, indent=2)
        os.replace(tmp_path, self.path)

    def get_or_assign(self, milestone_id: str) -> str:
        code = self._id_to_code.get(milestone_id)
        if code is not None:
            return code
        code = f"MilestoneCode{self._next_n}"
        self._id_to_code[milestone_id] = code
        self._next_n += 1
        self._save()
        return code

    @staticmethod
    def code_number(code: str) -> int:
        return int(code.replace("MilestoneCode", ""))


def resolve_rule_id(milestone_code: str) -> str:
    """
    Maps a MilestoneCodeN -> "MS00N" rule-set key. Returns None if no
    matching rule-set is defined in MILESTONE_RULES yet.
    """
    n = MilestoneCodeMapper.code_number(milestone_code)
    rule_id = f"MS{str(n).zfill(3)}"
    return rule_id if rule_id in MILESTONE_RULES else None


def get_hotspot_collection(db, company_id: str, project_id: str):
    """companies/{company_id}/projects/{project_id}/hotspotAI"""
    return (
        db.collection("companies").document(company_id)
          .collection("projects").document(project_id)
          .collection("hotspotAI")
    )


def mark_processed_and_store_hotspot(db, doc_ref, company_id: str, project_id: str,
                                      milestone_id: str, milestone_code: str,
                                      rule_id: str, file_path: str, result: dict):
    """
    Single atomic batch write, used by BOTH the batch script and the Kafka
    consumer, so results end up in exactly the same shape no matter which
    one processed a given image:
      1. Sets processedByAI = True on the source threeSixtyImages doc.
      2. Creates/overwrites its companion doc in hotspotAI (same doc ID)
         with the detected defect hotspots for this image.
    Both writes commit together or not at all.
    """
    hotspot_ref = get_hotspot_collection(db, company_id, project_id).document(doc_ref.id)
    hotspot_data = {
        "imageDocId": doc_ref.id,
        "filePath": file_path,
        "milestoneId": milestone_id,
        "milestoneCode": milestone_code,
        "ruleId": rule_id,
        "processedByAI": True,
        "processedAt": firestore.SERVER_TIMESTAMP,
        "summary": result.get("summary", ""),
        "defectCount": len(result.get("defects", [])),
        "defects": [
            {
                "label": d["label"],
                "longitude": d["longitude"],
                "latitude": d["latitude"],
                "box_px": d["box_px"],
            }
            for d in result.get("defects", [])
        ],
    }
    batch = db.batch()
    batch.update(doc_ref, {"processedByAI": True})
    batch.set(hotspot_ref, hotspot_data)
    batch.commit()