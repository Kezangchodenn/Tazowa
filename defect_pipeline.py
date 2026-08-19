"""
defect_pipeline.py
===================
Shared detection logic used by BOTH the batch script (firebase_defect_detector.py)
and the streaming Kafka consumer (kafka_consumer.py). Matches the current
project logic: equirectangular longitude/latitude output (not raw box_2d),
no date-folder guessing -- milestone identity comes from Firestore's
milestoneId + status fields.

STRUCTURE (updated)
--------------------
Each Milestone now contains multiple PHASES, and each Phase contains
multiple TASKS, following the same hierarchy style used in the master
milestone library document (Purpose / Expected visible scope / etc.):

    Milestone (e.g. MS001 - Water Leakage & Dampness Control)
        Phase 1 (e.g. Active Water Leakage)
            Task 1 ...
            Task 2 ...
        Phase 2 (e.g. Damp Area / Moisture Staining)
            Task 1 ...
            Task 2 ...
        ...

A "label" returned by the vision model (e.g. "Puddle") is matched back
to the Phase it belongs to, so every stored defect carries both a label
and a phase_id/phase_name.
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
# Detection rule-sets: Milestone -> Phases -> Tasks
# ============================================================

MILESTONE_RULES = {
    "MS001": {
        "name": "Milestone 1: Water Leakage & Dampness Control",
        "exclusions": (
            "ABSOLUTELY DO NOT detect wall cracks, concrete damage, or "
            "electrical sockets as water leakage. ABSOLUTELY DO NOT detect "
            "sunlight glare, lens flare, window backlight, overexposed/blown-out "
            "highlights, or reflections off glass, metal, or varnished wood as "
            "water damage or dampness -- these are lighting artifacts, not defects."
        ),
        "phases": [
            {
                "phase_id": "MS001-P1",
                "phase_name": "Active Water Leakage",
                "label": "Water Leakage",
                "tasks": [
                    "Look for water actively flowing, dripping, or seeping from pipes, joints, fittings, ceilings, or walls.",
                    "Confirm the leak source area is clearly visible in the frame before flagging it.",
                    "Do not flag dry stains or shadows that only resemble a leak.",
                    "Do not flag bright sunlit or backlit areas near windows/doors as leakage.",
                ],
            },
            {
                "phase_id": "MS001-P2",
                "phase_name": "Damp Area / Moisture Staining",
                "label": "Damp Area",
                "tasks": [
                    "Identify discolored, darkened, or visibly wet patches on walls, ceilings, or floors.",
                    "Check for mold or mildew growth that indicates prolonged dampness.",
                    "Do not confuse paint variation, lighting shadows, or dust marks with dampness.",
                    "Genuine dampness/moisture staining is a DARKENING (brownish, greyish, or yellowish patch). "
                    "A BRIGHTENED, washed-out, or glowing area caused by direct sunlight or backlight through a "
                    "window/door is the opposite of this and must NOT be reported.",
                    "If the surface in question is directly in front of, on, or immediately beside a bright "
                    "window/doorway and appears overexposed (white/blown-out with no visible texture), do not "
                    "classify it as damp -- there is not enough visible surface detail to confirm dampness there.",
                ],
            },
            {
                "phase_id": "MS001-P3",
                "phase_name": "Puddle / Standing Water",
                "label": "Puddle",
                "tasks": [
                    "Detect pooled or standing water on floors or horizontal surfaces.",
                    "Verify the puddle boundary is visible and not a reflection or wet-look tile finish.",
                ],
            },
            {
                "phase_id": "MS001-P4",
                "phase_name": "Water Damage",
                "label": "Water Damage",
                "tasks": [
                    "Detect peeling paint, blistering, or plaster damage caused by water exposure.",
                    "Detect stained or sagging ceiling tiles or panels.",
                    "Detect swelling or warping of adjacent materials (wood, boards, panels) from moisture.",
                    "Peeling/blistering paint MUST show an actual visible physical texture change -- flakes lifting "
                    "off the surface, cracked/curling paint edges, or bubbled material. A smooth surface that is "
                    "simply very bright due to sunlight/glare is NOT peeling paint, even if it looks 'white' or "
                    "'washed out' in the photo.",
                    "Wood window/door frames that appear bright or glowing from direct sunlight behind them are "
                    "NOT water damage unless you can also see actual cracking, flaking, or discoloration texture "
                    "independent of the lighting.",
                ],
            },
        ],
    },
    "MS002": {
        "name": "Milestone 2: Electrical Safety Compliance",
        "exclusions": (
            "ABSOLUTELY DO NOT detect water stains or concrete cracks as electrical hazards."
        ),
        "phases": [
            {
                "phase_id": "MS002-P1",
                "phase_name": "Exposed Wiring",
                "label": "Exposed Wire",
                "tasks": [
                    "Look for loose, cut, or exposed electrical cables/conductors.",
                    "Check for cables without proper conduit, casing, or insulation.",
                ],
            },
            {
                "phase_id": "MS002-P2",
                "phase_name": "Broken Switch / Outlet",
                "label": "Broken Switch",
                "tasks": [
                    "Look for cracked, broken, or missing wall switch and outlet covers.",
                    "Check for outlets with visibly damaged or scorched faceplates.",
                ],
            },
            {
                "phase_id": "MS002-P3",
                "phase_name": "Open Fusebox / Junction Box",
                "label": "Open Fusebox",
                "tasks": [
                    "Look for open or uncovered fuse boxes, distribution boards, or junction boxes.",
                    "Check for exposed internal wiring or components inside an open box.",
                ],
            },
            {
                "phase_id": "MS002-P4",
                "phase_name": "Spark Hazard",
                "label": "Spark Hazard",
                "tasks": [
                    "Look for visible sparking, arcing, scorch marks, or burn damage near electrical points.",
                    "Check for signs of overheating such as melted casing or discoloration.",
                ],
            },
        ],
    },
    "MS003": {
        "name": "Milestone 3: Machinery & Safety Gear Audit",
        "exclusions": (
            "Do NOT detect minor cracks, water, or wiring under this milestone."
        ),
        "phases": [
            {
                "phase_id": "MS003-P1",
                "phase_name": "Personnel & PPE",
                "label": "Person",
                "tasks": [
                    "Detect all workers/personnel visible on site.",
                    "For each detected person, check whether a Safety Helmet is being worn.",
                    "For each detected person, check whether a High-Visibility Vest is being worn.",
                ],
            },
            {
                "phase_id": "MS003-P2",
                "phase_name": "Heavy Machinery",
                "label": "Machinery",
                "tasks": [
                    "Detect cranes, excavators, and concrete trucks present in the frame.",
                    "Note machinery positioned close to workers or site edges as a coordination point.",
                ],
            },
        ],
    },
    "MS004": {
        "name": "Milestone 4: Structural Integrity Inspection",
        "exclusions": (
            "Do NOT detect worker helmets, cables, or clean surfaces under this milestone."
        ),
        "phases": [
            {
                "phase_id": "MS004-P1",
                "phase_name": "Wall / Surface Cracks",
                "label": "Wall Crack",
                "tasks": [
                    "Look for visible fissures or cracks in walls, beams, or slabs.",
                    "Distinguish structural cracks from surface finish lines or shadows.",
                ],
            },
            {
                "phase_id": "MS004-P2",
                "phase_name": "Exposed Rebar",
                "label": "Exposed Rebar",
                "tasks": [
                    "Look for reinforcement steel bars exposed due to spalled or missing concrete cover.",
                    "Check for corrosion visible on exposed rebar.",
                ],
            },
            {
                "phase_id": "MS004-P3",
                "phase_name": "Concrete Spalling",
                "label": "Concrete Spalling",
                "tasks": [
                    "Look for concrete surface flaking, chipping, or breaking away.",
                    "Check for honeycombing or voids in the concrete surface.",
                ],
            },
            {
                "phase_id": "MS004-P4",
                "phase_name": "Sinking / Settlement",
                "label": "Sinking Foundation",
                "tasks": [
                    "Look for visible tilting, sinking, or uneven settlement of foundation or floor elements.",
                    "Check for gaps forming between structural elements that suggest movement.",
                ],
            },
        ],
    },
}

RESIZE_MAX_W = 1024

GENERAL_IMAGE_NOTES = """
    GENERAL IMAGE NOTES (apply these to every phase below):
    - This is a full 360-degree EQUIRECTANGULAR panorama (the image is a flat
      unwrap of a spherical view, roughly 2:1 width:height). The far LEFT edge
      and far RIGHT edge of the image are the SAME physical direction in the
      room (the panorama wraps around). Do not report the same real-world
      defect twice just because a similar-looking area appears near both edges.
    - Straight real-world lines (window frames, ceiling edges, furniture edges)
      can appear curved or warped due to the equirectangular projection -- this
      warping is a projection artifact, not physical damage or structural
      deformation.
    - Bright windows/doorways with strong backlight are common in these photos.
      Do NOT mistake sunlight glare, lens flare, overexposed/blown-out
      highlights (bright white areas with little to no visible surface
      texture), or reflections off glass/metal/varnished wood for a physical
      defect of ANY kind (not water damage, not cracks, not electrical
      hazards). If an area is too bright/washed-out to see actual surface
      texture, treat it as "insufficient visual detail" and do not classify
      it as a defect.
    - Only report a defect when the physical texture, color change, or
      deformation is clearly and unambiguously visible. When uncertain,
      exclude it rather than guess.
    """


def get_all_phase_ids(milestone_id: str) -> set:
    """The complete set of phase_ids that belong to a milestone."""
    rule = MILESTONE_RULES.get(milestone_id, MILESTONE_RULES["MS001"])
    return {p["phase_id"] for p in rule["phases"]}


def get_phases_for_milestone(milestone_id: str, selected_phase_ids=None) -> list:
    """
    Returns the phase list for a milestone, optionally filtered down to only
    the selected_phase_ids (e.g. ["MS001-P1", "MS001-P3"]). If selected_phase_ids
    is None/empty, ALL phases for the milestone are used (i.e. a full run).
    """
    rule = MILESTONE_RULES.get(milestone_id, MILESTONE_RULES["MS001"])
    if not selected_phase_ids:
        return rule["phases"]
    selected_set = set(selected_phase_ids)
    filtered = [p for p in rule["phases"] if p["phase_id"] in selected_set]
    return filtered if filtered else rule["phases"]


def _milestone_targets(milestone_id: str, selected_phase_ids=None) -> str:
    phases = get_phases_for_milestone(milestone_id, selected_phase_ids)
    return ", ".join(p["label"] for p in phases)


def build_prompt(milestone_id: str, selected_phase_ids=None) -> str:
    rule = MILESTONE_RULES.get(milestone_id, MILESTONE_RULES["MS001"])
    phases = get_phases_for_milestone(milestone_id, selected_phase_ids)

    phase_blocks = []
    for phase in phases:
        task_lines = "\n".join(f"      - {t}" for t in phase["tasks"])
        phase_blocks.append(
            f"    Phase {phase['phase_id']} - {phase['phase_name']} (label: \"{phase['label']}\")\n"
            f"{task_lines}"
        )
    phases_text = "\n".join(phase_blocks)

    return f"""
    Milestone ID: {milestone_id} ({rule['name']})
    Task: Detect ONLY these targets: [{_milestone_targets(milestone_id, selected_phase_ids)}].
{GENERAL_IMAGE_NOTES}
    This run covers the following selected phases. Run every task listed
    under every phase below against the image (phases NOT listed here must
    be ignored even though they belong to the same milestone):

{phases_text}

    Exclusion rule: {rule['exclusions']}

    STRICT ACCURACY RULES:
    - Do not hallucinate or make up detections to satisfy the prompt.
    - If you do not see any of the targets above, return an empty array for "defects" (e.g. "defects": []).
    - Do NOT label unrelated elements as belonging to this milestone unless clearly visible.
    - For each detection, "label" MUST be exactly one of the phase labels listed above.

    Provide bounding boxes in [ymin, xmin, ymax, xmax] format using normalized coordinates (0 to 1000).
    Return ONLY a JSON object with keys "defects" (array of objects with label, box_2d) and "summary" (string).
    """


def get_phase_for_label(milestone_id: str, label: str) -> dict:
    """
    Maps a detected label (e.g. "Puddle") back to its phase definition
    (phase_id + phase_name) within the given milestone. Falls back to a
    generic 'unmatched' phase if the model returns an unexpected label.
    """
    rule = MILESTONE_RULES.get(milestone_id, MILESTONE_RULES["MS001"])
    for phase in rule["phases"]:
        if phase["label"].strip().lower() == str(label).strip().lower():
            return {"phase_id": phase["phase_id"], "phase_name": phase["phase_name"]}
    return {"phase_id": None, "phase_name": "Unmatched"}


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


def get_defects_normalized(small_image: Image.Image, milestone_id: str, api_key: str, selected_phase_ids=None):
    buf = io.BytesIO()
    small_image.convert("RGB").save(buf, format="JPEG", quality=90)
    image_bytes = buf.getvalue()
    prompt = build_prompt(milestone_id, selected_phase_ids)

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


def scale_boxes(normalized_result: dict, orig_w: int, orig_h: int, milestone_id: str = None) -> dict:
    """
    Convert Gemini normalized bounding boxes (0-1000 scale) into original
    image pixel coordinates and calculate longitude/latitude position.
    Also tags each defect with its phase_id/phase_name when milestone_id
    is provided.
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

        entry = {
            "label": det["label"],
            "box_px": box_px,
            "longitude": longitude,
            "latitude": latitude
        }
        if milestone_id:
            entry.update(get_phase_for_label(milestone_id, det["label"]))
        final_boxes.append(entry)
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
                                      rule_id: str, file_path: str, result: dict,
                                      selected_phase_ids=None):
    """
    Single atomic batch write, used by BOTH the batch script and the Kafka
    consumer, so results end up in exactly the same shape no matter which
    one processed a given image.

    processedByAI now only becomes True once the image has been sent through
    ALL phases of its milestone -- not after a single partial-phase run.

    How it works:
      - selected_phase_ids is the set of phases that were just run for THIS
        call (None/empty means "all phases", i.e. a full run in one shot).
      - We read the image's existing hotspotAI doc (if any) to see which
        phases were already completed on PREVIOUS calls, and merge that with
        the phases just completed now -> phasesCompleted.
      - processedByAI = True only when phasesCompleted covers every phase_id
        defined for this milestone's rule_id (rule_id is the "MS00N" key,
        e.g. "MS001"). Until then it stays False, even though this call
        still records its defects.
      - Defects are merged too: any previously-stored defects belonging to
        phases NOT re-run this time are kept; defects for phases run this
        time are replaced with the fresh results.
      1. Sets processedByAI accordingly on the source threeSixtyImages doc.
      2. Creates/overwrites its companion doc in hotspotAI (same doc ID)
         with the detected defect hotspots for this image, including
         phase_id/phase_name per defect and cumulative phase-progress fields.
    Both writes commit together or not at all.
    """
    all_phase_ids = get_all_phase_ids(rule_id) if rule_id in MILESTONE_RULES else set()
    # No explicit selection = this call covered every phase in one shot.
    run_phase_ids = set(selected_phase_ids) if selected_phase_ids else set(all_phase_ids)

    hotspot_ref = get_hotspot_collection(db, company_id, project_id).document(doc_ref.id)

    # Pull prior progress (if this image was already partially processed before).
    previously_completed = set()
    previous_defects = []
    existing_snap = hotspot_ref.get()
    if getattr(existing_snap, "exists", False):
        existing_data = existing_snap.to_dict() or {}
        previously_completed = set(existing_data.get("phasesCompleted", []))
        previous_defects = existing_data.get("defects", [])

    phases_completed_now = previously_completed | run_phase_ids
    is_fully_processed = all_phase_ids.issubset(phases_completed_now) if all_phase_ids else True

    new_defects = [
        {
            "label": d["label"],
            "phaseId": d.get("phase_id"),
            "phaseName": d.get("phase_name"),
            "longitude": d["longitude"],
            "latitude": d["latitude"],
            "box_px": d["box_px"],
        }
        for d in result.get("defects", [])
    ]
    # Keep old defects from phases that weren't part of THIS run; replace/refresh
    # defects for the phases that were just (re)run.
    kept_old_defects = [d for d in previous_defects if d.get("phaseId") not in run_phase_ids]
    merged_defects = kept_old_defects + new_defects

    hotspot_data = {
        "imageDocId": doc_ref.id,
        "filePath": file_path,
        "milestoneId": milestone_id,
        "milestoneCode": milestone_code,
        "ruleId": rule_id,
        "phasesCompleted": sorted(phases_completed_now),
        "phasesTotal": sorted(all_phase_ids),
        "processedByAI": is_fully_processed,
        "processedAt": firestore.SERVER_TIMESTAMP,
        "summary": result.get("summary", ""),
        "defectCount": len(merged_defects),
        "defects": merged_defects,
    }
    batch = db.batch()
    batch.update(doc_ref, {"processedByAI": is_fully_processed})
    batch.set(hotspot_ref, hotspot_data)
    batch.commit()
    return is_fully_processed
