# Batch Defect Detector — Firebase edition

Fetches images from Firestore + Firebase Storage, groups them by their `milestoneId`
field, runs each group through Gemini for defect detection, and writes an HTML
report (image + defect list, grouped by milestone) plus a JSON report.

## 1. Install dependencies

Open this folder in VSCode, open a terminal (`` Ctrl+` ``), then:

```bash
python3 -m venv venv
source venv/bin/activate        # on Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## 2. Get your Firebase service account key

Firebase Console → Project settings → Service accounts → **Generate new private key**.
Save the downloaded JSON file into this folder (e.g. `serviceAccountKey.json`).

**Do not commit this file to git or share it anywhere.**

## 3. Configure `.env`

```bash
cp .env.example .env
```

Then open `.env` and fill in:

- `FIREBASE_SERVICE_ACCOUNT_KEY_PATH` — path to the JSON key from step 2
- `FIREBASE_STORAGE_BUCKET` — your bucket name (Firebase Console → Storage → shows at the top, e.g. `my-app.appspot.com`)
- `FIREBASE_COMPANY_ID` / `FIREBASE_PROJECT_ID` — the doc IDs in your Firestore path `companies/{COMPANY_ID}/projects/{PROJECT_ID}/threeSixtyImages`
- `GEMINI_API_KEY` — your Gemini API key. **Rotate any key you've ever pasted into a chat or shared publicly** — treat it as compromised.

## 4. Map your milestone IDs

Open `firebase_defect_detector.py` and find `MILESTONE_ID_MAP` near the top.
Your Firestore `milestoneId` field (e.g. `L7sboZReNKzieaFKnbvr_mlstn-bRmRikZ8oNDOtXKt7wBZ`)
is an opaque ID from your own app — it does **not** automatically tell this script
which type of defect to look for. You need to map each real milestoneId to one of:

| Rule ID | Looks for |
|---|---|
| `MS001` | Water leakage / dampness |
| `MS002` | Electrical hazards |
| `MS003` | Personnel / safety gear / heavy machinery |
| `MS004` | Structural defects (cracks, spalling, rebar) |

Example:
```python
MILESTONE_ID_MAP = {
    "L7sboZReNKzieaFKnbvr_mlstn-bRmRikZ8oNDOtXKt7wBZ": "MS001",
    "some-other-milestoneId-from-your-db": "MS004",
}
```

Any `milestoneId` not listed here is **skipped** (with a printed warning) — nothing
gets silently analyzed with the wrong rule set.

## 5. Run it

```bash
python3 firebase_defect_detector.py
```

It will:
1. Connect to Firestore, pull every doc under `companies/{COMPANY_ID}/projects/{PROJECT_ID}/threeSixtyImages`
2. Group them by `milestoneId`
3. For each mapped group: download each image from Storage, resize it, skip exact/near-duplicates within that group, send new ones to Gemini, scale the returned bounding boxes back to full resolution, and draw them
4. Write `batch_defect_outputs/report.html` (auto-opens in your browser) and `batch_defect_outputs/report.json`

## Troubleshooting

- **"Cannot start"** with a list of problems → fix each line, they're config issues (missing `.env` values or missing key file).
- **"NOT MAPPED - will be skipped"** in the console → add that `milestoneId` to `MILESTONE_ID_MAP`.
- **No images found** → double check `FIREBASE_COMPANY_ID` / `FIREBASE_PROJECT_ID` match your actual Firestore path exactly.
- Any other error prints a full Python traceback — paste that back if you get stuck.
