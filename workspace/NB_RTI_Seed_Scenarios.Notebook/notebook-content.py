# Fabric notebook source

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# # RTI Seeded Demo Scenarios -- Deterministic Patient Stories
#
# Injects FOUR named scenarios into the live Eventstream so the dashboard,
# Activator, and Operations Agent have something specific to talk about
# during an executive demo. Run this notebook ~2 minutes BEFORE the demo.
#
# Scenarios:
#   1. SEED:MARIA_LOPEZ  -- 5d post-discharge readmit (DRG 291 heart failure)
#                           -> fires Activator Rule 4 (readmission)
#                           -> appears in vw_all_alerts() as READMISSION
#   2. SEED:RAJ_SINGH    -- 8 high-amount claims in 10 min, 2 facilities
#                           -> fraud_score >= 80 after scorer runs
#                           -> SIU Activator card
#   3. SEED:JOHN_PATIENT -- overdue HbA1c gap + high-cost ($52k, 4 ED visits)
#                           + 1 claim
#                           -> appears in vw_cross_stream_patients() (>=2 streams)
#   4. SEED:BEAUMONT_RO  -- ops capacity (BED_OCCUPANCY 95%, ED_BOARDING 18)
#                           -> COO alert path
#
# All seeded events are tagged so vw_seeded_scenarios() can surface them
# on the Triage & Performance dashboard page. The first patient and first two
# providers in lh_gold_curated.dim_patient / dim_provider are reused as the
# logical "Maria Lopez" / "Dr Raj Singh" identities so streaming patient_ids
# match lakehouse dimension rows (production gap B2 -- see PRODUCTION_GAPS.md).
#
# **Required**: Set ES_CONNECTION_STRING to the same Eventstream Custom
# Endpoint used by NB_RTI_Event_Simulator.

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Attach default lakehouse (self-healing) ----------
import requests as _req
_ws_id = notebookutils.runtime.context.get("currentWorkspaceId", "")
_tok = notebookutils.credentials.getToken("pbi")
_hdr = {"Authorization": f"Bearer {_tok}"}
_lh_resp = _req.get(f"https://api.fabric.microsoft.com/v1/workspaces/{_ws_id}/lakehouses", headers=_hdr)
if _lh_resp.status_code == 200:
    for _lh in _lh_resp.json().get("value", []):
        if _lh["displayName"] == "lh_gold_curated":
            try:
                notebookutils.lakehouse.setDefaultLakehouse(_ws_id, _lh["id"])
                print(f"  Attached lh_gold_curated ({_lh['id'][:8]}...)")
            except Exception as e:
                print(f"  WARNING: setDefaultLakehouse failed: {e}")
            break

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

%pip install azure-eventhub azure-core>=1.31.0 --quiet

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Parameters ----------
# Eventstream Custom Endpoint (required). Same one used by NB_RTI_Event_Simulator.
ES_CONNECTION_STRING = ES_CONNECTION_STRING if "ES_CONNECTION_STRING" in dir() and ES_CONNECTION_STRING else ""

# ---- Scenario identities (all optional) ----
# Leave any of these blank to let the notebook auto-pick the first eligible
# row from the corresponding gold dim. Override to drive the demo against a
# specific patient/provider/facility you want to talk about. Labels are just
# display strings -- they do NOT need to match the real dim name.

# Scenario 1 -- 5d post-discharge readmit (heart failure DRG 291)
SEED_READMIT_PATIENT_ID = SEED_READMIT_PATIENT_ID if "SEED_READMIT_PATIENT_ID" in dir() and SEED_READMIT_PATIENT_ID else ""
SEED_READMIT_LABEL      = SEED_READMIT_LABEL      if "SEED_READMIT_LABEL"      in dir() and SEED_READMIT_LABEL      else "MARIA_LOPEZ"
SEED_READMIT_DRG        = SEED_READMIT_DRG        if "SEED_READMIT_DRG"        in dir() and SEED_READMIT_DRG        else "291"
SEED_READMIT_DX         = SEED_READMIT_DX         if "SEED_READMIT_DX"         in dir() and SEED_READMIT_DX         else "I50.9"
SEED_READMIT_DAYS       = SEED_READMIT_DAYS       if "SEED_READMIT_DAYS"       in dir() and SEED_READMIT_DAYS       else 5
SEED_READMIT_SCORE      = SEED_READMIT_SCORE      if "SEED_READMIT_SCORE"      in dir() and SEED_READMIT_SCORE      else 85.0

# Scenario 2 -- provider fraud velocity (8 high-amount claims, 2 facilities)
SEED_FRAUD_PROVIDER_ID  = SEED_FRAUD_PROVIDER_ID  if "SEED_FRAUD_PROVIDER_ID"  in dir() and SEED_FRAUD_PROVIDER_ID  else ""
SEED_FRAUD_LABEL        = SEED_FRAUD_LABEL        if "SEED_FRAUD_LABEL"        in dir() and SEED_FRAUD_LABEL        else "RAJ_SINGH"
SEED_FRAUD_CLAIM_COUNT  = SEED_FRAUD_CLAIM_COUNT  if "SEED_FRAUD_CLAIM_COUNT"  in dir() and SEED_FRAUD_CLAIM_COUNT  else 8
SEED_FRAUD_BASE_AMOUNT  = SEED_FRAUD_BASE_AMOUNT  if "SEED_FRAUD_BASE_AMOUNT"  in dir() and SEED_FRAUD_BASE_AMOUNT  else 15000.0
SEED_FRAUD_PROC_CODE    = SEED_FRAUD_PROC_CODE    if "SEED_FRAUD_PROC_CODE"    in dir() and SEED_FRAUD_PROC_CODE    else "99215"

# Scenario 3 -- cross-stream patient (care gap + high-cost + claim)
SEED_CROSS_PATIENT_ID   = SEED_CROSS_PATIENT_ID   if "SEED_CROSS_PATIENT_ID"   in dir() and SEED_CROSS_PATIENT_ID   else ""
SEED_CROSS_LABEL        = SEED_CROSS_LABEL        if "SEED_CROSS_LABEL"        in dir() and SEED_CROSS_LABEL        else "JOHN_PATIENT"
SEED_CROSS_GAP_MEASURE  = SEED_CROSS_GAP_MEASURE  if "SEED_CROSS_GAP_MEASURE"  in dir() and SEED_CROSS_GAP_MEASURE  else "HBA1C-DM"
SEED_CROSS_GAP_DAYS     = SEED_CROSS_GAP_DAYS     if "SEED_CROSS_GAP_DAYS"     in dir() and SEED_CROSS_GAP_DAYS     else 120
SEED_CROSS_SPEND_30D    = SEED_CROSS_SPEND_30D    if "SEED_CROSS_SPEND_30D"    in dir() and SEED_CROSS_SPEND_30D    else 52000.0
SEED_CROSS_ED_VISITS    = SEED_CROSS_ED_VISITS    if "SEED_CROSS_ED_VISITS"    in dir() and SEED_CROSS_ED_VISITS    else 4

# Scenario 4 -- facility capacity / staffing mismatch
SEED_CAPACITY_FACILITY_ID = SEED_CAPACITY_FACILITY_ID if "SEED_CAPACITY_FACILITY_ID" in dir() and SEED_CAPACITY_FACILITY_ID else ""
SEED_CAPACITY_LABEL       = SEED_CAPACITY_LABEL       if "SEED_CAPACITY_LABEL"       in dir() and SEED_CAPACITY_LABEL       else "BEAUMONT_RO"
SEED_CAPACITY_NAME_HINT   = SEED_CAPACITY_NAME_HINT   if "SEED_CAPACITY_NAME_HINT"   in dir() and SEED_CAPACITY_NAME_HINT   else "Beaumont"
SEED_CAPACITY_BED_PCT     = SEED_CAPACITY_BED_PCT     if "SEED_CAPACITY_BED_PCT"     in dir() and SEED_CAPACITY_BED_PCT     else 95.0
SEED_CAPACITY_ED_BOARDING = SEED_CAPACITY_ED_BOARDING if "SEED_CAPACITY_ED_BOARDING" in dir() and SEED_CAPACITY_ED_BOARDING else 18

# METADATA **{"language":"python","tags":["parameters"]}**

# CELL **{"language":"python"}**

import pandas as pd
from datetime import datetime, timedelta, timezone
import uuid
import json
import atexit

from azure.eventhub import EventHubProducerClient, EventData

if not ES_CONNECTION_STRING:
    raise RuntimeError("Set ES_CONNECTION_STRING (Eventstream Custom Endpoint) before running.")

_producer = EventHubProducerClient.from_connection_string(ES_CONNECTION_STRING)

# Notebook-idiomatic equivalent of `with EventHubProducerClient(...) as p:`.
# Cells run sequentially, so a mid-notebook exception would otherwise leak the
# AMQP connection. atexit guarantees close() runs on kernel teardown too.
def _close_producer():
    try:
        _producer.close()
    except Exception as _e:
        print(f"  WARNING: producer.close() raised: {_e}")
atexit.register(_close_producer)

# Idempotency key for this seed run. Stamped onto every pushed event as
# `seed_run_id` so downstream queries can detect / dedupe re-runs (e.g.
# vw_seeded_scenarios | summarize by seed_run_id).
SEED_RUN_ID = uuid.uuid4().hex
print(f"Eventstream producer ready. seed_run_id={SEED_RUN_ID}")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Resolve scenario identities from gold lakehouse ----------
# For each scenario:
#   - If the *_ID parameter is set, look it up in the gold dim (fail loud if missing).
#   - Otherwise auto-pick the first eligible row so the notebook still works
#     out-of-the-box.
# Labels (MARIA_LOPEZ, RAJ_SINGH, JOHN_PATIENT, BEAUMONT_RO) are demo aliases
# only -- they're what the presenter says aloud and what the SEED:* tags carry.
# The underlying patient/provider/facility is whatever you point the params at.

# Cap dim reads -- we only need a handful of rows for seeding. LIMIT keeps the
# driver pull bounded even on large production dims; _resolve_row() below
# verifies any requested IDs are present.
_DIM_CAP = 1000

df_patients = spark.sql(
    f"SELECT patient_id, first_name, last_name FROM lh_gold_curated.dim_patient "
    f"WHERE is_current = true LIMIT {_DIM_CAP}"
).toPandas()
df_providers = spark.sql(
    f"SELECT provider_id, display_name AS provider_name, specialty, facility_id "
    f"FROM lh_gold_curated.dim_provider WHERE is_current = true LIMIT {_DIM_CAP}"
).toPandas()
df_facilities = spark.sql(
    f"SELECT facility_id, facility_name, latitude, longitude "
    f"FROM lh_gold_curated.dim_facility LIMIT {_DIM_CAP}"
).toPandas()

if len(df_patients) < 2 or len(df_providers) < 1 or len(df_facilities) < 2:
    raise RuntimeError("Not enough dim rows to seed. Run Bronze/Silver/Gold first.")

def _resolve_row(df, id_col, requested_id, fallback_index, scope):
    """Return a row from df keyed by id_col. Uses requested_id when provided,
    else falls back to df.iloc[fallback_index]. Raises if requested_id is set
    but not present in the dim."""
    if requested_id:
        hit = df[df[id_col] == requested_id]
        if len(hit) == 0:
            raise RuntimeError(f"{scope}: requested {id_col}={requested_id!r} not found in gold dim.")
        return hit.iloc[0]
    if len(df) <= fallback_index:
        raise RuntimeError(f"{scope}: dim has only {len(df)} rows; need index {fallback_index}.")
    return df.iloc[fallback_index]

# Scenario 1 -- readmit patient (auto = first patient)
readmit_patient = _resolve_row(df_patients,   "patient_id",  SEED_READMIT_PATIENT_ID,  0, "readmit scenario")
# Scenario 3 -- cross-stream patient (auto = second patient, distinct from #1)
cross_patient   = _resolve_row(df_patients,   "patient_id",  SEED_CROSS_PATIENT_ID,    1, "cross-stream scenario")
# Scenario 2 -- fraud provider (auto = first provider)
fraud_provider  = _resolve_row(df_providers,  "provider_id", SEED_FRAUD_PROVIDER_ID,   0, "fraud scenario")
# Secondary provider for the cross-stream patient's claim (auto = 2nd if available, else 1st)
other_provider  = df_providers.iloc[1] if len(df_providers) >= 2 else fraud_provider

# Two distinct facilities for fraud velocity (geo-anomaly flag needs 2 sites)
fac_a = df_facilities.iloc[0]
fac_b = df_facilities.iloc[1] if len(df_facilities) >= 2 else fac_a

# Scenario 4 -- capacity facility. If ID provided, use it; else try name hint;
# else fall back to fac_a.
if SEED_CAPACITY_FACILITY_ID:
    capacity_facility = _resolve_row(df_facilities, "facility_id", SEED_CAPACITY_FACILITY_ID, 0, "capacity scenario")
else:
    hint_match = df_facilities[df_facilities["facility_name"].str.contains(SEED_CAPACITY_NAME_HINT or "", case=False, na=False)]
    capacity_facility = hint_match.iloc[0] if len(hint_match) > 0 else fac_a

LABELS = {
    SEED_READMIT_LABEL:  readmit_patient["patient_id"],
    SEED_CROSS_LABEL:    cross_patient["patient_id"],
    SEED_FRAUD_LABEL:    fraud_provider["provider_id"],
    SEED_CAPACITY_LABEL: capacity_facility["facility_id"],
}
print("Seed identity mapping (label -> real dim id):")
for k, v in LABELS.items():
    print(f"  SEED:{k:20s} -> {v}")
print(f"  Fraud claim facilities: {fac_a['facility_id']} + {fac_b['facility_id']}")
print(f"  Cross-stream claim provider: {other_provider['provider_id']}")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Push helper (same shape as NB_RTI_Event_Simulator) ----------
def push(records, table_name):
    """Push a list of dicts to Eventstream, tagged with _table for the update
    policy and stamped with seed_run_id for idempotent re-runs."""
    if not records:
        return
    batch = _producer.create_batch()
    for record in records:
        record["_table"] = table_name
        record["seed_run_id"] = SEED_RUN_ID
        for k, v in list(record.items()):
            if hasattr(v, "isoformat"):
                record[k] = v.isoformat()
        try:
            batch.add(EventData(json.dumps(record)))
        except ValueError:
            _producer.send_batch(batch)
            batch = _producer.create_batch()
            batch.add(EventData(json.dumps(record)))
    _producer.send_batch(batch)
    print(f"  Pushed {len(records)} -> {table_name}")

# Timezone-aware UTC (datetime.utcnow() is deprecated in Py 3.12+).
# Strip tzinfo before serialization so the resulting ISO string is naive UTC --
# matches what NB_RTI_Event_Simulator emits and what Eventhouse's datetime
# parser expects.
now = datetime.now(timezone.utc).replace(tzinfo=None)

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Scenario 1: 5d post-discharge readmit ----------
# Fires Activator Rule 4 (readmission_risk_score >= 70 OR days <= 7 + score >= 50)
# and lights up vw_all_alerts() READMISSION row.
_readmit_enc_id = f"SEED-{SEED_READMIT_LABEL}-001"
readmit_event = [{
    "event_id":              str(uuid.uuid4()),
    "event_timestamp":       now,
    "event_type":            "READMISSION_DETECTED",
    "patient_id":            LABELS[SEED_READMIT_LABEL],
    "provider_id":           fraud_provider["provider_id"],
    "facility_id":           capacity_facility["facility_id"],
    "current_encounter_id":  _readmit_enc_id,
    "prior_discharge_date":  (now - timedelta(days=int(SEED_READMIT_DAYS))).isoformat(),
    "days_since_discharge":  int(SEED_READMIT_DAYS),
    "current_diagnosis":     SEED_READMIT_DX,
    "prior_diagnosis":       SEED_READMIT_DX,
    "readmission_risk_score": float(SEED_READMIT_SCORE),
    "drg_code":              SEED_READMIT_DRG,
    "latitude":              float(capacity_facility.get("latitude") or 42.49),
    "longitude":             float(capacity_facility.get("longitude") or -83.14),
}]
push(readmit_event, "readmission_events")

# Companion ADT ADMIT so the agent can join readmission to admit context
readmit_adt = [{
    "event_id":          str(uuid.uuid4()),
    "event_timestamp":   now,
    "event_type":        "ADMIT",
    "patient_id":        LABELS[SEED_READMIT_LABEL],
    "facility_id":       capacity_facility["facility_id"],
    "facility_name":     str(capacity_facility.get("facility_name") or SEED_CAPACITY_LABEL),
    "admission_type":    "EMERGENCY",
    "primary_diagnosis": SEED_READMIT_DX,
    "latitude":          float(capacity_facility.get("latitude") or 42.49),
    "longitude":         float(capacity_facility.get("longitude") or -83.14),
    "has_open_care_gaps": False,
    "open_gap_measures":  "",
}]
push(readmit_adt, "adt_events")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Scenario 2: provider fraud velocity -- N high-amount claims, 2 facilities ----------
# velocity + amount + geo anomaly all fire -> fraud_score >= 80 after scorer runs
_fraud_n = int(SEED_FRAUD_CLAIM_COUNT)
_fraud_pool = df_patients[df_patients["patient_id"] != LABELS[SEED_READMIT_LABEL]].reset_index(drop=True)
if len(_fraud_pool) == 0:
    _fraud_pool = df_patients
fraud_claims = []
for i in range(_fraud_n):
    fac = fac_a if i % 2 == 0 else fac_b
    fraud_claims.append({
        "event_id":            str(uuid.uuid4()),
        "event_timestamp":     now - timedelta(seconds=60 * i),
        "event_type":          "CLAIM_SUBMITTED",
        "claim_id":            f"SEED-{SEED_FRAUD_LABEL}-{i:03d}",
        "patient_id":          _fraud_pool.iloc[i % len(_fraud_pool)]["patient_id"],
        "provider_id":         LABELS[SEED_FRAUD_LABEL],
        "facility_id":         fac["facility_id"],
        "payer_id":            "PAY-001",
        "diagnosis_code":      "Z00.00",
        "procedure_code":      SEED_FRAUD_PROC_CODE,
        "claim_type":          "professional",
        "claim_amount":        float(SEED_FRAUD_BASE_AMOUNT) + (i * 250.0),
        "latitude":            float(fac.get("latitude") or 42.49),
        "longitude":           float(fac.get("longitude") or -83.14),
        "injected_fraud_flags": f"SEED:{SEED_FRAUD_LABEL}|velocity|amount_outlier|geo_anomaly|upcoding",
    })
push(fraud_claims, "claims_events")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Scenario 3: cross-stream patient (care gap + high-cost + claim) ----------
# Will surface on vw_cross_stream_patients() as CRITICAL (>=3 streams).

cross_claim = [{
    "event_id":            str(uuid.uuid4()),
    "event_timestamp":     now,
    "event_type":          "CLAIM_SUBMITTED",
    "claim_id":            f"SEED-{SEED_CROSS_LABEL}-001",
    "patient_id":          LABELS[SEED_CROSS_LABEL],
    "provider_id":         other_provider["provider_id"],
    "facility_id":         fac_a["facility_id"],
    "payer_id":            "PAY-002",
    "diagnosis_code":      "E11.9",   # Type 2 diabetes
    "procedure_code":      "99214",
    "claim_type":          "professional",
    "claim_amount":        850.0,
    "latitude":            float(fac_a.get("latitude") or 42.49),
    "longitude":           float(fac_a.get("longitude") or -83.14),
    "injected_fraud_flags": f"SEED:{SEED_CROSS_LABEL}",
}]
push(cross_claim, "claims_events")

cross_care_gap = [{
    "alert_id":          f"SEED-{SEED_CROSS_LABEL}-CGAP-001",
    "alert_timestamp":   now,
    "patient_id":        LABELS[SEED_CROSS_LABEL],
    "facility_id":       fac_a["facility_id"],
    "facility_name":     str(fac_a.get("facility_name") or "Facility A"),
    "measure_id":        SEED_CROSS_GAP_MEASURE,
    "measure_name":      f"{SEED_CROSS_GAP_MEASURE} screening",
    "gap_days_overdue":  int(SEED_CROSS_GAP_DAYS),
    "alert_priority":    "CRITICAL",
    "alert_text":        f"{SEED_CROSS_GAP_MEASURE} overdue {int(SEED_CROSS_GAP_DAYS)} days for high-utilizer patient. Recommend in-visit order.",
    "latitude":          float(fac_a.get("latitude") or 42.49),
    "longitude":         float(fac_a.get("longitude") or -83.14),
}]
push(cross_care_gap, "care_gap_alerts")

cross_highcost = [{
    "alert_id":          f"SEED-{SEED_CROSS_LABEL}-HCOST-001",
    "alert_timestamp":   now,
    "patient_id":        LABELS[SEED_CROSS_LABEL],
    "rolling_spend_30d": float(SEED_CROSS_SPEND_30D),
    "rolling_spend_90d": float(SEED_CROSS_SPEND_30D) * 2.27,
    "ed_visits_30d":     int(SEED_CROSS_ED_VISITS),
    "readmission_flag":  True,
    "risk_tier":         "CRITICAL",
    "cost_trend":        "INCREASING",
    "latitude":          float(fac_a.get("latitude") or 42.49),
    "longitude":         float(fac_a.get("longitude") or -83.14),
}]
push(cross_highcost, "highcost_alerts")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Scenario 4: facility capacity/staffing mismatch ----------
# Lights up ops_capacity_events + staffing_acuity_events. event_type starts with
# SEED_ so vw_seeded_scenarios() filters them in.

_cap_fac_name = str(capacity_facility.get("facility_name") or SEED_CAPACITY_LABEL)
_cap_lat = float(capacity_facility.get("latitude") or 42.49)
_cap_lon = float(capacity_facility.get("longitude") or -83.14)

capacity_events = [
    {
        "event_id":         str(uuid.uuid4()),
        "event_timestamp":  now,
        "event_type":       "SEED_BED_OCCUPANCY",
        "facility_id":      LABELS[SEED_CAPACITY_LABEL],
        "facility_name":    _cap_fac_name,
        "capacity_type":    "BED_OCCUPANCY",
        "current_value":    float(SEED_CAPACITY_BED_PCT),
        "threshold_value":  85.0,
        "utilization_pct":  float(SEED_CAPACITY_BED_PCT),
        "los_hours":        0.0,
        "ward":             "Med-Surg",
        "latitude":         _cap_lat,
        "longitude":        _cap_lon,
    },
    {
        "event_id":         str(uuid.uuid4()),
        "event_timestamp":  now,
        "event_type":       "SEED_ED_BOARDING",
        "facility_id":      LABELS[SEED_CAPACITY_LABEL],
        "facility_name":    _cap_fac_name,
        "capacity_type":    "ED_BOARDING",
        "current_value":    float(SEED_CAPACITY_ED_BOARDING),
        "threshold_value":  10.0,
        "utilization_pct":  0.0,
        "los_hours":        6.5,
        "ward":             "ED",
        "latitude":         _cap_lat,
        "longitude":        _cap_lon,
    },
]
push(capacity_events, "ops_capacity_events")

staffing_event = [{
    "event_id":         str(uuid.uuid4()),
    "event_timestamp":  now,
    "event_type":       "SEED_STAFFING_GAP",
    "facility_id":      LABELS[SEED_CAPACITY_LABEL],
    "ward":             "ICU",
    "nurse_count":      6,
    "patient_count":    18,
    "acuity_score":     4.2,
    "ratio_actual":     3.0,
    "ratio_target":     2.0,
    "staffing_gap":     3,
}]
push(staffing_event, "staffing_acuity_events")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Done ----------
# Explicit close in the happy path; atexit handler is the safety net for the
# exception path (and for kernel shutdown without re-running this cell).
_close_producer()

print(f"\nSeeded scenarios pushed (seed_run_id={SEED_RUN_ID}).")
print("Allow ~30s for update policies to materialize,")
print("then verify with:")
print("  Healthcare_RTI_DB> vw_seeded_scenarios(2h)")
print("  Healthcare_RTI_DB> vw_cross_stream_patients(24h)")
print("\nDashboard: open Triage & Performance page.")
print("Operations Agent suggested prompts (substitute your label/ID):")
print(f"  - 'Show me readmission risk for patient {LABELS[SEED_READMIT_LABEL]} in the last hour.'")
print(f"  - 'Which providers have fraud_score above 50 in the last hour?  (watch for {LABELS[SEED_FRAUD_LABEL]})'")
print("  - 'Which patients appear in 2+ alert streams in the last 24 hours?'")
print(f"  - 'What is the current bed occupancy at facility {LABELS[SEED_CAPACITY_LABEL]}?'")
