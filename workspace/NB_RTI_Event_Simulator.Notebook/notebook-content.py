# Fabric notebook source

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# # RTI Event Simulator — Streaming Healthcare Events
# 
# Generates realistic **streaming** events for **7 RTI use cases**:
# 
# **Shared (3):**
# 1. **Claims Fraud Detection** — Claims submissions with amounts, diagnoses, geo
# 2. **Care Gap Closure** — ADT admit/discharge events triggering gap checks
# 3. **High-Cost Member Trajectory** — Claims + ED visits for rolling cost analysis
# 
# **Provider-Specific (2):**
# 4. **Readmission Detection** — Patient re-admitted within 30 days of discharge
# 5. **Provider Quality Drift** — Rolling quality metric snapshots vs benchmarks
# 
# **Payer-Specific (2):**
# 6. **Denial Adjudication** — Real-time claim approval/denial with reason + preventability
# 7. **AR Aging & Prior Auth** — AR snapshots per payer + auth lifecycle events
# 
# **How it works — Eventstream ingestion:**
# 
# ```
# This Notebook  ──► Eventstream Custom Endpoint (EventHub protocol)
#                         ├──► Eventhouse (rti_all_events → update policies → scoring)
#                         ├──► Lakehouse (lh_bronze_raw)  (raw archival)
#                         └──► Activator / Reflex   (fraud/care-gap alerts)
# ```
# 
# **Eventstream** is the primary ingestion path. Events are pushed to the
# Custom Endpoint using EventHub protocol. The Eventstream topology routes
# data to all destinations (Eventhouse, Lakehouse, Activator) automatically.
# 
# **Default lakehouse:** `lh_gold_curated`

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# NB_RTI_Event_Simulator
# ============================================================================
# Generates streaming healthcare events for 3 Fabric RTI use cases:
#   - Claims Fraud Detection (claims_events)
#   - Care Gap Closure (adt_events)
#   - High-Cost Member Trajectory (claims + ED events)
#
# Reads dimension/fact tables from lh_gold_curated, produces event batches.
# Primary path: Eventstream Custom Endpoint (EventHub protocol)
#   → Eventhouse + Lakehouse + Activator (all routed by Eventstream topology)
# Default lakehouse: lh_gold_curated
# ============================================================================

print("NB_RTI_Event_Simulator: Starting...")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Attach default lakehouse (self-healing) ----------
# When run via notebookutils.notebook.run(), the child notebook may not
# inherit the caller's lakehouse context. Discover and attach lh_gold_curated
# so spark.table("lh_gold_curated.xxx") resolves correctly.
import requests as _req
_ws_id = notebookutils.runtime.context.get("currentWorkspaceId", "")
_tok = notebookutils.credentials.getToken("pbi")
_hdr = {"Authorization": f"Bearer {_tok}"}
_lh_resp = _req.get(f"https://api.fabric.microsoft.com/v1/workspaces/{_ws_id}/lakehouses", headers=_hdr)
if _lh_resp.status_code == 200:
    for _lh in _lh_resp.json().get("value", []):
        if _lh["displayName"] == "lh_gold_curated":
            _lh_id = _lh["id"]
            _attached = False
            try:
                notebookutils.lakehouse.setDefaultLakehouse(_ws_id, _lh_id)
                print(f"  Attached lh_gold_curated ({_lh_id[:8]}...)")
                _attached = True
            except (AttributeError, Exception):
                pass
            if not _attached:
                import re as _re_mod
                _abfss = f"abfss://{_ws_id}@onelake.dfs.fabric.microsoft.com/{_lh_id}/Tables"
                _orig_sql = spark.sql
                def _patched_sql(query, _base=_abfss, _orig=_orig_sql):
                    query = _re_mod.sub(
                        r'\blh_gold_curated\.(\w+)\b',
                        lambda m: f'delta.`{_base}/{m.group(1)}`',
                        query
                    )
                    return _orig(query)
                spark.sql = _patched_sql
                # Also patch saveAsTable for DataFrame writes
                from pyspark.sql import DataFrameWriter as _DFW
                _orig_sat = _DFW.saveAsTable
                def _patched_sat(self, name, _base=_abfss, _orig=_orig_sat, **kwargs):
                    if name.startswith('lh_gold_curated.'):
                        tbl = name.split('.', 1)[1]
                        self.save(f'{_base}/{tbl}')
                        return
                    return _orig(self, name, **kwargs)
                _DFW.saveAsTable = _patched_sat
                # Also patch spark.table() for reading
                _orig_table = spark.table
                def _patched_table(name, _base=_abfss, _orig=_orig_table):
                    if name.startswith('lh_gold_curated.'):
                        tbl = name.split('.', 1)[1]
                        return spark.read.format('delta').load(f'{_base}/{tbl}')
                    return _orig(name)
                spark.table = _patched_table
                print(f"  Registered lh_gold_curated via ABFSS path rewriter ({_lh_id[:8]}...)")
                _attached = True
            if not _attached:
                print(f"  WARNING: Could not attach lh_gold_curated ({_lh_id[:8]}...)")
                print(f"  Lakehouse methods: {[m for m in dir(notebookutils.lakehouse) if not m.startswith('_')]}")
            break
    else:
        print("  WARNING: lh_gold_curated not found in workspace")
else:
    print(f"  WARNING: Could not list lakehouses (HTTP {_lh_resp.status_code})")
del _req, _ws_id, _tok, _hdr, _lh_resp

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

%pip install azure-eventhub azure-core>=1.31.0 --quiet

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Parameters (override from pipeline or %run) ----------
BATCH_SIZE = 500         # events per streaming batch
STREAM_INTERVAL_SEC = 5  # seconds between batches
STREAM_BATCHES = STREAM_BATCHES if "STREAM_BATCHES" in dir() and STREAM_BATCHES else 10

# ┌─────────────────────────────────────────────────────────────────┐
# │  PASTE YOUR EVENTSTREAM CONNECTION STRING BELOW                │
# │                                                                │
# │  1. Open Healthcare_RTI_Eventstream in the Fabric portal       │
# │  2. Click 'HealthcareCustomEndpoint' source node               │
# │  3. Copy the Connection String → paste below                   │
# │                                                                │
# │  Or run PL_Healthcare_RTI pipeline with ES_CONNECTION_STRING   │
# │  parameter — it passes the value to this notebook.             │
# └─────────────────────────────────────────────────────────────────┘
ES_CONNECTION_STRING = ES_CONNECTION_STRING if "ES_CONNECTION_STRING" in dir() and ES_CONNECTION_STRING else ""

# METADATA **{"language":"python","tags":["parameters"]}**

# CELL **{"language":"python"}**

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid
import json
import requests

np.random.seed(None)  # Truly random for each run

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Connect to Eventstream (primary) + KQL Eventhouse (verification) ----------
#
# Primary:   Eventstream Custom Endpoint → Eventhouse + Lakehouse + Activator
# KQL query: Used only to verify data landed in Eventhouse after streaming

WORKSPACE_ID = notebookutils.runtime.context.get("currentWorkspaceId", "")

# ── Primary path: Eventstream Custom Endpoint ──
_es_producer = None
if ES_CONNECTION_STRING:
    try:
        from azure.eventhub import EventHubProducerClient, EventData
        _es_producer = EventHubProducerClient.from_connection_string(ES_CONNECTION_STRING)
        print(f"  Eventstream: Connected (primary ingestion path)")
        print(f"    → Eventhouse (rti_all_events → update policies → scoring)")
        print(f"    → Lakehouse (raw archival)")
        print(f"    → Activator (real-time alerts)")
    except Exception as _es_err:
        print(f"  Eventstream ERROR: Could not connect -- {_es_err}")
else:
    print("  ERROR: No ES_CONNECTION_STRING provided.")
    print("  Copy from Healthcare_RTI_Eventstream → HealthcareCustomEndpoint → Connection String")

# ── KQL query path (verification only — not used for ingestion) ──
_KUSTO_QUERY_URI = None
_KQL_DB_NAME = "Healthcare_RTI_DB"
_fabric_tok = notebookutils.credentials.getToken("pbi")
_hdr = {"Authorization": f"Bearer {_fabric_tok}", "Content-Type": "application/json"}

_eh_resp = requests.get(
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items?type=Eventhouse",
    headers=_hdr
)
if _eh_resp.status_code == 200:
    for _item in _eh_resp.json().get("value", []):
        if "Healthcare" in _item.get("displayName", ""):
            _props_resp = requests.get(
                f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/eventhouses/{_item['id']}",
                headers=_hdr
            )
            if _props_resp.status_code == 200:
                _props = _props_resp.json().get("properties", _props_resp.json())
                _KUSTO_QUERY_URI = _props.get("queryServiceUri", "")
            break

if _KUSTO_QUERY_URI:
    print(f"  KQL verify: {_KUSTO_QUERY_URI} (query-only, not for ingestion)")

if not _es_producer:
    print("="*60)
    print("  ERROR: Eventstream is not available.")
    print("  Set ES_CONNECTION_STRING and re-run this notebook.")
    print("="*60)

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- KQL Query (verification only) ----------
# Used after streaming to confirm data landed in Eventhouse via Eventstream.

def _get_kusto_token():
    """Get a fresh Kusto token (tokens expire, so refresh per batch)."""
    return notebookutils.credentials.getToken("kusto")

def _kql_query(query, token=None):
    """Execute a KQL query via REST API. Returns rows list or []."""
    if not _KUSTO_QUERY_URI:
        return []
    tok = token or _get_kusto_token()
    headers = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}
    resp = requests.post(
        f"{_KUSTO_QUERY_URI}/v1/rest/query",
        headers=headers,
        json={"db": _KQL_DB_NAME, "csl": query},
    )
    if resp.status_code != 200:
        return []
    data = resp.json()
    if data.get("Tables") and data["Tables"][0].get("Rows"):
        return data["Tables"][0]["Rows"]
    return []

# ---------- Eventstream (primary ingestion path) ----------

def push_to_eventstream(df_pandas, table_name):
    """Push events to Eventstream Custom Endpoint → Eventhouse + Lakehouse + Activator."""
    if _es_producer is None:
        return False
    try:
        records = df_pandas.to_dict(orient="records")
        batch = _es_producer.create_batch()
        for record in records:
            record["_table"] = table_name
            for k, v in record.items():
                if hasattr(v, 'isoformat'):
                    record[k] = v.isoformat()
            event = EventData(json.dumps(record))
            try:
                batch.add(event)
            except ValueError:
                _es_producer.send_batch(batch)
                batch = _es_producer.create_batch()
                batch.add(event)
        _es_producer.send_batch(batch)
        print(f"  Eventstream: {len(df_pandas)} events -> {table_name}")
        return True
    except Exception as e:
        print(f"  Eventstream ERROR: {table_name} push failed: {e}")
        return False

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Load dimension/fact data from Gold lakehouse ----------
print("Loading dimension tables from lh_gold_curated...")

df_patients = spark.sql("SELECT patient_id, first_name, last_name, gender, date_of_birth, zip_code FROM lh_gold_curated.dim_patient WHERE is_current = true").toPandas()
df_providers = spark.sql("SELECT provider_id, display_name AS provider_name, specialty, facility_id FROM lh_gold_curated.dim_provider WHERE is_current = true").toPandas()
df_facilities = spark.sql("SELECT facility_id, facility_name, facility_type, latitude, longitude FROM lh_gold_curated.dim_facility").toPandas()
df_diagnoses = spark.sql("SELECT DISTINCT icd_code AS diagnosis_code, icd_description AS diagnosis_description FROM lh_gold_curated.dim_diagnosis").toPandas()
df_medications = spark.sql("SELECT DISTINCT rxnorm_code AS medication_code, medication_name, drug_class FROM lh_gold_curated.dim_medication").toPandas()
df_payers = spark.sql("SELECT payer_id, payer_name, payer_type AS plan_type FROM lh_gold_curated.dim_payer").toPandas()

try:
    df_care_gaps = spark.sql("SELECT patient_id, measure_id, measure_name, is_gap_open, gap_days_overdue FROM lh_gold_curated.care_gaps WHERE is_gap_open = true").toPandas()
    print(f"  Loaded {len(df_care_gaps)} open care gaps")
except Exception:
    df_care_gaps = pd.DataFrame(columns=["patient_id", "measure_id", "measure_name", "is_gap_open", "gap_days_overdue"])
    print("  No care_gaps table found -- care gap alerts will be empty")

print(f"  Patients: {len(df_patients)}, Providers: {len(df_providers)}, Facilities: {len(df_facilities)}")
print(f"  Diagnoses: {len(df_diagnoses)}, Medications: {len(df_medications)}, Payers: {len(df_payers)}")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Reference data for realistic event generation ----------

CLAIM_TYPES = ["professional", "institutional", "pharmacy"]
ADT_EVENT_TYPES = ["ADMIT", "DISCHARGE", "TRANSFER", "OBSERVATION"]
ADMISSION_TYPES = ["EMERGENCY", "URGENT", "ELECTIVE", "NEWBORN"]

PROCEDURE_CODES = {
    "Cardiology": ["99213", "99214", "93000", "93306", "93458"],
    "Orthopedics": ["99213", "99214", "27447", "27130", "29881"],
    "Oncology": ["99214", "99215", "96413", "77386", "88305"],
    "Primary Care": ["99213", "99214", "99215", "99396", "90471"],
    "Neurology": ["99213", "99214", "95819", "70553", "95910"],
    "Emergency Medicine": ["99281", "99282", "99283", "99284", "99285"],
    "Pediatrics": ["99213", "99214", "99392", "99393", "90471"],
    "Internal Medicine": ["99213", "99214", "99215", "99396", "80053"],
}

FRAUD_PATTERNS = {
    "velocity": 0.03,
    "geo_anomaly": 0.02,
    "amount_outlier": 0.04,
    "upcoding": 0.03,
}

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Event Generator Functions
# ============================================================================

def generate_claims_events(n: int) -> pd.DataFrame:
    """Generate n claims submission events with controlled fraud pattern injection."""
    events = []
    now = datetime.utcnow()

    fraud_velocity_providers = set(
        df_providers.sample(max(1, int(len(df_providers) * FRAUD_PATTERNS["velocity"])))["provider_id"]
    )

    for i in range(n):
        patient = df_patients.sample(1).iloc[0]
        provider = df_providers.sample(1).iloc[0]
        payer = df_payers.sample(1).iloc[0]

        fac = df_facilities[df_facilities["facility_id"] == provider["facility_id"]]
        lat = float(fac["latitude"].iloc[0]) if len(fac) > 0 and pd.notna(fac["latitude"].iloc[0]) else 42.96
        lon = float(fac["longitude"].iloc[0]) if len(fac) > 0 and pd.notna(fac["longitude"].iloc[0]) else -85.67

        diag = df_diagnoses.sample(1).iloc[0] if len(df_diagnoses) > 0 else {"diagnosis_code": "Z00.00", "diagnosis_description": "General exam"}

        specialty = provider.get("specialty", "Primary Care")
        proc_codes = PROCEDURE_CODES.get(specialty, PROCEDURE_CODES["Primary Care"])
        proc_code = random.choice(proc_codes)

        claim_type = random.choice(CLAIM_TYPES)
        if claim_type == "institutional":
            base_amount = round(random.gauss(8500, 3000), 2)
        elif claim_type == "pharmacy":
            base_amount = round(random.gauss(250, 150), 2)
        else:
            base_amount = round(random.gauss(350, 200), 2)
        base_amount = max(25.0, base_amount)

        fraud_flags = []

        if provider["provider_id"] in fraud_velocity_providers and random.random() < 0.5:
            event_ts = now - timedelta(minutes=random.randint(0, 60))
            fraud_flags.append("velocity_burst")
        else:
            event_ts = now - timedelta(minutes=random.randint(0, 1440))

        if random.random() < FRAUD_PATTERNS["amount_outlier"]:
            base_amount *= random.uniform(3.0, 8.0)
            fraud_flags.append("amount_outlier")

        if random.random() < FRAUD_PATTERNS["upcoding"]:
            proc_code = "99215"
            fraud_flags.append("upcoding")

        if random.random() < FRAUD_PATTERNS["geo_anomaly"]:
            lat += random.uniform(3, 8) * random.choice([-1, 1])
            lon += random.uniform(3, 8) * random.choice([-1, 1])
            fraud_flags.append("geo_anomaly")

        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": event_ts.isoformat(),
            "event_type": "CLAIM_SUBMITTED",
            "claim_id": f"CLM-{uuid.uuid4().hex[:10].upper()}",
            "patient_id": patient["patient_id"],
            "provider_id": provider["provider_id"],
            "facility_id": provider.get("facility_id", ""),
            "payer_id": payer["payer_id"],
            "diagnosis_code": diag["diagnosis_code"] if isinstance(diag, dict) else diag.get("diagnosis_code", "Z00.00"),
            "procedure_code": proc_code,
            "claim_type": claim_type,
            "claim_amount": round(base_amount, 2),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "injected_fraud_flags": "|".join(fraud_flags) if fraud_flags else "",
        })

    return pd.DataFrame(events)


def generate_adt_events(n: int) -> pd.DataFrame:
    """Generate n ADT events for Care Gap Closure use case."""
    events = []
    now = datetime.utcnow()

    for i in range(n):
        patient = df_patients.sample(1).iloc[0]
        facility = df_facilities.sample(1).iloc[0]

        event_type = random.choices(ADT_EVENT_TYPES, weights=[0.35, 0.35, 0.15, 0.15], k=1)[0]
        admission_type = random.choices(ADMISSION_TYPES, weights=[0.30, 0.25, 0.35, 0.10], k=1)[0]

        diag = df_diagnoses.sample(1).iloc[0] if len(df_diagnoses) > 0 else {"diagnosis_code": "Z00.00", "diagnosis_description": "General exam"}

        lat = float(facility["latitude"]) if pd.notna(facility.get("latitude")) else 42.96
        lon = float(facility["longitude"]) if pd.notna(facility.get("longitude")) else -85.67

        patient_gaps = df_care_gaps[df_care_gaps["patient_id"] == patient["patient_id"]]
        has_open_gaps = len(patient_gaps) > 0
        open_gap_measures = "|".join(patient_gaps["measure_id"].tolist()) if has_open_gaps else ""

        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": (now - timedelta(minutes=random.randint(0, 1440))).isoformat(),
            "event_type": event_type,
            "patient_id": patient["patient_id"],
            "facility_id": facility["facility_id"],
            "facility_name": facility.get("facility_name", ""),
            "admission_type": admission_type,
            "primary_diagnosis": diag["diagnosis_code"] if isinstance(diag, dict) else diag.get("diagnosis_code", "Z00.00"),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "has_open_care_gaps": has_open_gaps,
            "open_gap_measures": open_gap_measures,
        })

    return pd.DataFrame(events)


def generate_rx_events(n: int) -> pd.DataFrame:
    """Generate n prescription fill events."""
    events = []
    now = datetime.utcnow()

    for i in range(n):
        patient = df_patients.sample(1).iloc[0]
        provider = df_providers.sample(1).iloc[0]
        med = df_medications.sample(1).iloc[0] if len(df_medications) > 0 else {"medication_code": "RX0001", "medication_name": "Generic Med", "drug_class": "Other"}

        fac = df_facilities[df_facilities["facility_id"] == provider["facility_id"]]
        lat = float(fac["latitude"].iloc[0]) if len(fac) > 0 and pd.notna(fac["latitude"].iloc[0]) else 42.96
        lon = float(fac["longitude"].iloc[0]) if len(fac) > 0 and pd.notna(fac["longitude"].iloc[0]) else -85.67

        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": (now - timedelta(minutes=random.randint(0, 1440))).isoformat(),
            "event_type": "RX_FILL",
            "patient_id": patient["patient_id"],
            "provider_id": provider["provider_id"],
            "medication_code": med["medication_code"] if isinstance(med, dict) else med.get("medication_code", "RX0001"),
            "medication_name": med["medication_name"] if isinstance(med, dict) else med.get("medication_name", "Generic"),
            "drug_class": med.get("drug_class", "Other") if isinstance(med, dict) else "Other",
            "quantity": random.choice([30, 60, 90]),
            "days_supply": random.choice([30, 60, 90]),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
        })

    return pd.DataFrame(events)

print("Event generators ready (original 3).")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Provider-Specific Event Generators
# ============================================================================

QUALITY_METRICS = [
    {"name": "readmission_rate_30d", "benchmark": 0.12, "direction": "lower_is_better"},
    {"name": "avg_length_of_stay", "benchmark": 4.5, "direction": "lower_is_better"},
    {"name": "patient_satisfaction", "benchmark": 0.85, "direction": "higher_is_better"},
    {"name": "documentation_score", "benchmark": 0.90, "direction": "higher_is_better"},
    {"name": "preventable_complication_rate", "benchmark": 0.05, "direction": "lower_is_better"},
    {"name": "medication_reconciliation_rate", "benchmark": 0.95, "direction": "higher_is_better"},
]

DRG_CODES = [
    ("291", "Heart Failure & Shock"),
    ("470", "Major Hip & Knee Joint Replacement"),
    ("871", "Septicemia or Severe Sepsis"),
    ("690", "Kidney & UTI"),
    ("392", "Esophagitis & Misc Digestive"),
    ("683", "Renal Failure"),
    ("194", "Simple Pneumonia & Pleurisy"),
    ("065", "Intracranial Hemorrhage or Cerebral Infarction"),
]


def generate_readmission_events(n: int) -> pd.DataFrame:
    """Generate readmission detection events — patient re-admitted within 30 days."""
    events = []
    now = datetime.utcnow()

    for i in range(n):
        patient = df_patients.sample(1).iloc[0]
        provider = df_providers.sample(1).iloc[0]

        fac = df_facilities[df_facilities["facility_id"] == provider["facility_id"]]
        lat = float(fac["latitude"].iloc[0]) if len(fac) > 0 and pd.notna(fac["latitude"].iloc[0]) else 40.84
        lon = float(fac["longitude"].iloc[0]) if len(fac) > 0 and pd.notna(fac["longitude"].iloc[0]) else -73.94

        days_since = random.choices(
            range(1, 31),
            weights=[max(1, 30 - d) for d in range(1, 31)],
            k=1
        )[0]

        drg = random.choice(DRG_CODES)
        diag = df_diagnoses.sample(1).iloc[0] if len(df_diagnoses) > 0 else {"diagnosis_code": "I50.9"}
        prior_diag = df_diagnoses.sample(1).iloc[0] if len(df_diagnoses) > 0 else {"diagnosis_code": "I50.9"}

        risk_score = round(random.gauss(45, 20), 1)
        risk_score = max(5, min(100, risk_score))

        # Higher risk for shorter readmission windows
        if days_since <= 7:
            risk_score = min(100, risk_score + 25)
        elif days_since <= 14:
            risk_score = min(100, risk_score + 10)

        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": (now - timedelta(minutes=random.randint(0, 1440))).isoformat(),
            "event_type": "READMISSION_DETECTED",
            "patient_id": patient["patient_id"],
            "provider_id": provider["provider_id"],
            "facility_id": provider.get("facility_id", ""),
            "current_encounter_id": f"ENC-{uuid.uuid4().hex[:10].upper()}",
            "prior_discharge_date": (now - timedelta(days=days_since)).isoformat(),
            "days_since_discharge": days_since,
            "current_diagnosis": diag.get("diagnosis_code", "I50.9") if isinstance(diag, dict) else diag["diagnosis_code"],
            "prior_diagnosis": prior_diag.get("diagnosis_code", "I50.9") if isinstance(prior_diag, dict) else prior_diag["diagnosis_code"],
            "readmission_risk_score": risk_score,
            "drg_code": drg[0],
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
        })

    return pd.DataFrame(events)


def generate_quality_metric_events(n: int) -> pd.DataFrame:
    """Generate provider quality metric snapshots — detect quality drift."""
    events = []
    now = datetime.utcnow()

    sampled_providers = df_providers.sample(min(n, len(df_providers)))
    for _, provider in sampled_providers.iterrows():
        for metric in random.sample(QUALITY_METRICS, k=random.randint(1, 3)):
            benchmark = metric["benchmark"]
            # Most providers near benchmark, some drifting
            if random.random() < 0.15:
                # Drifting provider — performance outside benchmark
                if metric["direction"] == "lower_is_better":
                    value = round(benchmark * random.uniform(1.3, 2.0), 4)
                else:
                    value = round(benchmark * random.uniform(0.6, 0.85), 4)
            else:
                if metric["direction"] == "lower_is_better":
                    value = round(benchmark * random.uniform(0.5, 1.15), 4)
                else:
                    value = round(benchmark * random.uniform(0.9, 1.1), 4)

            denom = random.randint(20, 200)
            numer = int(value * denom)

            events.append({
                "event_id": str(uuid.uuid4()),
                "event_timestamp": now.isoformat(),
                "event_type": "QUALITY_METRIC_SNAPSHOT",
                "provider_id": provider["provider_id"],
                "provider_name": provider.get("provider_name", provider.get("display_name", "")),
                "specialty": provider.get("specialty", ""),
                "facility_id": provider.get("facility_id", ""),
                "metric_name": metric["name"],
                "metric_value": round(value, 4),
                "benchmark_value": benchmark,
                "direction": metric["direction"],
                "rolling_window_days": 30,
                "denominator": denom,
                "numerator": numer,
            })

    return pd.DataFrame(events) if events else pd.DataFrame()


# ============================================================================
# Payer-Specific Event Generators
# ============================================================================

DENIAL_REASONS = [
    ("Prior Auth Required", True, "Submit prior authorization before procedure"),
    ("Not Medically Necessary", False, "Attach clinical documentation and appeal"),
    ("Duplicate Claim", True, "Check billing system for duplicate submissions"),
    ("Invalid Code", True, "Verify CPT/ICD codes and resubmit"),
    ("Coverage Expired", False, "Verify patient eligibility before service"),
    ("Out of Network", False, "Verify network status or obtain gap exception"),
    ("Missing Documentation", True, "Attach required clinical notes and resubmit"),
]

AUTH_STATUSES = ["SUBMITTED", "APPROVED", "DENIED", "EXPIRING_SOON", "EXPIRED"]


def generate_denial_adjudication_events(n: int) -> pd.DataFrame:
    """Generate claim adjudication events — real-time approval/denial decisions."""
    events = []
    now = datetime.utcnow()

    for i in range(n):
        patient = df_patients.sample(1).iloc[0]
        provider = df_providers.sample(1).iloc[0]
        payer = df_payers.sample(1).iloc[0]

        # 88% approved, 12% denied (slightly above 8% benchmark to show issues)
        if random.random() < 0.12:
            result = "DENIED"
            reason_tuple = random.choice(DENIAL_REASONS)
            denial_reason = reason_tuple[0]
            is_preventable = reason_tuple[1]
            recommended_action = reason_tuple[2]
            billed = round(random.gauss(2500, 1500), 2)
            billed = max(100, billed)
            allowed = 0.0
            paid = 0.0
        else:
            result = "APPROVED"
            denial_reason = ""
            is_preventable = False
            recommended_action = ""
            billed = round(random.gauss(2500, 1500), 2)
            billed = max(100, billed)
            allowed = round(billed * random.uniform(0.65, 0.95), 2)
            paid = round(allowed * random.uniform(0.85, 1.0), 2)

        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": (now - timedelta(minutes=random.randint(0, 1440))).isoformat(),
            "event_type": "CLAIM_ADJUDICATED",
            "claim_id": f"CLM-{uuid.uuid4().hex[:10].upper()}",
            "patient_id": patient["patient_id"],
            "provider_id": provider["provider_id"],
            "payer_id": payer["payer_id"],
            "payer_name": payer.get("payer_name", ""),
            "adjudication_result": result,
            "denial_reason": denial_reason,
            "billed_amount": billed,
            "allowed_amount": allowed,
            "paid_amount": paid,
            "is_preventable": is_preventable,
            "recommended_action": recommended_action,
        })

    return pd.DataFrame(events)


def generate_ar_snapshot_events() -> pd.DataFrame:
    """Generate AR aging snapshots — one per payer, periodic check."""
    events = []
    now = datetime.utcnow()

    for _, payer in df_payers.iterrows():
        open_claims = random.randint(50, 500)
        avg_days = round(random.gauss(38, 12), 1)
        avg_days = max(10, avg_days)
        prior_avg = avg_days + random.gauss(0, 5)

        if avg_days > prior_avg + 3:
            trend = "INCREASING"
        elif avg_days < prior_avg - 3:
            trend = "DECREASING"
        else:
            trend = "STABLE"

        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": now.isoformat(),
            "event_type": "AR_SNAPSHOT",
            "payer_id": payer["payer_id"],
            "payer_name": payer.get("payer_name", ""),
            "open_claims_count": open_claims,
            "total_ar_balance": round(open_claims * random.uniform(800, 3000), 2),
            "avg_days_in_ar": round(avg_days, 1),
            "claims_over_45_days": int(open_claims * max(0, (avg_days - 30) / 60)),
            "claims_over_90_days": int(open_claims * max(0, (avg_days - 60) / 120)),
            "net_collection_rate": round(random.uniform(0.88, 0.98), 4),
            "prior_period_avg_days": round(prior_avg, 1),
            "trend_direction": trend,
        })

    return pd.DataFrame(events)


def generate_auth_lifecycle_events(n: int) -> pd.DataFrame:
    """Generate prior auth lifecycle events — submissions, approvals, expirations."""
    events = []
    now = datetime.utcnow()

    for i in range(n):
        patient = df_patients.sample(1).iloc[0]
        provider = df_providers.sample(1).iloc[0]
        payer = df_payers.sample(1).iloc[0]

        specialty = provider.get("specialty", "Primary Care")
        proc_codes = PROCEDURE_CODES.get(specialty, PROCEDURE_CODES["Primary Care"])
        proc_code = random.choice(proc_codes)

        status = random.choices(
            AUTH_STATUSES,
            weights=[0.15, 0.40, 0.10, 0.25, 0.10],
            k=1
        )[0]

        if status == "EXPIRING_SOON":
            days_until = random.randint(1, 7)
        elif status == "EXPIRED":
            days_until = random.randint(-30, -1)
        elif status == "APPROVED":
            days_until = random.randint(8, 90)
        else:
            days_until = random.randint(14, 60)

        expiry_date = now + timedelta(days=days_until)
        sched_date = expiry_date - timedelta(days=random.randint(1, max(2, days_until)))

        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": (now - timedelta(minutes=random.randint(0, 480))).isoformat(),
            "event_type": f"AUTH_{status}",
            "auth_id": f"AUTH-{uuid.uuid4().hex[:8].upper()}",
            "patient_id": patient["patient_id"],
            "provider_id": provider["provider_id"],
            "payer_id": payer["payer_id"],
            "payer_name": payer.get("payer_name", ""),
            "procedure_code": proc_code,
            "procedure_description": f"Procedure {proc_code}",
            "auth_status": status,
            "auth_expiry_date": expiry_date.isoformat(),
            "days_until_expiry": days_until,
            "scheduled_procedure_date": sched_date.isoformat(),
            "estimated_charge": round(random.gauss(5000, 3000), 2),
        })

    return pd.DataFrame(events)


# ============================================================================
# Phase-2 generators — close CMO/CFO/COO/CTO gaps (8 new event types)
# ============================================================================
WARDS = ["ICU", "ED", "Med-Surg", "OR", "PICU", "L&D", "Cardiology", "Oncology"]
HAI_TYPES = ["CLABSI", "CAUTI", "SSI", "MRSA", "C-DIFF", "VAP"]
HAI_ORGANISMS = ["Staphylococcus aureus", "Escherichia coli", "Clostridioides difficile",
                 "Klebsiella pneumoniae", "Pseudomonas aeruginosa", "Enterococcus faecalis"]
CONTRACT_TYPES = ["FFS", "DRG", "CAP", "VALUE_BASED", "PER_DIEM"]
CAPACITY_TYPES = ["ED_BOARDING", "BED_OCCUPANCY", "OR_TURNOVER", "LOS_HOURS"]
DQ_RULE_TYPES = ["NULL_RATE", "SCHEMA_DRIFT", "FRESHNESS", "VOLUME_ANOMALY", "REFERENTIAL"]
DQ_SOURCE_TABLES = ["claims_events", "adt_events", "rx_events", "denial_adjudication_events", "ar_snapshot_events"]
ACTION_TYPES = ["READ", "EXPORT", "SHARE", "DELETE", "QUERY"]
GEO_COUNTRIES = ["US", "US", "US", "US", "US", "CA", "MX", "RU", "CN", "BR"]  # weighted to US
MODELS = [("fraud_classifier", "v3.2"), ("readmit_risk", "v2.1"), ("denial_predictor", "v1.4")]
DETERIORATION_TYPES = ["SEPSIS", "RESPIRATORY", "CARDIAC", "NEURO", "MEWS_BREACH"]


def generate_clinical_deterioration_events(n: int) -> pd.DataFrame:
    """CMO: NEWS2/SIRS/MEWS scoring spikes — sub-minute clinical alerts."""
    events = []
    now = datetime.utcnow()
    for _ in range(n):
        patient = df_patients.sample(1).iloc[0]
        facility = df_facilities.sample(1).iloc[0]
        det_type = random.choice(DETERIORATION_TYPES)
        # NEWS2 0–20 scale; >=5 escalate, >=7 critical
        news2 = round(max(0, random.gauss(4.5, 2.0)), 1)
        sirs = round(max(0, min(4, random.gauss(2.0, 1.0))), 1)
        mews = round(max(0, random.gauss(3.0, 1.5)), 1)
        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": (now - timedelta(seconds=random.randint(0, 600))).isoformat(),
            "event_type": "CLINICAL_DETERIORATION",
            "patient_id": patient["patient_id"],
            "encounter_id": f"ENC-{uuid.uuid4().hex[:10].upper()}",
            "facility_id": facility["facility_id"],
            "ward": random.choice(WARDS),
            "news2_score": news2,
            "sirs_score": sirs,
            "mews_score": mews,
            "deterioration_type": det_type,
            "requires_rrt": news2 >= 7 or det_type == "SEPSIS" and sirs >= 2,
            "primary_diagnosis": random.choice(df_diagnoses["diagnosis_code"].tolist()) if len(df_diagnoses) else "",
            "latitude": float(facility.get("latitude", 0) or 0),
            "longitude": float(facility.get("longitude", 0) or 0),
        })
    return pd.DataFrame(events)


def generate_mortality_hai_events(n: int) -> pd.DataFrame:
    """CMO: HAI cluster surveillance — infection counts per ward."""
    events = []
    now = datetime.utcnow()
    for _ in range(n):
        facility = df_facilities.sample(1).iloc[0]
        cluster_size = random.choices([0, 1, 2, 3, 4, 6], weights=[0.55, 0.20, 0.10, 0.08, 0.05, 0.02], k=1)[0]
        sev = "CRITICAL" if cluster_size >= 5 else ("HIGH" if cluster_size >= 3 else ("MEDIUM" if cluster_size >= 1 else "LOW"))
        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": (now - timedelta(minutes=random.randint(0, 720))).isoformat(),
            "event_type": "HAI_SURVEILLANCE",
            "facility_id": facility["facility_id"],
            "ward": random.choice(WARDS),
            "hai_type": random.choice(HAI_TYPES),
            "organism": random.choice(HAI_ORGANISMS),
            "cluster_size": cluster_size,
            "infection_count_72h": cluster_size + random.randint(0, 4),
            "severity": sev,
            "latitude": float(facility.get("latitude", 0) or 0),
            "longitude": float(facility.get("longitude", 0) or 0),
        })
    return pd.DataFrame(events)


def generate_net_revenue_variance_events(n: int) -> pd.DataFrame:
    """CFO: expected vs paid by contract — underpayment detection."""
    events = []
    now = datetime.utcnow()
    for _ in range(n):
        payer = df_payers.sample(1).iloc[0]
        provider = df_providers.sample(1).iloc[0]
        contract_type = random.choice(CONTRACT_TYPES)
        expected = round(random.uniform(500, 25000), 2)
        # ~70% paid as expected (within 5%), 20% slightly off, 10% underpaid >10%
        roll = random.random()
        if roll < 0.70:
            actual = expected * random.uniform(0.97, 1.02)
        elif roll < 0.90:
            actual = expected * random.uniform(0.90, 0.97)
        else:
            actual = expected * random.uniform(0.60, 0.88)
        variance_dollars = round(actual - expected, 2)
        variance_pct = round(((actual - expected) / expected) * 100, 2) if expected else 0.0
        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": (now - timedelta(minutes=random.randint(0, 360))).isoformat(),
            "event_type": "NET_REVENUE_VARIANCE",
            "claim_id": f"CLM-{uuid.uuid4().hex[:10].upper()}",
            "payer_id": payer["payer_id"],
            "payer_name": payer.get("payer_name", ""),
            "contract_id": f"CTR-{payer['payer_id']}-{contract_type}",
            "contract_type": contract_type,
            "expected_payment": expected,
            "actual_payment": round(actual, 2),
            "variance_pct": variance_pct,
            "variance_dollars": variance_dollars,
            "provider_id": provider["provider_id"],
            "facility_id": provider.get("facility_id", "") or "",
        })
    return pd.DataFrame(events)


def generate_ops_capacity_events(n: int) -> pd.DataFrame:
    """COO: ED throughput, bed occupancy, OR turnover, LOS — operational pulse."""
    events = []
    now = datetime.utcnow()
    for _ in range(n):
        facility = df_facilities.sample(1).iloc[0]
        ctype = random.choice(CAPACITY_TYPES)
        if ctype == "ED_BOARDING":
            current, threshold = round(random.gauss(8, 6), 0), 10.0
            los = round(max(0.5, random.gauss(4.0, 2.5)), 1)
        elif ctype == "BED_OCCUPANCY":
            current, threshold = round(random.gauss(78, 12), 1), 85.0
            los = 0.0
        elif ctype == "OR_TURNOVER":
            current, threshold = round(random.gauss(38, 15), 0), 45.0
            los = 0.0
        else:  # LOS_HOURS
            current, threshold = round(random.gauss(72, 24), 1), 96.0
            los = current
        current = max(0, current)
        util = round((current / threshold * 100) if threshold else 0, 1)
        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": (now - timedelta(minutes=random.randint(0, 60))).isoformat(),
            "event_type": f"CAPACITY_{ctype}",
            "facility_id": facility["facility_id"],
            "facility_name": facility.get("facility_name", ""),
            "capacity_type": ctype,
            "current_value": float(current),
            "threshold_value": threshold,
            "utilization_pct": util,
            "los_hours": float(los),
            "ward": random.choice(WARDS),
            "latitude": float(facility.get("latitude", 0) or 0),
            "longitude": float(facility.get("longitude", 0) or 0),
        })
    return pd.DataFrame(events)


def generate_staffing_acuity_events(n: int) -> pd.DataFrame:
    """COO: nurse-to-acuity ratio — staffing gap detection."""
    events = []
    now = datetime.utcnow()
    for _ in range(n):
        facility = df_facilities.sample(1).iloc[0]
        nurses = max(2, int(random.gauss(8, 3)))
        patients = max(nurses, int(random.gauss(nurses * 4, 4)))
        acuity = round(random.uniform(1.5, 4.5), 2)
        ratio_actual = round(patients / nurses, 2)
        ratio_target = 4.0 if acuity < 3.0 else 3.0
        gap = max(0, int((patients / ratio_target) - nurses))
        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": (now - timedelta(minutes=random.randint(0, 30))).isoformat(),
            "event_type": "STAFFING_SNAPSHOT",
            "facility_id": facility["facility_id"],
            "ward": random.choice(WARDS),
            "nurse_count": nurses,
            "patient_count": patients,
            "acuity_score": acuity,
            "ratio_actual": ratio_actual,
            "ratio_target": ratio_target,
            "staffing_gap": gap,
        })
    return pd.DataFrame(events)


def generate_dq_violation_events(n: int) -> pd.DataFrame:
    """CTO: real-time data quality violations across silver/gold tables."""
    events = []
    now = datetime.utcnow()
    for _ in range(n):
        rule_type = random.choice(DQ_RULE_TYPES)
        expected = 100.0 if rule_type == "NULL_RATE" else round(random.uniform(50, 1000), 1)
        actual = expected * random.uniform(0.7, 1.3)
        vc = max(0, int(abs(expected - actual)))
        vp = round(abs(expected - actual) / expected * 100 if expected else 0, 2)
        sev = "CRITICAL" if vp > 10 else ("HIGH" if vp > 5 else ("MEDIUM" if vp > 1 else "LOW"))
        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": (now - timedelta(minutes=random.randint(0, 60))).isoformat(),
            "event_type": "DQ_VIOLATION",
            "source_table": random.choice(DQ_SOURCE_TABLES),
            "rule_name": f"{rule_type}_{uuid.uuid4().hex[:6]}",
            "rule_type": rule_type,
            "expected_value": expected,
            "actual_value": round(actual, 2),
            "violation_count": vc,
            "violation_pct": vp,
            "severity": sev,
        })
    return pd.DataFrame(events)


def generate_audit_access_events(n: int) -> pd.DataFrame:
    """CTO: PHI access anomaly detection (HIPAA-grade audit)."""
    events = []
    now = datetime.utcnow()
    for _ in range(n):
        action = random.choice(ACTION_TYPES)
        country = random.choice(GEO_COUNTRIES)
        # Higher anomaly when foreign + non-read action
        base = 0.1 if country == "US" else 0.6
        if action in ("EXPORT", "SHARE", "DELETE"):
            base += 0.25
        anomaly = round(min(1.0, max(0.0, base + random.gauss(0, 0.15))), 3)
        sev = "CRITICAL" if anomaly >= 0.9 else ("HIGH" if anomaly >= 0.7 else ("MEDIUM" if anomaly >= 0.4 else "LOW"))
        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": (now - timedelta(seconds=random.randint(0, 1800))).isoformat(),
            "event_type": "AUDIT_ACCESS",
            "user_principal": f"user{random.randint(100, 999)}@contoso.com",
            "action_type": action,
            "resource_accessed": random.choice([
                "lh_gold_curated.dim_patient", "Healthcare_RTI_DB.claims_events",
                "lh_gold_curated.fact_claims", "lh_silver_validated.adt_events"
            ]),
            "anomaly_score": anomaly,
            "geo_country": country,
            "severity": sev,
        })
    return pd.DataFrame(events)


def generate_model_drift_events(n: int) -> pd.DataFrame:
    """CTO: PSI/KS drift on production ML features."""
    events = []
    now = datetime.utcnow()
    for _ in range(n):
        model_name, model_version = random.choice(MODELS)
        feature = random.choice([
            "claim_amount", "patient_age", "provider_specialty", "diagnosis_code",
            "days_since_discharge", "prior_admission_count", "denial_rate_30d"
        ])
        psi = round(max(0, random.gauss(0.08, 0.07)), 4)
        ks = round(max(0, min(1, random.gauss(0.15, 0.1))), 4)
        drift = round(max(psi, ks), 4)
        sev = "CRITICAL" if psi >= 0.25 else ("HIGH" if psi >= 0.1 else ("MEDIUM" if psi >= 0.05 else "LOW"))
        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": (now - timedelta(minutes=random.randint(0, 1440))).isoformat(),
            "event_type": "MODEL_DRIFT",
            "model_name": model_name,
            "model_version": model_version,
            "feature_name": feature,
            "drift_score": drift,
            "psi_value": psi,
            "ks_value": ks,
            "baseline_period": "last_30d",
            "severity": sev,
        })
    return pd.DataFrame(events)


print("Event generators ready (15: original 3 + Provider 2 + Payer 3 + CMO 2 + CFO 1 + COO 2 + CTO 3).")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Stream Events — Eventstream primary path
# ============================================================================
# Eventstream: EventHub → Eventhouse + Lakehouse + Activator
# All routing is handled by the Eventstream topology (no direct KQL ingestion)
# ============================================================================

if not _es_producer:
    print("STOPPING: Eventstream is not available.")
    print("Set ES_CONNECTION_STRING and re-run this notebook.")
else:
    import time as _stream_time

    batch_num = 0
    max_batches = STREAM_BATCHES if STREAM_BATCHES > 0 else float("inf")
    print(f"Streaming {BATCH_SIZE} events/batch every {STREAM_INTERVAL_SEC}s...")
    print(f"  Eventstream → Eventhouse + Lakehouse + Activator")
    if STREAM_BATCHES > 0:
        print(f"  Batches: {STREAM_BATCHES} (then stop)")
    else:
        print(f"  Batches: infinite (Ctrl+C to stop)")

    try:
        while batch_num < max_batches:
            batch_num += 1

            # ── Original 3 event types ──
            claims_pdf = generate_claims_events(BATCH_SIZE)
            adt_pdf = generate_adt_events(BATCH_SIZE // 2)
            rx_pdf = generate_rx_events(BATCH_SIZE // 3)

            # ── Provider-specific events ──
            readmit_pdf = generate_readmission_events(BATCH_SIZE // 5)
            quality_pdf = generate_quality_metric_events(min(20, len(df_providers)))

            # ── Payer-specific events ──
            denial_adj_pdf = generate_denial_adjudication_events(BATCH_SIZE // 2)
            ar_pdf = generate_ar_snapshot_events()  # 1 per payer
            auth_pdf = generate_auth_lifecycle_events(BATCH_SIZE // 4)

            # ── Phase-2: CMO/CFO/COO/CTO gap-closing events ──
            clin_pdf  = generate_clinical_deterioration_events(max(1, BATCH_SIZE // 8))
            hai_pdf   = generate_mortality_hai_events(max(1, BATCH_SIZE // 12))
            nrv_pdf   = generate_net_revenue_variance_events(BATCH_SIZE // 4)
            cap_pdf   = generate_ops_capacity_events(max(1, BATCH_SIZE // 6))
            staff_pdf = generate_staffing_acuity_events(max(1, BATCH_SIZE // 10))
            dq_pdf    = generate_dq_violation_events(max(1, BATCH_SIZE // 12))
            audit_pdf = generate_audit_access_events(max(1, BATCH_SIZE // 6))
            drift_pdf = generate_model_drift_events(max(1, BATCH_SIZE // 20))

            # Push all to Eventstream (routes to all destinations)
            push_to_eventstream(claims_pdf, "claims_events")
            push_to_eventstream(adt_pdf, "adt_events")
            push_to_eventstream(rx_pdf, "rx_events")
            push_to_eventstream(readmit_pdf, "readmission_events")
            if len(quality_pdf) > 0:
                push_to_eventstream(quality_pdf, "quality_metric_events")
            push_to_eventstream(denial_adj_pdf, "denial_adjudication_events")
            push_to_eventstream(ar_pdf, "ar_snapshot_events")
            push_to_eventstream(auth_pdf, "auth_lifecycle_events")
            push_to_eventstream(clin_pdf,  "clinical_deterioration_events")
            push_to_eventstream(hai_pdf,   "mortality_hai_events")
            push_to_eventstream(nrv_pdf,   "net_revenue_variance_events")
            push_to_eventstream(cap_pdf,   "ops_capacity_events")
            push_to_eventstream(staff_pdf, "staffing_acuity_events")
            push_to_eventstream(dq_pdf,    "dq_violation_events")
            push_to_eventstream(audit_pdf, "audit_access_events")
            push_to_eventstream(drift_pdf, "model_drift_events")

            total = (len(claims_pdf) + len(adt_pdf) + len(rx_pdf) +
                     len(readmit_pdf) + len(quality_pdf) +
                     len(denial_adj_pdf) + len(ar_pdf) + len(auth_pdf) +
                     len(clin_pdf) + len(hai_pdf) + len(nrv_pdf) +
                     len(cap_pdf) + len(staff_pdf) + len(dq_pdf) +
                     len(audit_pdf) + len(drift_pdf))
            print(f"  Batch {batch_num}: {total} events → Eventstream (15 types)")

            if batch_num < max_batches:
                _stream_time.sleep(STREAM_INTERVAL_SEC)

    except KeyboardInterrupt:
        print(f"\nStreaming stopped by user after {batch_num} batches.")

    # Verify data landed in KQL (via Eventstream → Eventhouse routing)
    if _KUSTO_QUERY_URI:
        import time as _verify_time
        print("\nWaiting 10s for Eventstream → Eventhouse routing...")
        _verify_time.sleep(10)
        print("Verifying KQL table counts:")
        for _tbl in [
            "rti_all_events", "claims_events", "adt_events", "rx_events",
            "readmission_events", "quality_metric_events",
            "denial_adjudication_events", "ar_snapshot_events", "auth_lifecycle_events",
            "clinical_deterioration_events", "mortality_hai_events",
            "net_revenue_variance_events", "ops_capacity_events",
            "staffing_acuity_events", "dq_violation_events",
            "audit_access_events", "model_drift_events"
        ]:
            _rows = _kql_query(f"{_tbl} | count")
            _cnt = _rows[0][0] if _rows else 0
            print(f"  {_tbl}: {_cnt} rows")

    print(f"\nStreaming complete — {batch_num} batches pushed via Eventstream.")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Summary ----------
print("\n" + "=" * 60)
print("NB_RTI_Event_Simulator: COMPLETE")
print("=" * 60)
print()
print("Ingestion path:")
if _es_producer:
    print("  ✓ Eventstream → Eventhouse + Lakehouse + Activator")
    _es_producer.close()
else:
    print("  ✗ Eventstream (no connection string)")
print()
print("Fraud pattern injection rates:")
for k, v in FRAUD_PATTERNS.items():
    print(f"  - {k}: {v*100:.0f}%")
print()
print("Re-run this notebook anytime to generate more streaming events.")
print("=" * 60)

# METADATA **{"language":"python"}**
