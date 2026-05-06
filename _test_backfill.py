"""
Test script: Verify RTI backfill logic works.
Connects to Fabric Eventhouse via azure-identity + azure-kusto-data,
checks table row counts, and optionally runs the backfill commands.

Usage:
    python _test_backfill.py [--backfill]

Without --backfill: read-only check of table counts.
With --backfill: runs .set-or-append if typed tables are empty.
"""

import sys
import requests
from azure.identity import InteractiveBrowserCredential
from azure.kusto.data import KustoConnectionStringBuilder, KustoClient

# ── Config ───────────────────────────────────────────────────────────────────
WORKSPACE_NAME = "IQFinalDemo"
EVENTHOUSE_NAME = "Healthcare_RTI_Eventhouse"
KQL_DB_NAME = "Healthcare_RTI_DB"
FABRIC_BASE = "https://api.fabric.microsoft.com/v1"

DO_BACKFILL = "--backfill" in sys.argv


# ── Auth ─────────────────────────────────────────────────────────────────────
print("Authenticating via browser...")
cred = InteractiveBrowserCredential()

# Get Fabric token
fabric_token = cred.get_token("https://analysis.windows.net/powerbi/api/.default").token
headers = {"Authorization": f"Bearer {fabric_token}", "Content-Type": "application/json"}

# Get Kusto token
kusto_token = cred.get_token("https://kusto.kusto.windows.net/.default").token


# ── Find workspace ───────────────────────────────────────────────────────────
print(f"Finding workspace '{WORKSPACE_NAME}'...")
resp = requests.get(f"{FABRIC_BASE}/workspaces", headers=headers)
resp.raise_for_status()
workspace = next(
    (w for w in resp.json()["value"] if w["displayName"] == WORKSPACE_NAME), None
)
if not workspace:
    print(f"ERROR: Workspace '{WORKSPACE_NAME}' not found")
    sys.exit(1)
WORKSPACE_ID = workspace["id"]
print(f"  Workspace ID: {WORKSPACE_ID}")


# ── Find Eventhouse ──────────────────────────────────────────────────────────
print(f"Finding Eventhouse '{EVENTHOUSE_NAME}'...")
resp = requests.get(
    f"{FABRIC_BASE}/workspaces/{WORKSPACE_ID}/items?type=Eventhouse", headers=headers
)
resp.raise_for_status()
eventhouse = next(
    (e for e in resp.json()["value"] if e["displayName"] == EVENTHOUSE_NAME), None
)
if not eventhouse:
    print(f"ERROR: Eventhouse '{EVENTHOUSE_NAME}' not found in workspace")
    sys.exit(1)
eventhouse_id = eventhouse["id"]
print(f"  Eventhouse ID: {eventhouse_id}")

# Get query URI
resp = requests.get(
    f"{FABRIC_BASE}/workspaces/{WORKSPACE_ID}/eventhouses/{eventhouse_id}",
    headers=headers,
)
resp.raise_for_status()
props = resp.json().get("properties", resp.json())
query_uri = props.get("queryServiceUri", "")
if not query_uri:
    print("ERROR: No queryServiceUri found on Eventhouse")
    sys.exit(1)
print(f"  Query URI: {query_uri}")


# ── Find KQL Database ────────────────────────────────────────────────────────
print(f"Finding KQL Database '{KQL_DB_NAME}'...")
resp = requests.get(
    f"{FABRIC_BASE}/workspaces/{WORKSPACE_ID}/items?type=KQLDatabase", headers=headers
)
resp.raise_for_status()
kql_db = next(
    (d for d in resp.json()["value"] if d["displayName"] == KQL_DB_NAME), None
)
if not kql_db:
    # Try with eventhouse name as fallback
    kql_db = next(
        (d for d in resp.json()["value"] if d["displayName"] == EVENTHOUSE_NAME), None
    )
    if kql_db:
        KQL_DB_NAME = EVENTHOUSE_NAME
if not kql_db:
    print(f"ERROR: KQL Database not found")
    sys.exit(1)
print(f"  KQL DB: {KQL_DB_NAME} ({kql_db['id']})")


# ── Connect Kusto client ─────────────────────────────────────────────────────
print("\nConnecting Kusto client...")
kcsb = KustoConnectionStringBuilder.with_aad_user_token_authentication(
    query_uri, kusto_token
)
client = KustoClient(kcsb)


# ── Check table counts ───────────────────────────────────────────────────────
def get_count(table_name):
    """Query row count for a table. Returns -1 if table doesn't exist."""
    try:
        result = client.execute(KQL_DB_NAME, f"{table_name} | count")
        for row in result.primary_results[0]:
            return int(row[0])
    except Exception as e:
        if "not found" in str(e).lower() or "semantic error" in str(e).lower():
            return -1
        raise
    return 0


print("\n" + "=" * 60)
print("TABLE ROW COUNTS")
print("=" * 60)

tables = ["rti_all_events", "claims_events", "adt_events", "rx_events"]
counts = {}
for t in tables:
    c = get_count(t)
    counts[t] = c
    status = "TABLE NOT FOUND" if c == -1 else f"{c:,} rows"
    print(f"  {t:25s} : {status}")

print("=" * 60)


# ── Evaluate backfill need ───────────────────────────────────────────────────
landing = counts["rti_all_events"]
typed_empty = all(counts[t] == 0 for t in ["claims_events", "adt_events", "rx_events"])
typed_missing = any(counts[t] == -1 for t in ["claims_events", "adt_events", "rx_events"])

if landing <= 0:
    print("\nrti_all_events is empty or missing — nothing to backfill.")
    print("Run the Event Simulator first to generate data.")
    sys.exit(0)

if typed_missing:
    print("\nSome typed tables don't exist yet — run NB_RTI_Setup_Eventhouse first")
    print("to create tables, functions, and update policies.")
    sys.exit(1)

if not typed_empty:
    print("\nTyped tables already have data — backfill not needed!")
    print("The update policies are working correctly.")
    sys.exit(0)

print(f"\nBACKFILL NEEDED: rti_all_events has {landing:,} rows but typed tables are empty.")

if not DO_BACKFILL:
    print("\nRe-run with --backfill to execute the backfill:")
    print(f"  python _test_backfill.py --backfill")
    sys.exit(0)


# ── Create functions + update policies (if missing) ──────────────────────────
print("\nEnsuring extract functions and update policies exist...")

setup_commands = [
    ("ExtractClaimsEvents function", """.create-or-alter function ExtractClaimsEvents() {
        rti_all_events
        | where _table == "claims_events"
        | project event_id, event_timestamp, event_type, claim_id, patient_id,
                  provider_id, facility_id, payer_id, diagnosis_code,
                  procedure_code, claim_type, claim_amount, latitude, longitude,
                  injected_fraud_flags
    }"""),
    ("ExtractAdtEvents function", """.create-or-alter function ExtractAdtEvents() {
        rti_all_events
        | where _table == "adt_events"
        | project event_id, event_timestamp, event_type, patient_id, facility_id,
                  facility_name, admission_type, primary_diagnosis, latitude,
                  longitude, has_open_care_gaps, open_gap_measures
    }"""),
    ("ExtractRxEvents function", """.create-or-alter function ExtractRxEvents() {
        rti_all_events
        | where _table == "rx_events"
        | project event_id, event_timestamp, event_type, patient_id, provider_id,
                  medication_code, medication_name, drug_class, quantity,
                  days_supply, latitude, longitude
    }"""),
    ("claims_events update policy",
     '.alter table claims_events policy update @\'[{"IsEnabled": true, "Source": "rti_all_events", "Query": "ExtractClaimsEvents()", "IsTransactional": false, "PropagateIngestionProperties": true}]\''),
    ("adt_events update policy",
     '.alter table adt_events policy update @\'[{"IsEnabled": true, "Source": "rti_all_events", "Query": "ExtractAdtEvents()", "IsTransactional": false, "PropagateIngestionProperties": true}]\''),
    ("rx_events update policy",
     '.alter table rx_events policy update @\'[{"IsEnabled": true, "Source": "rti_all_events", "Query": "ExtractRxEvents()", "IsTransactional": false, "PropagateIngestionProperties": true}]\''),
]

# Need management client for control commands
mgmt_kcsb = KustoConnectionStringBuilder.with_aad_user_token_authentication(
    query_uri, kusto_token
)
mgmt_client = KustoClient(mgmt_kcsb)

for label, cmd in setup_commands:
    try:
        mgmt_client.execute_mgmt(KQL_DB_NAME, cmd)
        print(f"  OK: {label}")
    except Exception as e:
        print(f"  WARN: {label} — {e}")

# ── Run backfill ─────────────────────────────────────────────────────────────
print("\nRunning backfill commands...")

backfill_commands = [
    ("claims_events", ".set-or-append claims_events <| ExtractClaimsEvents()"),
    ("adt_events", ".set-or-append adt_events <| ExtractAdtEvents()"),
    ("rx_events", ".set-or-append rx_events <| ExtractRxEvents()"),
]

for table_name, cmd in backfill_commands:
    print(f"  Backfilling {table_name}...")
    try:
        result = mgmt_client.execute_mgmt(KQL_DB_NAME, cmd)
        # .set-or-append returns extent info
        ingested = 0
        for row in result.primary_results[0]:
            ingested += 1
        print(f"    OK: {table_name} — {ingested} extent(s) created")
    except Exception as e:
        print(f"    ERROR: {table_name} — {e}")


# ── Verify ───────────────────────────────────────────────────────────────────
print("\nVerifying after backfill...")
for t in ["claims_events", "adt_events", "rx_events"]:
    c = get_count(t)
    print(f"  {t:25s} : {c:,} rows")

print("\nDone! Scoring notebooks should now find data.")
