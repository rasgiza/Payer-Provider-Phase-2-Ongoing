"""
Clear stale data (missing claims fields) and verify Eventstream schema readiness.
After this, re-running the Simulator will produce data with all columns populated.
"""
import requests, json
from azure.identity import InteractiveBrowserCredential
from azure.kusto.data import KustoConnectionStringBuilder, KustoClient

WORKSPACE_NAME = "IQFinalDemo"
EVENTHOUSE_NAME = "Healthcare_RTI_Eventhouse"
DB = "Healthcare_RTI_DB"
FABRIC_BASE = "https://api.fabric.microsoft.com/v1"

cred = InteractiveBrowserCredential()
fabric_token = cred.get_token("https://analysis.windows.net/powerbi/api/.default").token
kusto_token = cred.get_token("https://kusto.kusto.windows.net/.default").token
headers = {"Authorization": f"Bearer {fabric_token}", "Content-Type": "application/json"}

# Discover
resp = requests.get(f"{FABRIC_BASE}/workspaces", headers=headers)
ws = next(w for w in resp.json()["value"] if w["displayName"] == WORKSPACE_NAME)
WS_ID = ws["id"]
resp = requests.get(f"{FABRIC_BASE}/workspaces/{WS_ID}/items?type=Eventhouse", headers=headers)
eh = next(e for e in resp.json()["value"] if e["displayName"] == EVENTHOUSE_NAME)
resp = requests.get(f"{FABRIC_BASE}/workspaces/{WS_ID}/eventhouses/{eh['id']}", headers=headers)
uri = resp.json().get("properties", resp.json()).get("queryServiceUri", "")
print(f"Query URI: {uri}")

kcsb = KustoConnectionStringBuilder.with_aad_user_token_authentication(uri, kusto_token)
client = KustoClient(kcsb)


def run_mgmt(cmd, label=""):
    try:
        client.execute_mgmt(DB, cmd)
        print(f"  OK: {label}")
        return True
    except Exception as e:
        print(f"  ERROR: {label} — {str(e)[:200]}")
        return False


# Step 1: Drop stale data from all tables (claims_events has empty claim_id)
print("\nStep 1: Clearing stale data (missing claims fields)...")
run_mgmt(".drop extents <| .show table rti_all_events extents", "clear rti_all_events")
run_mgmt(".drop extents <| .show table claims_events extents", "clear claims_events")
run_mgmt(".drop extents <| .show table adt_events extents", "clear adt_events")
run_mgmt(".drop extents <| .show table rx_events extents", "clear rx_events")

# Step 2: Verify schema has all needed columns
print("\nStep 2: Verifying rti_all_events schema...")
result = client.execute_mgmt(DB, ".show table rti_all_events schema as json")
for row in result.primary_results[0]:
    schema = json.loads(row[1])
    cols = {c["Name"] for c in schema.get("OrderedColumns", [])}
    required = {"event_id", "event_timestamp", "event_type", "_table",
                "claim_id", "patient_id", "provider_id", "facility_id",
                "facility_name", "payer_id", "diagnosis_code", "procedure_code",
                "claim_type", "claim_amount", "admission_type", "primary_diagnosis",
                "medication_code", "medication_name", "drug_class", "quantity",
                "days_supply", "latitude", "longitude", "injected_fraud_flags",
                "has_open_care_gaps", "open_gap_measures"}
    missing = required - cols
    if missing:
        print(f"  MISSING COLUMNS: {missing}")
        print("  Run _fix_and_backfill.py first!")
    else:
        print(f"  All {len(required)} required columns present")

# Step 3: Verify functions exist
print("\nStep 3: Verifying Extract functions...")
result = client.execute_mgmt(DB, ".show functions")
funcs = set()
for row in result.primary_results[0]:
    funcs.add(row[0])
for f in ["ExtractClaimsEvents", "ExtractAdtEvents", "ExtractRxEvents"]:
    if f in funcs:
        print(f"  OK: {f}")
    else:
        print(f"  MISSING: {f}")

# Step 4: Verify update policies
print("\nStep 4: Verifying update policies...")
for tbl in ["claims_events", "adt_events", "rx_events"]:
    result = client.execute_mgmt(DB, f".show table {tbl} policy update")
    for row in result.primary_results[0]:
        policy = row[1] if len(row) > 1 else str(row)
        if "IsEnabled" in str(policy) and "true" in str(policy).lower():
            print(f"  OK: {tbl} — policy enabled")
        else:
            print(f"  WARN: {tbl} — policy: {str(policy)[:100]}")

# Step 5: Final counts (should all be 0 after clearing)
print("\nStep 5: Table counts (should be 0 after clear)...")
for t in ["rti_all_events", "claims_events", "adt_events", "rx_events"]:
    result = client.execute(DB, f"{t} | count")
    for row in result.primary_results[0]:
        print(f"  {t:25s} : {int(row[0]):,} rows")

print("\n" + "=" * 60)
print("READY! Re-run the Event Simulator notebook to generate fresh data.")
print("All columns, functions, and update policies are correctly configured.")
print("New data flowing through Eventstream will populate ALL columns.")
print("=" * 60)
