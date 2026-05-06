"""
Fix and backfill: Create corrected Extract functions that match the ACTUAL
rti_all_events schema, then run backfill.
"""
import requests
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

# Discover workspace
print("Discovering workspace and Eventhouse...")
resp = requests.get(f"{FABRIC_BASE}/workspaces", headers=headers)
resp.raise_for_status()
workspace = next((w for w in resp.json()["value"] if w["displayName"] == WORKSPACE_NAME), None)
if not workspace:
    raise RuntimeError(f"Workspace '{WORKSPACE_NAME}' not found")
WS_ID = workspace["id"]

# Discover Eventhouse query URI
resp = requests.get(f"{FABRIC_BASE}/workspaces/{WS_ID}/items?type=Eventhouse", headers=headers)
resp.raise_for_status()
eventhouse = next((e for e in resp.json()["value"] if e["displayName"] == EVENTHOUSE_NAME), None)
if not eventhouse:
    raise RuntimeError(f"Eventhouse '{EVENTHOUSE_NAME}' not found")
resp = requests.get(f"{FABRIC_BASE}/workspaces/{WS_ID}/eventhouses/{eventhouse['id']}", headers=headers)
resp.raise_for_status()
props = resp.json().get("properties", resp.json())
uri = props.get("queryServiceUri", "")
if not uri:
    raise RuntimeError("No queryServiceUri found")
print(f"  Query URI: {uri}")

kcsb = KustoConnectionStringBuilder.with_aad_user_token_authentication(uri, kusto_token)
client = KustoClient(kcsb)


def run_mgmt(cmd, label=""):
    try:
        result = client.execute_mgmt(DB, cmd)
        print(f"  OK: {label}")
        return result
    except Exception as e:
        err = str(e)[:200]
        print(f"  ERROR: {label} — {err}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# Step 1: Add missing columns to rti_all_events so future ingestion captures them
# ══════════════════════════════════════════════════════════════════════════════
print("Step 1: Adding missing columns to rti_all_events...")

# Only add columns that DON'T already exist. Don't re-specify existing columns
# (Eventstream created quantity/days_supply as 'long', medication_code etc as 'string')
# .alter-merge fails if you specify a column with a different type than what exists.
run_mgmt(""".alter-merge table rti_all_events (
    claim_id: string,
    facility_id: string,
    facility_name: string,
    payer_id: string,
    diagnosis_code: string,
    procedure_code: string,
    claim_type: string,
    claim_amount: real,
    admission_type: string,
    primary_diagnosis: string,
    has_open_care_gaps: bool,
    open_gap_measures: string,
    injected_fraud_flags: string
)""", "add missing columns to rti_all_events")

# ══════════════════════════════════════════════════════════════════════════════
# Step 2: Create corrected Extract functions
# These use todatetime() for the timestamp cast and coalesce/defaults for
# columns that may not have data in existing rows.
# ══════════════════════════════════════════════════════════════════════════════
print("\nStep 2: Creating corrected Extract functions...")

run_mgmt(""".create-or-alter function ExtractClaimsEvents() {
    rti_all_events
    | where _table == "claims_events"
    | project event_id,
              event_timestamp = todatetime(event_timestamp),
              event_type,
              claim_id = coalesce(claim_id, ""),
              patient_id,
              provider_id = coalesce(provider_id, ""),
              facility_id = coalesce(facility_id, ""),
              payer_id = coalesce(payer_id, ""),
              diagnosis_code = coalesce(diagnosis_code, ""),
              procedure_code = coalesce(procedure_code, ""),
              claim_type = coalesce(claim_type, ""),
              claim_amount = coalesce(claim_amount, 0.0),
              latitude,
              longitude,
              injected_fraud_flags = coalesce(injected_fraud_flags, "")
}""", "ExtractClaimsEvents")

run_mgmt(""".create-or-alter function ExtractAdtEvents() {
    rti_all_events
    | where _table == "adt_events"
    | project event_id,
              event_timestamp = todatetime(event_timestamp),
              event_type,
              patient_id,
              facility_id = coalesce(facility_id, ""),
              facility_name = coalesce(facility_name, ""),
              admission_type = coalesce(admission_type, ""),
              primary_diagnosis = coalesce(primary_diagnosis, ""),
              latitude,
              longitude,
              has_open_care_gaps = coalesce(has_open_care_gaps, false),
              open_gap_measures = coalesce(open_gap_measures, "")
}""", "ExtractAdtEvents")

run_mgmt(""".create-or-alter function ExtractRxEvents() {
    rti_all_events
    | where _table == "rx_events"
    | project event_id,
              event_timestamp = todatetime(event_timestamp),
              event_type,
              patient_id,
              provider_id = coalesce(provider_id, ""),
              medication_code = coalesce(medication_code, ""),
              medication_name = coalesce(medication_name, ""),
              drug_class = coalesce(drug_class, ""),
              quantity = toint(coalesce(quantity, 0)),
              days_supply = toint(coalesce(days_supply, 0)),
              latitude,
              longitude
}""", "ExtractRxEvents")

# ══════════════════════════════════════════════════════════════════════════════
# Step 3: Set update policies (for future data)
# ══════════════════════════════════════════════════════════════════════════════
print("\nStep 3: Setting update policies...")

run_mgmt(
    '.alter table claims_events policy update @\'[{"IsEnabled": true, "Source": "rti_all_events", "Query": "ExtractClaimsEvents()", "IsTransactional": false, "PropagateIngestionProperties": true}]\'',
    "claims_events update policy"
)
run_mgmt(
    '.alter table adt_events policy update @\'[{"IsEnabled": true, "Source": "rti_all_events", "Query": "ExtractAdtEvents()", "IsTransactional": false, "PropagateIngestionProperties": true}]\'',
    "adt_events update policy"
)
run_mgmt(
    '.alter table rx_events policy update @\'[{"IsEnabled": true, "Source": "rti_all_events", "Query": "ExtractRxEvents()", "IsTransactional": false, "PropagateIngestionProperties": true}]\'',
    "rx_events update policy"
)

# ══════════════════════════════════════════════════════════════════════════════
# Step 4: Update JSON mapping to include the new columns
# ══════════════════════════════════════════════════════════════════════════════
print("\nStep 4: Updating JSON ingestion mapping...")

run_mgmt(""".create-or-alter table rti_all_events ingestion json mapping 'rti_all_events_mapping'
'[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"string"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"_table","path":"$._table","datatype":"string"},{"column":"claim_id","path":"$.claim_id","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"provider_id","path":"$.provider_id","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"},{"column":"facility_name","path":"$.facility_name","datatype":"string"},{"column":"payer_id","path":"$.payer_id","datatype":"string"},{"column":"diagnosis_code","path":"$.diagnosis_code","datatype":"string"},{"column":"procedure_code","path":"$.procedure_code","datatype":"string"},{"column":"claim_type","path":"$.claim_type","datatype":"string"},{"column":"claim_amount","path":"$.claim_amount","datatype":"real"},{"column":"admission_type","path":"$.admission_type","datatype":"string"},{"column":"primary_diagnosis","path":"$.primary_diagnosis","datatype":"string"},{"column":"medication_code","path":"$.medication_code","datatype":"string"},{"column":"medication_name","path":"$.medication_name","datatype":"string"},{"column":"drug_class","path":"$.drug_class","datatype":"string"},{"column":"quantity","path":"$.quantity","datatype":"long"},{"column":"days_supply","path":"$.days_supply","datatype":"long"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"},{"column":"injected_fraud_flags","path":"$.injected_fraud_flags","datatype":"string"},{"column":"has_open_care_gaps","path":"$.has_open_care_gaps","datatype":"bool"},{"column":"open_gap_measures","path":"$.open_gap_measures","datatype":"string"}]'""",
    "rti_all_events_mapping (full)")

# ══════════════════════════════════════════════════════════════════════════════
# Step 5: Backfill typed tables from existing data
# ══════════════════════════════════════════════════════════════════════════════
print("\nStep 5: Backfilling typed tables...")

for table, cmd in [
    ("claims_events", ".set-or-append claims_events <| ExtractClaimsEvents()"),
    ("adt_events", ".set-or-append adt_events <| ExtractAdtEvents()"),
    ("rx_events", ".set-or-append rx_events <| ExtractRxEvents()"),
]:
    run_mgmt(cmd, f"backfill {table}")

# ══════════════════════════════════════════════════════════════════════════════
# Step 6: Verify
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("FINAL TABLE COUNTS")
print("=" * 60)
for t in ["rti_all_events", "claims_events", "adt_events", "rx_events"]:
    result = client.execute(DB, f"{t} | count")
    for row in result.primary_results[0]:
        print(f"  {t:25s} : {int(row[0]):,} rows")
print("=" * 60)
print("\nDone! Update policies set for future data, existing data backfilled.")
