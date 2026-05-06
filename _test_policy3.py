"""Definitive test: insert into rti_all_events via batch and check if update policies fire.
Uses the ACTUAL 29-column schema discovered from .show table rti_all_events schema as json."""
import requests, json, time
from azure.identity import InteractiveBrowserCredential

cred = InteractiveBrowserCredential()
token = cred.get_token('https://trd-8ahv0svs43af8c5324.z9.kusto.fabric.microsoft.com/.default').token
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
URI = 'https://trd-8ahv0svs43af8c5324.z9.kusto.fabric.microsoft.com'
DB = 'Healthcare_RTI_DB'

def mgmt(cmd):
    r = requests.post(f'{URI}/v1/rest/mgmt', headers=headers, json={'db': DB, 'csl': cmd})
    if r.status_code != 200:
        print(f"  ERROR: HTTP {r.status_code}: {r.text[:500]}")
        return None
    if not r.text.strip():
        print(f"  ERROR: Empty response body (HTTP {r.status_code})")
        return None
    return r.json()

def kql(q):
    r = requests.post(f'{URI}/v1/rest/query', headers=headers, json={'db': DB, 'csl': q})
    return r.json()

# Step 1: Clear all tables
print("=== Step 1: Clear all tables ===")
for t in ['claims_events', 'adt_events', 'rx_events', 'rti_all_events']:
    mgmt(f'.clear table {t} data')
    print(f"  Cleared {t}")

# Step 2: Insert using .set-or-append with a datatable that matches the EXACT schema
# Schema: event_id(string), event_timestamp(string), event_type(string), patient_id(string),
#   provider_id(string), medication_code(string), medication_name(string), drug_class(string),
#   quantity(long), days_supply(long), latitude(real), longitude(real), _table(string),
#   EventProcessedUtcTime(datetime), PartitionId(long), EventEnqueuedUtcTime(datetime),
#   claim_id(string), facility_id(string), facility_name(string), payer_id(string),
#   diagnosis_code(string), procedure_code(string), claim_type(string), claim_amount(real),
#   admission_type(string), primary_diagnosis(string), has_open_care_gaps(bool),
#   open_gap_measures(string), injected_fraud_flags(string)

# Use a single-line datatable approach with let statement
print("\n=== Step 2: Insert test claims row ===")
claims_cmd = (
    '.set-or-append rti_all_events <| '
    'print event_id="TEST-CLM-001", event_timestamp="2026-04-28T16:00:00Z", '
    'event_type="CLAIM_SUBMITTED", patient_id="PAT001", provider_id="PRV001", '
    'medication_code="", medication_name="", drug_class="", '
    'quantity=long(0), days_supply=long(0), latitude=42.33, longitude=-83.05, '
    '_table="claims_events", EventProcessedUtcTime=datetime(null), '
    'PartitionId=long(0), EventEnqueuedUtcTime=datetime(null), '
    'claim_id="CLM_TEST_001", facility_id="FAC001", facility_name="Test Hospital", '
    'payer_id="PAY001", diagnosis_code="J18.9", procedure_code="99213", '
    'claim_type="Professional", claim_amount=1500.0, admission_type="", '
    'primary_diagnosis="Pneumonia", has_open_care_gaps=true, '
    'open_gap_measures="HbA1c", injected_fraud_flags=""'
)
result = mgmt(claims_cmd)
if result:
    tbl = result.get('Tables', [{}])[0]
    rows = tbl.get('Rows', [])
    if rows:
        cols = [c['ColumnName'] for c in tbl.get('Columns', [])]
        d = dict(zip(cols, rows[0]))
        print(f"  SUCCESS: ExtentId={d.get('ExtentId', '?')}")
    else:
        print(f"  Returned but no rows")

print("\n=== Step 3: Insert test ADT row ===")
adt_cmd = (
    '.set-or-append rti_all_events <| '
    'print event_id="TEST-ADT-001", event_timestamp="2026-04-28T16:01:00Z", '
    'event_type="ADMISSION", patient_id="PAT002", provider_id="PRV002", '
    'medication_code="", medication_name="", drug_class="", '
    'quantity=long(0), days_supply=long(0), latitude=42.34, longitude=-83.04, '
    '_table="adt_events", EventProcessedUtcTime=datetime(null), '
    'PartitionId=long(0), EventEnqueuedUtcTime=datetime(null), '
    'claim_id="", facility_id="FAC002", facility_name="Test ER", '
    'payer_id="", diagnosis_code="", procedure_code="", '
    'claim_type="", claim_amount=0.0, admission_type="Emergency", '
    'primary_diagnosis="Chest Pain", has_open_care_gaps=false, '
    'open_gap_measures="", injected_fraud_flags=""'
)
result = mgmt(adt_cmd)
if result:
    tbl = result.get('Tables', [{}])[0]
    rows = tbl.get('Rows', [])
    print(f"  {'SUCCESS' if rows else 'No rows returned'}")

print("\n=== Step 4: Insert test RX row ===")
rx_cmd = (
    '.set-or-append rti_all_events <| '
    'print event_id="TEST-RX-001", event_timestamp="2026-04-28T16:02:00Z", '
    'event_type="PRESCRIPTION_FILLED", patient_id="PAT003", provider_id="PRV003", '
    'medication_code="MED001", medication_name="Metformin", drug_class="Biguanide", '
    'quantity=long(60), days_supply=long(30), latitude=42.35, longitude=-83.03, '
    '_table="rx_events", EventProcessedUtcTime=datetime(null), '
    'PartitionId=long(0), EventEnqueuedUtcTime=datetime(null), '
    'claim_id="", facility_id="", facility_name="", '
    'payer_id="", diagnosis_code="", procedure_code="", '
    'claim_type="", claim_amount=0.0, admission_type="", '
    'primary_diagnosis="", has_open_care_gaps=false, '
    'open_gap_measures="", injected_fraud_flags=""'
)
result = mgmt(rx_cmd)
if result:
    tbl = result.get('Tables', [{}])[0]
    rows = tbl.get('Rows', [])
    print(f"  {'SUCCESS' if rows else 'No rows returned'}")

# Step 5: Wait for update policies (they run async, typically <5s)
print("\n  Waiting 10s for update policies to process...")
time.sleep(10)

# Step 6: Check results
print("\n=== Step 5: Results ===")
print("  rti_all_events count:", end=" ")
r = kql('rti_all_events | count')
print(r.get('Tables', [{}])[0].get('Rows', [[0]])[0][0])

all_pass = True
for t in ['claims_events', 'adt_events', 'rx_events']:
    r = kql(f'{t} | count')
    cnt = r.get('Tables', [{}])[0].get('Rows', [[0]])[0][0]
    status = "PASS - policy fired!" if cnt > 0 else "FAIL - policy NOT firing"
    if cnt == 0:
        all_pass = False
    print(f"  {t}: {cnt} — {status}")

if all_pass:
    print("\n  *** ALL UPDATE POLICIES WORK ***")
    print("  The issue is ONLY the Launcher's ProcessedIngestion mode.")
    print("  Fix: Change Launcher Cell 14 to use DirectIngestion.")
else:
    print("\n  *** UPDATE POLICIES NOT FIRING ***")
    print("  Checking function output directly...")
    for fn in ['ExtractClaimsEvents', 'ExtractAdtEvents', 'ExtractRxEvents']:
        r = kql(f'{fn}() | count')
        cnt = r.get('Tables', [{}])[0].get('Rows', [[0]])[0][0]
        print(f"    {fn}(): {cnt} rows")
    
    # Check policies
    print("\n  Checking update policy definitions...")
    for t in ['claims_events', 'adt_events', 'rx_events']:
        r = mgmt(f'.show table {t} policy update')
        if r:
            tbl = r.get('Tables', [{}])[0]
            rows = tbl.get('Rows', [])
            if rows:
                cols = [c['ColumnName'] for c in tbl.get('Columns', [])]
                d = dict(zip(cols, rows[0]))
                print(f"    {t}: enabled={d.get('IsEnabled')} policy={d.get('Policy','?')[:150]}")
    
    # Check ingestion failures from policy
    print("\n  Checking ingestion failures (last 5 min)...")
    r = mgmt('.show ingestion failures | where FailedOn > ago(5m) | take 5')
    if r:
        tbl = r.get('Tables', [{}])[0]
        rows = tbl.get('Rows', [])
        if rows:
            cols = [c['ColumnName'] for c in tbl.get('Columns', [])]
            for row in rows:
                d = dict(zip(cols, row))
                print(f"    Table={d.get('Table')} Error={d.get('Details','')[:200]}")
        else:
            print("    No ingestion failures")
