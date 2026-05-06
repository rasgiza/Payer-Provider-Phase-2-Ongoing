"""Test update policies with a simpler approach - use .ingest inline"""
import requests, json, time
from azure.identity import InteractiveBrowserCredential

cred = InteractiveBrowserCredential()
token = cred.get_token('https://trd-8ahv0svs43af8c5324.z9.kusto.fabric.microsoft.com/.default').token
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
URI = 'https://trd-8ahv0svs43af8c5324.z9.kusto.fabric.microsoft.com'
DB = 'Healthcare_RTI_DB'

def kql(q):
    r = requests.post(f'{URI}/v1/rest/query', headers=headers, json={'db': DB, 'csl': q})
    if r.status_code != 200:
        print(f"  KQL ERROR: HTTP {r.status_code}: {r.text[:200]}")
        return {"Tables": [{"Columns": [], "Rows": []}]}
    return r.json()

def mgmt(cmd):
    r = requests.post(f'{URI}/v1/rest/mgmt', headers=headers, json={'db': DB, 'csl': cmd})
    if r.status_code != 200:
        print(f"  MGMT ERROR: HTTP {r.status_code}: {r.text[:300]}")
        return None
    try:
        return r.json()
    except:
        print(f"  MGMT: Empty response (status={r.status_code})")
        return None

# 1. Check current counts
print("=== Before: counts ===")
for t in ['rti_all_events', 'claims_events', 'adt_events', 'rx_events']:
    r = kql(f'{t} | count')
    cnt = r.get('Tables', [{}])[0].get('Rows', [[0]])[0][0]
    print(f"  {t}: {cnt}")

# 2. Insert test data using .set-or-append with datatable
print("\n=== Inserting test claims row ===")
cmd = """.set-or-append rti_all_events <|
datatable(event_id:string, event_timestamp:string, event_type:string, patient_id:string, provider_id:string, latitude:real, longitude:real, _table:string, claim_id:string, facility_id:string, facility_name:string, payer_id:string, diagnosis_code:string, procedure_code:string, claim_type:string, claim_amount:real, admission_type:string, primary_diagnosis:string, has_open_care_gaps:string, open_gap_measures:string, injected_fraud_flags:string, medication_name:string, dosage:string, quantity:long, days_supply:long, pharmacy_id:string)
["TEST-CLM-001", "2026-04-28T16:00:00Z", "CLAIM_SUBMITTED", "PAT_TEST_001", "PRV_TEST_001", 42.33, -83.05, "claims_events", "CLM_TEST_001", "FAC_TEST_001", "Test Hospital", "PAY_TEST_001", "J18.9", "99213", "Professional", 1500.0, "", "Pneumonia", "true", "HbA1c", "", "", "", 0, 0, ""]
"""
result = mgmt(cmd)
if result:
    tbl = result.get('Tables', [{}])[0]
    rows = tbl.get('Rows', [])
    if rows:
        cols = [c['ColumnName'] for c in tbl.get('Columns', [])]
        d = dict(zip(cols, rows[0]))
        print(f"  OK: ExtentId={d.get('ExtentId', '?')}")
    else:
        print(f"  Result: {json.dumps(result)[:300]}")

# 3. Insert test ADT row
print("\n=== Inserting test ADT row ===")
cmd2 = """.set-or-append rti_all_events <|
datatable(event_id:string, event_timestamp:string, event_type:string, patient_id:string, provider_id:string, latitude:real, longitude:real, _table:string, claim_id:string, facility_id:string, facility_name:string, payer_id:string, diagnosis_code:string, procedure_code:string, claim_type:string, claim_amount:real, admission_type:string, primary_diagnosis:string, has_open_care_gaps:string, open_gap_measures:string, injected_fraud_flags:string, medication_name:string, dosage:string, quantity:long, days_supply:long, pharmacy_id:string)
["TEST-ADT-001", "2026-04-28T16:01:00Z", "ADMISSION", "PAT_TEST_002", "PRV_TEST_002", 42.34, -83.04, "adt_events", "", "FAC_TEST_002", "Test ER", "", "", "", "", 0.0, "Emergency", "Chest Pain", "", "", "", "", "", 0, 0, ""]
"""
result = mgmt(cmd2)
if result:
    rows = result.get('Tables', [{}])[0].get('Rows', [])
    print(f"  {'OK' if rows else 'No rows returned'}")

# 4. Insert test RX row
print("\n=== Inserting test RX row ===")
cmd3 = """.set-or-append rti_all_events <|
datatable(event_id:string, event_timestamp:string, event_type:string, patient_id:string, provider_id:string, latitude:real, longitude:real, _table:string, claim_id:string, facility_id:string, facility_name:string, payer_id:string, diagnosis_code:string, procedure_code:string, claim_type:string, claim_amount:real, admission_type:string, primary_diagnosis:string, has_open_care_gaps:string, open_gap_measures:string, injected_fraud_flags:string, medication_name:string, dosage:string, quantity:long, days_supply:long, pharmacy_id:string)
["TEST-RX-001", "2026-04-28T16:02:00Z", "PRESCRIPTION_FILLED", "PAT_TEST_003", "PRV_TEST_003", 42.35, -83.03, "rx_events", "", "", "", "", "", "", "", 0.0, "", "", "", "", "", "Metformin", "500mg", 60, 30, "PHR_TEST_001"]
"""
result = mgmt(cmd3)
if result:
    rows = result.get('Tables', [{}])[0].get('Rows', [])
    print(f"  {'OK' if rows else 'No rows returned'}")

# 5. Wait for update policies
print("\n  Waiting 10 seconds for update policies to fire...")
time.sleep(10)

# 6. Check typed tables
print("\n=== After: typed table counts ===")
all_ok = True
for t in ['claims_events', 'adt_events', 'rx_events']:
    r = kql(f'{t} | count')
    cnt = r.get('Tables', [{}])[0].get('Rows', [[0]])[0][0]
    status = "POLICY FIRED!" if cnt > 0 else "STILL 0"
    if cnt == 0:
        all_ok = False
    print(f"  {t}: {cnt} — {status}")

if all_ok:
    print("\n  UPDATE POLICIES WORK! The DirectIngestion fix will solve everything.")
    # Show a sample
    r = kql("claims_events | take 1")
    tbl = r.get('Tables', [{}])[0]
    cols = [c['ColumnName'] for c in tbl.get('Columns', [])]
    rows = tbl.get('Rows', [])
    if rows:
        d = dict(zip(cols, rows[0]))
        print(f"  Sample: event_id={d.get('event_id')} claim_id={d.get('claim_id')}")
else:
    print("\n  POLICIES NOT FIRING - checking why...")
    # Check ingestion failures
    r = mgmt('.show ingestion failures | where FailedOn > ago(5m) | take 10')
    if r:
        tbl = r.get('Tables', [{}])[0]
        cols = [c['ColumnName'] for c in tbl.get('Columns', [])]
        rows = tbl.get('Rows', [])
        if rows:
            for row in rows:
                d = dict(zip(cols, row))
                print(f"  FAILURE: table={d.get('Table')} err={d.get('Details', d.get('ErrorCode',''))[:200]}")
        else:
            print("  No ingestion failures")
    
    # Check function output
    print("\n  Testing ExtractClaimsEvents()...")
    r = kql("ExtractClaimsEvents() | take 3")
    tbl = r.get('Tables', [{}])[0]
    cols = [c['ColumnName'] for c in tbl.get('Columns', [])]
    rows = tbl.get('Rows', [])
    if rows:
        print(f"  Function returns {len(rows)} rows — the function WORKS but policy doesn't trigger it")
        d = dict(zip(cols, rows[0]))
        print(f"  Sample cols: {list(d.keys())[:10]}")
    else:
        print("  Function returns 0 rows")
        # Check what's in source
        r2 = kql("rti_all_events | where _table == 'claims_events' | take 1")
        tbl2 = r2.get('Tables', [{}])[0]
        rows2 = tbl2.get('Rows', [])
        if rows2:
            print(f"  Source HAS claims data but function doesn't match")
            # Show function definition
            r3 = kql(".show function ExtractClaimsEvents")
            tbl3 = r3.get('Tables', [{}])[0]
            cols3 = [c['ColumnName'] for c in tbl3.get('Columns', [])]
            rows3 = tbl3.get('Rows', [])
            if rows3:
                d3 = dict(zip(cols3, rows3[0]))
                print(f"  Function body: {d3.get('Body', '?')[:500]}")
        else:
            print("  Source has no claims data either")
