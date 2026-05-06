"""
Test update policies by manually ingesting a synthetic row into rti_all_events.
This confirms whether the update policy mechanism itself works (independent of Eventstream).
"""
import requests, json, time
from azure.identity import InteractiveBrowserCredential

cred = InteractiveBrowserCredential()
token = cred.get_token('https://trd-8ahv0svs43af8c5324.z9.kusto.fabric.microsoft.com/.default').token
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
URI = 'https://trd-8ahv0svs43af8c5324.z9.kusto.fabric.microsoft.com'
DB = 'Healthcare_RTI_DB'

def kql(q):
    r = requests.post(f'{URI}/v1/rest/query', headers=headers, json={'db': DB, 'csl': q})
    return r.json()

def mgmt(cmd):
    r = requests.post(f'{URI}/v1/rest/mgmt', headers=headers, json={'db': DB, 'csl': cmd})
    return r.json()

# 1. Check current typed table counts
print("=== Before: typed table counts ===")
for t in ['claims_events', 'adt_events', 'rx_events']:
    r = kql(f'{t} | count')
    cnt = r.get('Tables', [{}])[0].get('Rows', [[0]])[0][0]
    print(f"  {t}: {cnt}")

# 2. Insert a synthetic claims row using .set-or-append with inline datatable
print("\n=== Inserting synthetic claims row into rti_all_events ===")
ingest_cmd = """.set-or-append rti_all_events <|
    print event_id="TEST-CLAIM-001",
          event_timestamp="2026-04-28T16:00:00Z",
          event_type="CLAIM_SUBMITTED",
          patient_id="PAT_TEST_001",
          provider_id="PRV_TEST_001",
          latitude=42.33,
          longitude=-83.05,
          _table="claims_events",
          claim_id="CLM_TEST_001",
          facility_id="FAC_TEST_001",
          facility_name="Test Hospital",
          payer_id="PAY_TEST_001",
          diagnosis_code="J18.9",
          procedure_code="99213",
          claim_type="Professional",
          claim_amount=1500.00,
          admission_type="",
          primary_diagnosis="Pneumonia",
          has_open_care_gaps="true",
          open_gap_measures="HbA1c",
          injected_fraud_flags="",
          medication_name="",
          dosage="",
          quantity=0,
          days_supply=0,
          pharmacy_id=""
"""
r = mgmt(ingest_cmd)
tbl = r.get('Tables', [{}])[0]
cols = [c['ColumnName'] for c in tbl.get('Columns', [])]
rows = tbl.get('Rows', [])
if rows:
    d = dict(zip(cols, rows[0]))
    print(f"  Ingest OK: ExtentId={d.get('ExtentId', '?')}")
else:
    errs = r.get('Exceptions', [])
    if errs:
        print(f"  ERROR: {errs[0][:300]}")
    else:
        print(f"  Raw response: {json.dumps(r)[:500]}")
    exit(1)

# 3. Also insert a synthetic ADT and RX row
print("\n=== Inserting synthetic ADT row ===")
adt_cmd = """.set-or-append rti_all_events <|
    print event_id="TEST-ADT-001",
          event_timestamp="2026-04-28T16:01:00Z",
          event_type="ADMISSION",
          patient_id="PAT_TEST_002",
          provider_id="PRV_TEST_002",
          latitude=42.34,
          longitude=-83.04,
          _table="adt_events",
          claim_id="",
          facility_id="FAC_TEST_002",
          facility_name="Test ER",
          payer_id="",
          diagnosis_code="",
          procedure_code="",
          claim_type="",
          claim_amount=0.0,
          admission_type="Emergency",
          primary_diagnosis="Chest Pain",
          has_open_care_gaps="",
          open_gap_measures="",
          injected_fraud_flags="",
          medication_name="",
          dosage="",
          quantity=0,
          days_supply=0,
          pharmacy_id=""
"""
r = mgmt(adt_cmd)
rows = r.get('Tables', [{}])[0].get('Rows', [])
print(f"  {'OK' if rows else 'FAILED'}")

print("\n=== Inserting synthetic RX row ===")
rx_cmd = """.set-or-append rti_all_events <|
    print event_id="TEST-RX-001",
          event_timestamp="2026-04-28T16:02:00Z",
          event_type="PRESCRIPTION_FILLED",
          patient_id="PAT_TEST_003",
          provider_id="PRV_TEST_003",
          latitude=42.35,
          longitude=-83.03,
          _table="rx_events",
          claim_id="",
          facility_id="",
          facility_name="",
          payer_id="",
          diagnosis_code="",
          procedure_code="",
          claim_type="",
          claim_amount=0.0,
          admission_type="",
          primary_diagnosis="",
          has_open_care_gaps="",
          open_gap_measures="",
          injected_fraud_flags="",
          medication_name="Metformin",
          dosage="500mg",
          quantity=60,
          days_supply=30,
          pharmacy_id="PHR_TEST_001"
"""
r = mgmt(rx_cmd)
rows = r.get('Tables', [{}])[0].get('Rows', [])
print(f"  {'OK' if rows else 'FAILED'}")

# 4. Wait for update policies to fire
print("\n  Waiting 5 seconds for update policies to fire...")
time.sleep(5)

# 5. Check typed tables
print("\n=== After: typed table counts ===")
for t in ['claims_events', 'adt_events', 'rx_events']:
    r = kql(f'{t} | count')
    cnt = r.get('Tables', [{}])[0].get('Rows', [[0]])[0][0]
    status = "OK!" if cnt > 0 else "STILL 0 - policy not firing"
    print(f"  {t}: {cnt} — {status}")

# 6. If claims_events has data, show the row
r = kql("claims_events | take 1")
tbl = r.get('Tables', [{}])[0]
cols = [c['ColumnName'] for c in tbl.get('Columns', [])]
rows = tbl.get('Rows', [])
if rows:
    d = dict(zip(cols, rows[0]))
    print(f"\n=== Sample claims_events row ===")
    for k, v in d.items():
        if v not in (None, "", 0, 0.0):
            print(f"  {k}: {v}")
else:
    print("\n  claims_events still empty - checking ingestion failures...")
    r = mgmt('.show ingestion failures | where FailedOn > ago(5m) | take 5')
    tbl = r.get('Tables', [{}])[0]
    cols = [c['ColumnName'] for c in tbl.get('Columns', [])]
    rows = tbl.get('Rows', [])
    if rows:
        for row in rows:
            d = dict(zip(cols, row))
            print(f"  Failure: {d.get('Table','?')} — {d.get('Details', d.get('ErrorCode','?'))[:200]}")
    else:
        print("  No ingestion failures detected")
        # Check if the function itself works
        print("\n  Testing ExtractClaimsEvents() directly...")
        r = kql("ExtractClaimsEvents() | take 3")
        tbl = r.get('Tables', [{}])[0]
        cols = [c['ColumnName'] for c in tbl.get('Columns', [])]
        rows = tbl.get('Rows', [])
        if rows:
            print(f"  Function returns {len(rows)} rows — policy not being triggered")
            d = dict(zip(cols, rows[0]))
            print(f"  Sample: event_id={d.get('event_id')} claim_id={d.get('claim_id')}")
        else:
            print("  Function returns 0 rows — check function logic")
            # Show what's in rti_all_events
            r2 = kql("rti_all_events | where _table == 'claims_events' | take 1 | project event_id, _table, claim_id")
            tbl2 = r2.get('Tables', [{}])[0]
            cols2 = [c['ColumnName'] for c in tbl2.get('Columns', [])]
            rows2 = tbl2.get('Rows', [])
            if rows2:
                d2 = dict(zip(cols2, rows2[0]))
                print(f"  rti_all_events has: {d2}")
