"""Deeper diagnostics: check if Eventstream is flowing again + ingestion failures."""
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

# 1. Check current counts
print("=== Current table counts ===")
for t in ['rti_all_events', 'claims_events', 'adt_events', 'rx_events']:
    r = kql(f'{t} | count')
    cnt = r.get('Tables', [{}])[0].get('Rows', [[0]])[0][0]
    print(f"  {t}: {cnt}")

# 2. Check ingestion failures
print("\n=== Ingestion failures (last hour) ===")
r = mgmt('.show ingestion failures | where FailedOn > ago(1h) | take 10')
tbl = r.get('Tables', [{}])[0]
cols = [c['ColumnName'] for c in tbl.get('Columns', [])]
rows = tbl.get('Rows', [])
if not rows:
    print("  No ingestion failures")
else:
    for row in rows:
        d = dict(zip(cols, row))
        print(f"  Table: {d.get('Table', '?')} | Error: {d.get('Details', d.get('ErrorCode', '?'))[:200]}")

# 3. Check streaming ingestion policy on rti_all_events
print("\n=== Streaming ingestion policy ===")
r = mgmt('.show table rti_all_events policy streamingingestion')
tbl = r.get('Tables', [{}])[0]
cols = [c['ColumnName'] for c in tbl.get('Columns', [])]
rows = tbl.get('Rows', [])
for row in rows:
    d = dict(zip(cols, row))
    print(f"  Entity: {d.get('EntityName', '?')}")
    print(f"  Policy: {d.get('Policy', '?')}")

# 4. If rti_all_events has data, manually test the update policy function
print("\n=== Testing ExtractClaimsEvents() manually ===")
r = kql('ExtractClaimsEvents() | take 3')
tbl = r.get('Tables', [{}])[0]
cols = [c['ColumnName'] for c in tbl.get('Columns', [])]
rows = tbl.get('Rows', [])
if not rows:
    print("  No results from ExtractClaimsEvents()")
    # Check if rti_all_events has claims data
    r2 = kql("rti_all_events | where _table == 'claims_events' | count")
    cnt = r2.get('Tables', [{}])[0].get('Rows', [[0]])[0][0]
    print(f"  Claims rows in rti_all_events: {cnt}")
else:
    print(f"  Got {len(rows)} rows. Columns: {cols}")
    for row in rows[:2]:
        d = dict(zip(cols, row))
        print(f"    event_id={d.get('event_id','?')[:20]} claim_id={d.get('claim_id','?')} event_type={d.get('event_type','?')}")

# 5. Try manually running .set-or-append to test if update policies work with batch
print("\n=== Testing batch ingest to trigger update policy ===")
r = mgmt(".set-or-append rti_all_events <| rti_all_events | take 1")
tbl = r.get('Tables', [{}])[0]
cols = [c['ColumnName'] for c in tbl.get('Columns', [])]
rows = tbl.get('Rows', [])
if rows:
    d = dict(zip(cols, rows[0]))
    print(f"  Batch ingest result: {d.get('ExtentId', 'ok')}")
else:
    # Check for error
    errs = r.get('Exceptions', [])
    if errs:
        print(f"  Error: {errs[0][:200]}")
    else:
        print(f"  Raw: {json.dumps(r)[:300]}")

# Wait a moment for policy to fire
time.sleep(3)

# Check typed tables again
print("\n=== Typed table counts after batch ingest ===")
for t in ['claims_events', 'adt_events', 'rx_events']:
    r = kql(f'{t} | count')
    cnt = r.get('Tables', [{}])[0].get('Rows', [[0]])[0][0]
    print(f"  {t}: {cnt}")
