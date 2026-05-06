"""Check if the rewire fixed column mapping and update policy triggering."""
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

# Clear old data
print('Clearing old data...')
mgmt('.clear table rti_all_events data')
mgmt('.clear table claims_events data')
mgmt('.clear table adt_events data')
mgmt('.clear table rx_events data')
print('  Done.')

print('Waiting 45 seconds for fresh Eventstream data...')
time.sleep(45)

# Check new data
print('\n=== New data sample ===')
result = kql('rti_all_events | take 5 | project event_id, event_type, _table, claim_id, facility_id, diagnosis_code, claim_amount, medication_name, quantity')
cols = result.get('Tables', [{}])[0].get('Columns', [])
rows = result.get('Tables', [{}])[0].get('Rows', [])
col_names = [c['ColumnName'] for c in cols]
for row in rows:
    d = dict(zip(col_names, row))
    etype = d.get('event_type', '?')
    tbl = d.get('_table', '?')
    print(f"  {etype:20s} _table={tbl}")
    print(f"    claim_id={d.get('claim_id')} facility={d.get('facility_id')} diag={d.get('diagnosis_code')} amt={d.get('claim_amount')}")
    print(f"    med={d.get('medication_name')} qty={d.get('quantity')}")

# Check counts
print('\n=== Counts ===')
r2 = kql('rti_all_events | count')
cnt = r2.get('Tables', [{}])[0].get('Rows', [[0]])[0][0]
print(f'  rti_all_events: {cnt}')

for t in ['claims_events', 'adt_events', 'rx_events']:
    r3 = kql(f'{t} | count')
    c = r3.get('Tables', [{}])[0].get('Rows', [[0]])[0][0]
    print(f'  {t}: {c}')

if cnt == 0:
    print('\n  No data yet - Eventstream may still be restarting. Try again in a minute.')
