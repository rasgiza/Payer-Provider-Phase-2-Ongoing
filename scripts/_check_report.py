"""Diagnose: is the report actually in the workspace?"""
import json, subprocess, requests

token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com'],
    capture_output=True, text=True
).stdout)['accessToken']

H = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
WS = '1cee1797-ef2f-4911-b4e5-577eb64bd452'
API = 'https://api.fabric.microsoft.com/v1'
PBI = f'https://api.powerbi.com/v1.0/myorg/groups/{WS}'

# 1) Reports via Fabric API
r = requests.get(f'{API}/workspaces/{WS}/items?type=Report', headers=H)
reports = r.json().get('value', [])
print("=== Reports (Fabric API) ===")
for rpt in reports:
    print(f"  {rpt['displayName']} (id={rpt['id']})")
if not reports:
    print("  (NONE - report is missing!)")

# 2) Reports via Power BI API
r2 = requests.get(f'{PBI}/reports', headers=H)
pbi_reports = r2.json().get('value', [])
print("\n=== Reports (PBI API) ===")
for rpt in pbi_reports:
    ds = rpt.get('datasetId', '?')
    print(f"  {rpt['name']} (id={rpt['id']}, datasetId={ds})")
if not pbi_reports:
    print("  (NONE)")

# 3) Semantic Models
r3 = requests.get(f'{API}/workspaces/{WS}/items?type=SemanticModel', headers=H)
sms = r3.json().get('value', [])
print("\n=== Semantic Models ===")
for sm in sms:
    print(f"  {sm['displayName']} (id={sm['id']})")

# 4) Check ALL items - maybe report is under a different type
r4 = requests.get(f'{API}/workspaces/{WS}/items', headers=H)
all_items = r4.json().get('value', [])
print(f"\n=== All items ({len(all_items)}) ===")
for item in sorted(all_items, key=lambda x: x.get('type', '')):
    print(f"  [{item['type']}] {item['displayName']} ({item['id']})")
