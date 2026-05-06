"""Diagnose: show actual rti_all_events schema on new Eventhouse and sample data."""
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

# Show rti_all_events schema
print("\n=== rti_all_events SCHEMA ===")
result = client.execute_mgmt(DB, ".show table rti_all_events schema as json")
for row in result.primary_results[0]:
    schema = json.loads(row[1])
    for c in schema.get("OrderedColumns", []):
        print(f"  {c['Name']:35s} {c['CslType']}")

# Show all tables
print("\n=== ALL TABLES ===")
result = client.execute_mgmt(DB, ".show tables")
for row in result.primary_results[0]:
    print(f"  {row[0]}")

# Sample row
print("\n=== SAMPLE ROW (first 1) ===")
result = client.execute(DB, "rti_all_events | take 1")
cols = [c.column_name for c in result.primary_results[0].columns]
for row in result.primary_results[0]:
    for i, col in enumerate(cols):
        print(f"  {col:35s} = {row[i]}")

# Check _table values
print("\n=== _table DISTINCT VALUES ===")
try:
    result = client.execute(DB, "rti_all_events | summarize count() by _table")
    for row in result.primary_results[0]:
        print(f"  {row[0]:25s} : {row[1]} rows")
except Exception as e:
    print(f"  ERROR: {e}")
    # Maybe _table doesn't exist - check with getschema
    result = client.execute(DB, "rti_all_events | getschema | where ColumnName contains 'table'")
    for row in result.primary_results[0]:
        print(f"  Found column: {row[0]}")
