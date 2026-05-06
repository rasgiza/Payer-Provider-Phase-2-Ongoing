"""Diagnostic: show rti_all_events schema and a sample row."""
from azure.identity import InteractiveBrowserCredential
from azure.kusto.data import KustoConnectionStringBuilder, KustoClient
import json

cred = InteractiveBrowserCredential()
token = cred.get_token("https://kusto.kusto.windows.net/.default").token
uri = "https://trd-4smkxn6uvq82yb11q4.z0.kusto.fabric.microsoft.com"
kcsb = KustoConnectionStringBuilder.with_aad_user_token_authentication(uri, token)
client = KustoClient(kcsb)
DB = "Healthcare_RTI_DB"

print("=== rti_all_events SCHEMA ===")
result = client.execute_mgmt(DB, ".show table rti_all_events schema as json")
for row in result.primary_results[0]:
    schema = json.loads(row[1])
    cols = schema.get("OrderedColumns", [])
    for c in cols:
        print(f"  {c['Name']:30s} {c['CslType']}")

print("\n=== SAMPLE ROW (claims) ===")
result = client.execute(DB, 'rti_all_events | where _table == "claims_events" | take 1')
cols = [c.column_name for c in result.primary_results[0].columns]
for row in result.primary_results[0]:
    for i, col in enumerate(cols):
        print(f"  {col:30s} = {row[i]}")

print("\n=== SAMPLE ROW (adt) ===")
result = client.execute(DB, 'rti_all_events | where _table == "adt_events" | take 1')
cols = [c.column_name for c in result.primary_results[0].columns]
for row in result.primary_results[0]:
    for i, col in enumerate(cols):
        print(f"  {col:30s} = {row[i]}")

print("\n=== SAMPLE ROW (rx) ===")
result = client.execute(DB, 'rti_all_events | where _table == "rx_events" | take 1')
cols = [c.column_name for c in result.primary_results[0].columns]
for row in result.primary_results[0]:
    for i, col in enumerate(cols):
        print(f"  {col:30s} = {row[i]}")
