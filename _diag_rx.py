"""Diagnose rx_events schema mismatch."""
from azure.identity import InteractiveBrowserCredential
from azure.kusto.data import KustoConnectionStringBuilder, KustoClient
import json

cred = InteractiveBrowserCredential()
token = cred.get_token("https://kusto.kusto.windows.net/.default").token
uri = "https://trd-4smkxn6uvq82yb11q4.z0.kusto.fabric.microsoft.com"
kcsb = KustoConnectionStringBuilder.with_aad_user_token_authentication(uri, token)
client = KustoClient(kcsb)
DB = "Healthcare_RTI_DB"

print("=== rx_events TABLE SCHEMA ===")
result = client.execute_mgmt(DB, ".show table rx_events schema as json")
for row in result.primary_results[0]:
    schema = json.loads(row[1])
    for c in schema.get("OrderedColumns", []):
        print(f"  {c['Name']:30s} {c['CslType']}")

print("\n=== ExtractRxEvents() OUTPUT SCHEMA ===")
result = client.execute(DB, "ExtractRxEvents() | getschema")
for row in result.primary_results[0]:
    print(f"  {row[0]:30s} {row[2]}")
