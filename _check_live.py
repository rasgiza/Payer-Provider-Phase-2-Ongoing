"""Quick check: Does new Eventstream data have claim_id? Are update policies working?"""
import time
from azure.identity import InteractiveBrowserCredential
from azure.kusto.data import KustoConnectionStringBuilder, KustoClient

cred = InteractiveBrowserCredential()
token = cred.get_token("https://kusto.kusto.windows.net/.default").token
uri = "https://trd-8ahv0svs43af8c5324.z9.kusto.fabric.microsoft.com"
kcsb = KustoConnectionStringBuilder.with_aad_user_token_authentication(uri, token)
client = KustoClient(kcsb)
DB = "Healthcare_RTI_DB"

# Check new data quality
print("=== New data in rti_all_events ===")
result = client.execute(DB, "rti_all_events | summarize count() by _table")
for row in result.primary_results[0]:
    print(f"  {row[0]:20s} : {row[1]} rows")

print("\n=== Sample claims row from rti_all_events ===")
result = client.execute(DB, 'rti_all_events | where _table == "claims_events" | take 1')
cols = [c.column_name for c in result.primary_results[0].columns]
for row in result.primary_results[0]:
    for i, col in enumerate(cols):
        val = row[i]
        if val is not None and val != "" and val != 0 and val != 0.0:
            print(f"  {col:30s} = {val}")

# Check typed table counts (update policies may have a delay)
print("\n=== Typed table counts ===")
for t in ["claims_events", "adt_events", "rx_events"]:
    result = client.execute(DB, f"{t} | count")
    for row in result.primary_results[0]:
        print(f"  {t:25s} : {int(row[0]):,} rows")

# Check update policy details
print("\n=== Update policy details ===")
result = client.execute_mgmt(DB, ".show table claims_events policy update")
cols = [c.column_name for c in result.primary_results[0].columns]
for row in result.primary_results[0]:
    for i, col in enumerate(cols):
        print(f"  {col}: {row[i]}")
