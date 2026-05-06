from azure.identity import InteractiveBrowserCredential
from azure.kusto.data import KustoConnectionStringBuilder, KustoClient
cred = InteractiveBrowserCredential()
token = cred.get_token("https://kusto.kusto.windows.net/.default").token
uri = "https://trd-8ahv0svs43af8c5324.z9.kusto.fabric.microsoft.com"
kcsb = KustoConnectionStringBuilder.with_aad_user_token_authentication(uri, token)
client = KustoClient(kcsb)
DB = "Healthcare_RTI_DB"

# Check if claim_id has data
result = client.execute(DB, 'claims_events | where isnotempty(claim_id) | count')
for row in result.primary_results[0]:
    print(f"claims WITH claim_id: {row[0]}")

result = client.execute(DB, 'claims_events | where isempty(claim_id) | count')
for row in result.primary_results[0]:
    print(f"claims WITHOUT claim_id: {row[0]}")

# Sample a claims row
print("\n--- Sample claims_events row ---")
result = client.execute(DB, "claims_events | take 1")
cols = [c.column_name for c in result.primary_results[0].columns]
for row in result.primary_results[0]:
    for i, col in enumerate(cols):
        print(f"  {col:30s} = {row[i]}")

# Check rti_all_events - do claims rows have claim_id in source?
print("\n--- rti_all_events claims rows - claim_id ---")
result = client.execute(DB, 'rti_all_events | where _table == "claims_events" | where isnotempty(claim_id) | count')
for row in result.primary_results[0]:
    print(f"rti_all_events claims WITH claim_id: {row[0]}")
result = client.execute(DB, 'rti_all_events | where _table == "claims_events" | where isempty(claim_id) | count')
for row in result.primary_results[0]:
    print(f"rti_all_events claims WITHOUT claim_id: {row[0]}")
