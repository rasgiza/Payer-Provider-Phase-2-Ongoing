"""Fix rx_events: toint() casts + backfill."""
from azure.identity import InteractiveBrowserCredential
from azure.kusto.data import KustoConnectionStringBuilder, KustoClient

cred = InteractiveBrowserCredential()
token = cred.get_token("https://kusto.kusto.windows.net/.default").token
uri = "https://trd-4smkxn6uvq82yb11q4.z0.kusto.fabric.microsoft.com"
kcsb = KustoConnectionStringBuilder.with_aad_user_token_authentication(uri, token)
client = KustoClient(kcsb)
DB = "Healthcare_RTI_DB"

# Fix function with toint() casts
cmd = """.create-or-alter function ExtractRxEvents() {
    rti_all_events
    | where _table == "rx_events"
    | project event_id,
              event_timestamp = todatetime(event_timestamp),
              event_type,
              patient_id,
              provider_id,
              medication_code = coalesce(medication_code, ""),
              medication_name = coalesce(medication_name, ""),
              drug_class = coalesce(drug_class, ""),
              quantity = toint(coalesce(quantity, 0)),
              days_supply = toint(coalesce(days_supply, 0)),
              latitude,
              longitude
}"""
client.execute_mgmt(DB, cmd)
print("OK: ExtractRxEvents fixed with toint() casts")

# Set update policy
cmd2 = """.alter table rx_events policy update @'[{"IsEnabled": true, "Source": "rti_all_events", "Query": "ExtractRxEvents()", "IsTransactional": false, "PropagateIngestionProperties": true}]'"""
client.execute_mgmt(DB, cmd2)
print("OK: rx_events update policy set")

# Backfill
client.execute_mgmt(DB, ".set-or-append rx_events <| ExtractRxEvents()")
print("OK: rx_events backfilled")

# Verify
result = client.execute(DB, "rx_events | count")
for row in result.primary_results[0]:
    print(f"rx_events: {int(row[0]):,} rows")
