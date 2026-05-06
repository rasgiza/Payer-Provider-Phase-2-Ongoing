"""
Fix the Eventstream by switching from ProcessedIngestion to DirectIngestion mode.

ProcessedIngestion has TWO fundamental problems:
1. It caches the column mapping from first use - new columns aren't mapped
2. It may use a streaming path that doesn't trigger update policies

DirectIngestion mode:
- Uses the KQL table's JSON ingestion mapping to map ALL fields
- Uses native KQL batch ingestion which DOES trigger update policies
- This is the permanent fix for both issues
"""
import requests, json, base64, time
from azure.identity import InteractiveBrowserCredential

WORKSPACE_NAME = "IQFinalDemo"
ES_NAME = "Healthcare_RTI_Eventstream"
FABRIC_BASE = "https://api.fabric.microsoft.com/v1"

cred = InteractiveBrowserCredential()
token = cred.get_token("https://analysis.windows.net/powerbi/api/.default").token
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Discover workspace
print("Discovering workspace...")
resp = requests.get(f"{FABRIC_BASE}/workspaces", headers=headers)
resp.raise_for_status()
ws = next(w for w in resp.json()["value"] if w["displayName"] == WORKSPACE_NAME)
WS_ID = ws["id"]
api = f"{FABRIC_BASE}/workspaces/{WS_ID}"

# Get all items
resp = requests.get(f"{api}/items", headers=headers)
resp.raise_for_status()
items = resp.json()["value"]

es_id = None
kql_db_id = None
kql_db_name = "Healthcare_RTI_DB"
bronze_lh_id = None

for it in items:
    if it["type"] == "Eventstream" and it["displayName"] == ES_NAME:
        es_id = it["id"]
    if it["type"] == "KQLDatabase":
        kql_db_id = it["id"]
        kql_db_name = it["displayName"]
    if it["type"] == "Lakehouse" and "bronze" in it["displayName"].lower():
        bronze_lh_id = it["id"]

print(f"  Workspace: {WS_ID}")
print(f"  Eventstream: {es_id}")
print(f"  KQL DB: {kql_db_name} ({kql_db_id})")
print(f"  Bronze LH: {bronze_lh_id}")

if not es_id or not kql_db_id:
    print("ERROR: Missing items")
    exit(1)

# Build updated definition with DirectIngestion
print("\nBuilding Eventstream definition with DirectIngestion...")

_es_sources = [{
    "name": "HealthcareCustomEndpoint",
    "type": "CustomEndpoint",
    "properties": {}
}]

_es_streams = [{
    "name": "HealthcareRTI-stream",
    "type": "DefaultStream",
    "properties": {},
    "inputNodes": [{"name": "HealthcareCustomEndpoint"}]
}]

_es_destinations = [{
    "name": "HealthcareEventhouse",
    "type": "Eventhouse",
    "properties": {
        "dataIngestionMode": "DirectIngestion",
        "workspaceId": WS_ID,
        "itemId": kql_db_id,
        "databaseName": kql_db_name,
        "tableName": "rti_all_events",
        "inputSerialization": {"type": "Json", "properties": {"encoding": "UTF8"}}
    },
    "inputNodes": [{"name": "HealthcareRTI-stream"}]
}]
print(f"  + Eventhouse: DirectIngestion → rti_all_events")

if bronze_lh_id:
    _es_destinations.append({
        "name": "BronzeLakehouse",
        "type": "Lakehouse",
        "properties": {
            "workspaceId": WS_ID,
            "itemId": bronze_lh_id,
            "schema": "",
            "deltaTable": "rti_raw_events",
            "minimumRows": 1000,
            "maximumDurationInSeconds": 120,
            "inputSerialization": {"type": "Json", "properties": {"encoding": "UTF8"}}
        },
        "inputNodes": [{"name": "HealthcareRTI-stream"}]
    })
    print(f"  + Lakehouse: BronzeLakehouse → rti_raw_events")

_es_def = {
    "sources": _es_sources,
    "destinations": _es_destinations,
    "streams": _es_streams,
    "operators": [],
    "compatibilityLevel": "1.1"
}

# Push definition
print("\nPushing updated Eventstream definition...")
_es_json_b64 = base64.b64encode(json.dumps(_es_def, indent=2).encode()).decode()
_props_b64 = base64.b64encode(json.dumps({
    "retentionTimeInDays": 1, "eventThroughputLevel": "Low"
}).encode()).decode()
_platform_b64 = base64.b64encode(json.dumps({
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
    "metadata": {"type": "Eventstream", "displayName": ES_NAME},
    "config": {"version": "2.0", "logicalId": es_id}
}).encode()).decode()

_update_body = {"definition": {"parts": [
    {"path": "eventstream.json", "payload": _es_json_b64, "payloadType": "InlineBase64"},
    {"path": "eventstreamProperties.json", "payload": _props_b64, "payloadType": "InlineBase64"},
    {"path": ".platform", "payload": _platform_b64, "payloadType": "InlineBase64"},
]}}

r = requests.post(
    f"{api}/eventstreams/{es_id}/updateDefinition?updateMetadata=true",
    headers=headers, json=_update_body
)
print(f"  updateDefinition: HTTP {r.status_code}")

if r.status_code == 200:
    print("  SUCCESS: Eventstream switched to DirectIngestion!")
elif r.status_code == 202:
    loc = r.headers.get("Location", "")
    retry = int(r.headers.get("Retry-After", "2"))
    print(f"  LRO in progress...")
    for _ in range(30):
        time.sleep(retry)
        poll = requests.get(loc, headers=headers)
        if poll.status_code == 200:
            body = poll.json()
            status = body.get("status", "")
            if status == "Succeeded":
                print("  SUCCESS: Eventstream switched to DirectIngestion!")
                break
            elif status == "Failed":
                print(f"  FAILED: {body.get('error', body)}")
                break
        elif poll.status_code != 202:
            break
else:
    print(f"  Response: {r.text[:500]}")
    # If DirectIngestion fails, try without the mode (let Fabric default)
    print("\n  Trying without explicit mode (Fabric default)...")
    _es_destinations[0]["properties"].pop("dataIngestionMode", None)
    _es_def["destinations"] = _es_destinations
    _es_json_b64 = base64.b64encode(json.dumps(_es_def, indent=2).encode()).decode()
    _update_body["definition"]["parts"][0]["payload"] = _es_json_b64
    r2 = requests.post(
        f"{api}/eventstreams/{es_id}/updateDefinition?updateMetadata=true",
        headers=headers, json=_update_body
    )
    print(f"  updateDefinition (no mode): HTTP {r2.status_code}")
    if r2.status_code == 200:
        print("  SUCCESS: Eventstream updated (default mode)!")
    else:
        print(f"  Response: {r2.text[:300]}")

print("\nDone. The Eventstream will need ~30-60 seconds to restart.")
print("After that, you'll need to configure the Direct Ingestion mapping in the portal")
print("(Live View → Configure on the Eventhouse destination node)")
print("OR the existing rti_all_events_mapping will be used automatically.")
