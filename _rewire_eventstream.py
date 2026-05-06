"""
Re-wire the Eventstream destination to force it to re-discover the full
rti_all_events schema (including claim_id, facility_id, etc.).

The Eventstream's ProcessedIngestion mode locks its column mapping at wire-time.
If columns are added to the KQL table AFTER the Eventstream was wired, it ignores them.
Updating the Eventstream definition forces a re-bind.
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
name_to_id = {(it["type"], it["displayName"]): it["id"] for it in items}

es_id = name_to_id.get(("Eventstream", ES_NAME))
kql_db_id = None
kql_db_name = "Healthcare_RTI_DB"
for it in items:
    if it["type"] == "KQLDatabase":
        kql_db_id = it["id"]
        kql_db_name = it["displayName"]
        break

if not es_id:
    print(f"ERROR: Eventstream '{ES_NAME}' not found")
    exit(1)
if not kql_db_id:
    print("ERROR: KQL Database not found")
    exit(1)

print(f"  Eventstream: {es_id}")
print(f"  KQL DB: {kql_db_name} ({kql_db_id})")

# Get current Eventstream topology to see what's there
print("\nGetting current Eventstream topology...")
topo_resp = requests.get(f"{api}/eventstreams/{es_id}/topology", headers=headers)
if topo_resp.status_code == 200:
    topo = topo_resp.json()
    print(f"  Sources: {[s.get('name') for s in topo.get('sources', [])]}")
    print(f"  Streams: {[s.get('name') for s in topo.get('streams', [])]}")
    print(f"  Destinations: {[d.get('name') for d in topo.get('destinations', [])]}")
else:
    print(f"  Could not get topology: HTTP {topo_resp.status_code}")

# Get current Eventstream definition
print("\nGetting current Eventstream definition...")
def_resp = requests.post(f"{api}/eventstreams/{es_id}/getDefinition", headers=headers)
if def_resp.status_code == 202:
    loc = def_resp.headers.get("Location", "")
    retry = int(def_resp.headers.get("Retry-After", "2"))
    for _ in range(30):
        time.sleep(retry)
        poll = requests.get(loc, headers=headers)
        if poll.status_code == 200:
            body = poll.json()
            if body.get("status") == "Succeeded":
                def_resp = poll
                break
            elif body.get("status") in ("Running", "NotStarted"):
                continue
        break

current_def = None
if def_resp.status_code == 200:
    body = def_resp.json()
    parts = body.get("definition", body).get("parts", [])
    for part in parts:
        if part.get("path") == "eventstream.json":
            payload = part.get("payload", "")
            current_def = json.loads(base64.b64decode(payload))
            break

if current_def:
    print(f"  Got definition with {len(current_def.get('destinations', []))} destinations")
    for d in current_def.get("destinations", []):
        print(f"    - {d['name']} ({d['type']}): table={d.get('properties', {}).get('tableName', 'N/A')}")
else:
    print("  Could not retrieve definition, will build fresh")

# Build updated definition with explicit ingestion mapping
print("\nBuilding updated Eventstream definition...")

# Use current definition if available, otherwise build fresh
if current_def:
    es_def = current_def
    # Update the Eventhouse destination to reference the mapping
    for dest in es_def.get("destinations", []):
        if dest.get("type") == "Eventhouse" and dest.get("properties", {}).get("tableName") == "rti_all_events":
            # Force re-bind by touching the definition
            dest["properties"]["tableName"] = "rti_all_events"
            # Add ingestion mapping reference if supported
            dest["properties"]["ingestionMappingName"] = "rti_all_events_mapping"
            print(f"  Updated destination: {dest['name']} with mapping reference")
else:
    es_def = {
        "sources": [{
            "name": "HealthcareCustomEndpoint",
            "type": "CustomEndpoint",
            "properties": {}
        }],
        "streams": [{
            "name": "HealthcareRTI-stream",
            "type": "DefaultStream",
            "properties": {},
            "inputNodes": [{"name": "HealthcareCustomEndpoint"}]
        }],
        "destinations": [{
            "name": "HealthcareEventhouse",
            "type": "Eventhouse",
            "properties": {
                "dataIngestionMode": "ProcessedIngestion",
                "workspaceId": WS_ID,
                "itemId": kql_db_id,
                "databaseName": kql_db_name,
                "tableName": "rti_all_events",
                "ingestionMappingName": "rti_all_events_mapping",
                "inputSerialization": {"type": "Json", "properties": {"encoding": "UTF8"}}
            },
            "inputNodes": [{"name": "HealthcareRTI-stream"}]
        }],
        "operators": [],
        "compatibilityLevel": "1.1"
    }
    print("  Built fresh definition with mapping reference")

# Push updated definition
print("\nPushing updated Eventstream definition...")
es_json_b64 = base64.b64encode(json.dumps(es_def, indent=2).encode()).decode()
props_b64 = base64.b64encode(json.dumps({
    "retentionTimeInDays": 1, "eventThroughputLevel": "Low"
}).encode()).decode()
platform_b64 = base64.b64encode(json.dumps({
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
    "metadata": {"type": "Eventstream", "displayName": ES_NAME},
    "config": {"version": "2.0", "logicalId": es_id}
}).encode()).decode()

update_body = {"definition": {"parts": [
    {"path": "eventstream.json", "payload": es_json_b64, "payloadType": "InlineBase64"},
    {"path": "eventstreamProperties.json", "payload": props_b64, "payloadType": "InlineBase64"},
    {"path": ".platform", "payload": platform_b64, "payloadType": "InlineBase64"},
]}}

r = requests.post(
    f"{api}/eventstreams/{es_id}/updateDefinition?updateMetadata=true",
    headers=headers, json=update_body
)
print(f"  updateDefinition: HTTP {r.status_code}")

if r.status_code == 200:
    print("  Eventstream definition updated!")
elif r.status_code == 202:
    # LRO
    loc = r.headers.get("Location", "")
    retry = int(r.headers.get("Retry-After", "2"))
    print(f"  LRO in progress (polling {retry}s)...")
    for _ in range(30):
        time.sleep(retry)
        poll = requests.get(loc, headers=headers)
        if poll.status_code == 200:
            body = poll.json()
            status = body.get("status", "")
            if status == "Succeeded":
                print("  Eventstream definition updated!")
                break
            elif status == "Failed":
                print(f"  FAILED: {body.get('error', body)}")
                break
        elif poll.status_code != 202:
            print(f"  LRO unexpected: HTTP {poll.status_code}")
            break
else:
    print(f"  ERROR: {r.text[:300]}")

print("\nDone. Wait ~30 seconds for Eventstream to restart, then check data quality.")
