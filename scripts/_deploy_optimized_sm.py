"""Push updated TMDL measures to Fabric HealthcareDemoHLS semantic model."""
import json, requests, time, subprocess, base64, os

# --- Auth ---
result = subprocess.run(
    ["cmd", "/c", "az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com"],
    capture_output=True, text=True
)
token = json.loads(result.stdout)["accessToken"]
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
API = "https://api.fabric.microsoft.com/v1"
WS = "d6ed5901-0f1d-4a5c-a263-e5f857169a79"
SM_ID = "71c59890-00c5-489d-bfbd-147c915843e0"

# --- Read all TMDL files ---
SM_DIR = os.path.join(os.path.dirname(__file__), "..", "workspace", "HealthcareDemoHLS.SemanticModel", "definition")

parts = []
for root, dirs, files in os.walk(SM_DIR):
    for fname in files:
        if fname.endswith(".tmdl") or fname.endswith(".json"):
            full_path = os.path.join(root, fname)
            rel_path = os.path.relpath(full_path, SM_DIR).replace("\\", "/")
            with open(full_path, "r", encoding="utf-8") as f:
                content = f.read()
            b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")
            parts.append({"path": rel_path, "payload": b64, "payloadType": "InlineBase64"})

print(f"  Collected {len(parts)} definition files")

# --- Patch expressions.tmdl with lakehouse URL ---
LH_ID = "690b445b-de94-4531-8e1c-1e0d38e1ab37"
EXPR_URL = f"\"Provider=PowerBI.Mashup;Location=Lakehouse;Linked Server={LH_ID};Linked Database={LH_ID}\""
for part in parts:
    if part["path"] == "expressions.tmdl":
        decoded = base64.b64decode(part["payload"]).decode("utf-8")
        import re
        decoded = re.sub(
            r'"Provider=PowerBI\.Mashup;Location=Lakehouse[^"]*"',
            EXPR_URL,
            decoded
        )
        part["payload"] = base64.b64encode(decoded.encode("utf-8")).decode("ascii")
        print("  Patched expressions.tmdl with lakehouse URL")
        break

# --- Update Definition ---
body = {"definition": {"parts": parts}}
url = f"{API}/workspaces/{WS}/semanticModels/{SM_ID}/updateDefinition"
print(f"  Posting updateDefinition ({len(parts)} parts)...")
resp = requests.post(url, headers=headers, json=body)
print(f"  HTTP {resp.status_code}")

if resp.status_code == 202:
    op_url = resp.headers.get("Location") or resp.headers.get("Operation-Location")
    if op_url:
        for i in range(60):
            time.sleep(3)
            r = requests.get(op_url, headers=headers)
            if r.status_code == 200:
                state = r.json().get("status", "")
                if state in ("Succeeded", "Completed"):
                    print(f"  Update succeeded!")
                    break
                elif state in ("Failed", "Error"):
                    print(f"  Update FAILED: {r.text[:500]}")
                    break
                elif i % 5 == 0:
                    print(f"    ... {state}")
        else:
            print("  Timed out waiting for update")
    else:
        print("  No operation URL in headers — check workspace manually")
elif resp.status_code == 200:
    print("  Update succeeded (sync)!")
else:
    print(f"  Error: {resp.text[:500]}")

# --- Trigger Refresh ---
print("\n  Triggering SM refresh...")
result2 = subprocess.run(
    ["cmd", "/c", "az", "account", "get-access-token", "--resource", "https://analysis.windows.net/powerbi/api"],
    capture_output=True, text=True
)
pbi_token = json.loads(result2.stdout)["accessToken"]
pbi_headers = {"Authorization": f"Bearer {pbi_token}", "Content-Type": "application/json"}
PBI_API = f"https://api.powerbi.com/v1.0/myorg/groups/{WS}"

# Find dataset ID
r = requests.get(f"{PBI_API}/datasets", headers=pbi_headers)
ds_id = None
for ds in r.json().get("value", []):
    if ds["name"] == "HealthcareDemoHLS":
        ds_id = ds["id"]
        break

if ds_id:
    rr = requests.post(f"{PBI_API}/datasets/{ds_id}/refreshes", headers=pbi_headers, json={"type": "Full"})
    print(f"  Refresh HTTP {rr.status_code}")
    if rr.status_code == 202:
        print("  Refresh started — data will be available in ~30s")
    else:
        print(f"  {rr.text[:300]}")
else:
    print("  Dataset not found for refresh")
