"""Check what's actually inside the deployed report via PBI API."""
import requests, json, base64, time
from azure.identity import InteractiveBrowserCredential

WORKSPACE_ID = "d6ed5901-0f1d-4a5c-a263-e5f857169a79"
REPORT_ID = "454849b3-c607-4ae2-90cf-4c48592c8da0"
SM_ID = "da289586-aff9-4aec-8def-7145ce008e3c"

cred = InteractiveBrowserCredential(login_hint="admin@MngEnvMCAP661056.onmicrosoft.com")
token = cred.get_token("https://analysis.windows.net/powerbi/api/.default").token
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

FABRIC = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}"
PBI = f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}"

print("=" * 60)
print("  DETAILED REPORT CHECK")
print("=" * 60)

# 1. Check report pages via PBI REST API
print("\n1. Report pages (PBI API):")
resp = requests.get(f"{PBI}/reports/{REPORT_ID}/pages", headers=headers)
if resp.status_code == 200:
    pages = resp.json().get("value", [])
    print(f"   Found {len(pages)} pages:")
    for p in pages:
        print(f"   - {p.get('displayName', '?')} (name: {p.get('name', '?')}, order: {p.get('order', '?')})")
else:
    print(f"   HTTP {resp.status_code}: {resp.text[:300]}")

# 2. Get report definition from Fabric API (with proper LRO handling)
print("\n2. Fetching deployed report definition...")
resp = requests.post(f"{FABRIC}/reports/{REPORT_ID}/getDefinition", headers=headers)
print(f"   Initial response: HTTP {resp.status_code}")

parts = []
if resp.status_code == 200:
    parts = resp.json().get("definition", {}).get("parts", [])
elif resp.status_code == 202:
    loc = resp.headers.get("Location", "")
    retry = int(resp.headers.get("Retry-After", 3))
    print(f"   LRO started, polling...")
    for i in range(30):
        time.sleep(retry)
        r2 = requests.get(loc, headers=headers)
        if r2.status_code == 200:
            body = r2.json()
            status = body.get("status", "")
            if status == "Succeeded":
                # Try resourceLocation first
                res_loc = body.get("resourceLocation", "")
                if res_loc:
                    r3 = requests.get(res_loc, headers=headers)
                    if r3.status_code == 200:
                        parts = r3.json().get("definition", {}).get("parts", [])
                        break
                # Try /result
                r3 = requests.get(f"{loc}/result", headers=headers)
                if r3.status_code == 200:
                    parts = r3.json().get("definition", {}).get("parts", [])
                    break
                parts = body.get("definition", {}).get("parts", [])
                break
            elif status in ("Failed", "Cancelled"):
                print(f"   LRO {status}: {body}")
                break
            else:
                if i % 3 == 0:
                    print(f"   ... {status}")
        elif r2.status_code != 200:
            print(f"   Poll HTTP {r2.status_code}")
else:
    print(f"   Error: {resp.text[:300]}")

if parts:
    print(f"\n   Deployed definition has {len(parts)} parts:")
    page_parts = []
    visual_parts = []
    other_parts = []
    pbir_content = None
    
    for p in sorted(parts, key=lambda x: x.get("path", "")):
        path = p.get("path", "")
        if path == "definition.pbir":
            raw = base64.b64decode(p["payload"]).decode("utf-8")
            pbir_content = raw
            other_parts.append(path)
        elif "/page.json" in path:
            page_parts.append(path)
        elif "/visual.json" in path:
            visual_parts.append(path)
        else:
            other_parts.append(path)
    
    print(f"\n   Root/metadata files:")
    for p in other_parts:
        print(f"     {p}")
    print(f"\n   Pages ({len(page_parts)}):")
    for p in page_parts:
        print(f"     {p}")
    print(f"\n   Visuals ({len(visual_parts)}):")
    for p in visual_parts[:10]:
        print(f"     {p}")
    if len(visual_parts) > 10:
        print(f"     ... and {len(visual_parts) - 10} more")
    
    if pbir_content:
        print(f"\n   definition.pbir:")
        print(f"   {pbir_content}")
    
    # Decode a sample visual to check
    for p in parts:
        if "/v_p1c1/visual.json" in p.get("path", ""):
            raw = base64.b64decode(p["payload"]).decode("utf-8")
            vis = json.loads(raw)
            print(f"\n   Sample visual (v_p1c1 - Total Patients card):")
            q = vis.get("visual", {}).get("query", {}).get("queryState", {})
            for bucket, val in q.items():
                for proj in val.get("projections", []):
                    field = proj.get("field", {})
                    for ft in ["Measure", "Column"]:
                        if ft in field:
                            entity = field[ft]["Expression"]["SourceRef"]["Entity"]
                            prop = field[ft]["Property"]
                            print(f"     {bucket}: {entity}.{prop} [{ft}]")
            break
else:
    print("   Could not retrieve deployed definition")

# 3. Try Export Report to check if visuals have data
print(f"\n3. Trying executeQueries for Total Patients:")
resp = requests.post(
    f"{PBI}/datasets/{SM_ID}/executeQueries",
    headers=headers,
    json={"queries": [{"query": "EVALUATE ROW(\"val\", [Total Patients])"}], 
           "serializerSettings": {"includeNulls": True}}
)
if resp.status_code == 200:
    result = resp.json()
    tables = result.get("results", [{}])[0].get("tables", [{}])
    if tables and tables[0].get("rows"):
        print(f"   Total Patients = {tables[0]['rows'][0].get('[val]')}")
    else:
        print(f"   No data returned!")
else:
    print(f"   HTTP {resp.status_code}: {resp.text[:300]}")

print("\n" + "=" * 60)
