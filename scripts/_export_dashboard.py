"""Export NewRTDashboard_2 definition from Fabric and save raw + sanitized versions."""
import subprocess, json, base64, requests, time, os

token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com'],
    capture_output=True, text=True
).stdout)['accessToken']

H = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
API = 'https://api.fabric.microsoft.com/v1'

WS_ID = 'e0f2c894-7f63-4301-b2e1-2b8c6ae8c40c'
DASHBOARD_ID = '75ba0756-2b29-4661-ad15-585d1dfb7018'

OUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'rti_dashboard')
os.makedirs(OUT_DIR, exist_ok=True)

# ── Step 1: Get definition ──────────────────────────────────
print("1. Requesting dashboard definition...")
url = f"{API}/workspaces/{WS_ID}/items/{DASHBOARD_ID}/getDefinition"
resp = requests.post(url, headers=H)

if resp.status_code == 202:
    # Long-running operation
    op_url = resp.headers.get('Location')
    retry = int(resp.headers.get('Retry-After', 5))
    print(f"   LRO started, polling every {retry}s...")
    for i in range(60):
        time.sleep(retry)
        op_resp = requests.get(op_url, headers=H)
        if op_resp.status_code == 200:
            result = op_resp.json()
            if result.get('status') == 'Succeeded':
                resp = op_resp  # use the LRO result
                break
            elif result.get('status') in ('Failed', 'Cancelled'):
                print(f"   FAILED: {result}")
                exit(1)
        print(f"   Poll {i+1}: {op_resp.status_code}")
    else:
        print("   Timeout waiting for definition export")
        exit(1)
elif resp.status_code != 200:
    print(f"   ERROR: HTTP {resp.status_code}")
    print(f"   {resp.text[:1000]}")
    exit(1)

data = resp.json()
print(f"   Got response keys: {list(data.keys())}")

# ── Step 2: Decode parts ────────────────────────────────────
parts = data.get('definition', {}).get('parts', [])
if not parts:
    # Check if it's nested under response
    parts = data.get('response', {}).get('definition', {}).get('parts', [])

print(f"2. Found {len(parts)} definition parts:")
for p in parts:
    print(f"   - {p['path']} ({len(p.get('payload', ''))} chars base64)")

# Decode and save each part
for p in parts:
    path = p['path']
    payload_b64 = p.get('payload', '')
    if not payload_b64:
        print(f"   SKIP {path}: empty payload")
        continue
    
    decoded = base64.b64decode(payload_b64).decode('utf-8')
    
    # Try to pretty-print if JSON
    try:
        obj = json.loads(decoded)
        decoded_pretty = json.dumps(obj, indent=2, ensure_ascii=False)
    except json.JSONDecodeError:
        decoded_pretty = decoded
    
    # Save raw version
    raw_file = os.path.join(OUT_DIR, f'_raw_{path.replace("/", "_")}')
    with open(raw_file, 'w', encoding='utf-8') as f:
        f.write(decoded_pretty)
    print(f"   Saved raw: {raw_file}")
    
    # Print first 500 chars for inspection
    print(f"   Preview ({path}):")
    print(f"   {decoded_pretty[:500]}")
    print()

# ── Step 3: Also get Eventhouse/KQL DB info for reference ───
print("3. Collecting Eventhouse/KQL DB info for placeholder mapping...")
# KQL Databases in this workspace
kql_r = requests.get(f"{API}/workspaces/{WS_ID}/items?type=KQLDatabase", headers=H)
if kql_r.status_code == 200:
    for item in kql_r.json().get('value', []):
        print(f"   KQL DB: '{item['displayName']}' id={item['id']}")
        # Get properties
        detail = requests.get(f"{API}/workspaces/{WS_ID}/kqlDatabases/{item['id']}", headers=H)
        if detail.status_code == 200:
            props = detail.json().get('properties', {})
            print(f"     queryUri: {props.get('queryUri', 'N/A')}")
            print(f"     ingestionUri: {props.get('ingestionUri', 'N/A')}")
            print(f"     parentEventhouseItemId: {props.get('parentEventhouseItemId', 'N/A')}")

# Eventhouses
eh_r = requests.get(f"{API}/workspaces/{WS_ID}/items?type=Eventhouse", headers=H)
if eh_r.status_code == 200:
    for item in eh_r.json().get('value', []):
        print(f"   Eventhouse: '{item['displayName']}' id={item['id']}")
        detail = requests.get(f"{API}/workspaces/{WS_ID}/eventhouses/{item['id']}", headers=H)
        if detail.status_code == 200:
            props = detail.json().get('properties', {})
            print(f"     queryServiceUri: {props.get('queryServiceUri', 'N/A')}")

print("\nDone! Check rti_dashboard/ for exported files.")
