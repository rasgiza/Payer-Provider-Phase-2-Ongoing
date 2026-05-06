"""
Try to export KQL Dashboard via alternate methods:
1. Try NewRTDashboard_1 in same workspace (maybe it works)
2. Try with retries (transient 500s)
3. Try the 2025-05 API version
"""
import subprocess, json, base64, requests, time, os

token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com'],
    capture_output=True, text=True
).stdout)['accessToken']

API = 'https://api.fabric.microsoft.com/v1'
WS_ID = 'e0f2c894-7f63-4301-b2e1-2b8c6ae8c40c'

OUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'rti_dashboard')
os.makedirs(OUT_DIR, exist_ok=True)

dashboards = {
    'NewRTDashboard_2': '75ba0756-2b29-4661-ad15-585d1dfb7018',
    'NewRTDashboard_1': '8b0c69f1-4b1c-4666-9c5e-46ec9173687a',
}

def try_export(dashboard_name, dashboard_id, api_version=None):
    """Attempt getDefinition with retries."""
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    if api_version:
        headers['x-ms-version'] = api_version
    
    url = f"{API}/workspaces/{WS_ID}/items/{dashboard_id}/getDefinition"
    print(f"\n--- Trying: {dashboard_name} (id={dashboard_id}) api_version={api_version} ---")
    
    for attempt in range(3):
        resp = requests.post(url, headers=headers)
        print(f"  Attempt {attempt+1}: HTTP {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            parts = data.get('definition', {}).get('parts', [])
            print(f"  SUCCESS! {len(parts)} parts")
            return data
        elif resp.status_code == 202:
            # Long-running operation
            op_url = resp.headers.get('Location')
            retry_after = int(resp.headers.get('Retry-After', 5))
            print(f"  LRO started, polling...")
            for i in range(40):
                time.sleep(retry_after)
                op_resp = requests.get(op_url, headers=headers)
                if op_resp.status_code == 200:
                    result = op_resp.json()
                    st = result.get('status', '')
                    if st == 'Succeeded':
                        print(f"  SUCCESS via LRO!")
                        return result
                    elif st in ('Failed', 'Cancelled'):
                        err = result.get('error', {})
                        print(f"  LRO {st}: {err}")
                        break
                    else:
                        if i % 5 == 0:
                            print(f"    Poll {i+1}: {st}")
                elif op_resp.status_code == 202:
                    if i % 5 == 0:
                        print(f"    Poll {i+1}: still 202")
                else:
                    print(f"    Poll {i+1}: HTTP {op_resp.status_code}")
                    break
            break  # Don't retry after LRO
        elif resp.status_code == 500:
            err = resp.text[:200]
            print(f"  500: {err}")
            if attempt < 2:
                time.sleep(3)
        else:
            print(f"  {resp.status_code}: {resp.text[:200]}")
            break
    return None

# Try each dashboard with different API approaches
for name, did in dashboards.items():
    result = try_export(name, did)
    if result:
        # Save it!
        parts = result.get('definition', {}).get('parts', [])
        if not parts:
            parts = result.get('response', {}).get('definition', {}).get('parts', [])
        for p in parts:
            decoded = base64.b64decode(p['payload']).decode('utf-8')
            try:
                obj = json.loads(decoded)
                decoded = json.dumps(obj, indent=2, ensure_ascii=False)
            except:
                pass
            fname = f"_exported_{name}_{p['path'].replace('/', '_')}"
            fpath = os.path.join(OUT_DIR, fname)
            with open(fpath, 'w', encoding='utf-8') as f:
                f.write(decoded)
            print(f"  Saved: {fpath}")
            print(f"  Preview: {decoded[:800]}")
        break  # Got one, stop

# Also try: check if workspace has Git integration that might expose the definition
print("\n\n--- Checking Git integration on workspace ---")
resp = requests.get(
    f"{API}/workspaces/{WS_ID}/git/status",
    headers={'Authorization': f'Bearer {token}'}
)
print(f"  Git status: HTTP {resp.status_code}")
if resp.status_code == 200:
    print(f"  {json.dumps(resp.json(), indent=2)[:1000]}")
