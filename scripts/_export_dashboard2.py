"""Try multiple approaches to export KQL Dashboard definition."""
import subprocess, json, base64, requests, time

token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com'],
    capture_output=True, text=True
).stdout)['accessToken']

H = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
API = 'https://api.fabric.microsoft.com/v1'

WS_ID = 'e0f2c894-7f63-4301-b2e1-2b8c6ae8c40c'
DASHBOARD_ID = '75ba0756-2b29-4661-ad15-585d1dfb7018'

# ── Approach 1: POST getDefinition with format ──────────────
print("=== Approach 1: POST getDefinition with format ===")
for fmt in [None, "KQLDashboard"]:
    body = {"format": fmt} if fmt else None
    resp = requests.post(
        f"{API}/workspaces/{WS_ID}/items/{DASHBOARD_ID}/getDefinition",
        headers=H,
        json=body
    )
    print(f"  format={fmt}: HTTP {resp.status_code}")
    if resp.status_code in (200, 201):
        print(f"  SUCCESS: {json.dumps(resp.json(), indent=2)[:1000]}")
        break
    elif resp.status_code == 202:
        # LRO
        op_url = resp.headers.get('Location')
        retry_after = int(resp.headers.get('Retry-After', 5))
        print(f"  LRO started at {op_url}")
        for i in range(30):
            time.sleep(retry_after)
            op_resp = requests.get(op_url, headers=H)
            status_code = op_resp.status_code
            if status_code == 200:
                result = op_resp.json()
                st = result.get('status', '')
                print(f"  Poll {i+1}: status={st}")
                if st == 'Succeeded':
                    # The definition is in the response body
                    print(f"  RESULT: {json.dumps(result, indent=2)[:2000]}")
                    # Save it
                    parts = result.get('definition', {}).get('parts', [])
                    if not parts:
                        parts = result.get('response', {}).get('definition', {}).get('parts', [])
                    for p in parts:
                        decoded = base64.b64decode(p['payload']).decode('utf-8')
                        print(f"  Part '{p['path']}': {decoded[:500]}")
                    break
                elif st in ('Failed', 'Cancelled'):
                    print(f"  FAILED: {json.dumps(result, indent=2)[:500]}")
                    break
            elif status_code == 202:
                print(f"  Poll {i+1}: still running...")
            else:
                print(f"  Poll {i+1}: HTTP {status_code} {op_resp.text[:200]}")
        break
    else:
        print(f"  {resp.text[:300]}")

# ── Approach 2: GET item details ────────────────────────────
print("\n=== Approach 2: GET item details ===")
resp = requests.get(f"{API}/workspaces/{WS_ID}/items/{DASHBOARD_ID}", headers=H)
print(f"  HTTP {resp.status_code}")
if resp.status_code == 200:
    print(f"  {json.dumps(resp.json(), indent=2)}")

# ── Approach 3: Try KQLDashboard-specific endpoint ──────────
print("\n=== Approach 3: KQLDashboard-specific endpoints ===")
for endpoint in ['kqlDashboards', 'dashboards', 'realTimeDashboards']:
    url = f"{API}/workspaces/{WS_ID}/{endpoint}/{DASHBOARD_ID}"
    resp = requests.get(url, headers=H)
    print(f"  GET /{endpoint}/{{id}}: HTTP {resp.status_code}")
    if resp.status_code == 200:
        print(f"  {json.dumps(resp.json(), indent=2)[:500]}")

# ── Approach 4: Try getDefinition with type-specific path ───
print("\n=== Approach 4: Type-specific getDefinition ===")
for endpoint in ['kqlDashboards', 'dashboards']:
    url = f"{API}/workspaces/{WS_ID}/{endpoint}/{DASHBOARD_ID}/getDefinition"
    resp = requests.post(url, headers=H)
    print(f"  POST /{endpoint}/{{id}}/getDefinition: HTTP {resp.status_code}")
    if resp.status_code in (200, 202):
        if resp.status_code == 202:
            op_url = resp.headers.get('Location')
            retry_after = int(resp.headers.get('Retry-After', 3))
            print(f"  LRO: {op_url}")
            for i in range(30):
                time.sleep(retry_after)
                op_resp = requests.get(op_url, headers=H)
                if op_resp.status_code == 200:
                    result = op_resp.json()
                    st = result.get('status', '')
                    print(f"  Poll {i+1}: status={st}")
                    if st in ('Succeeded', 'Failed', 'Cancelled'):
                        print(f"  {json.dumps(result, indent=2)[:2000]}")
                        break
                else:
                    print(f"  Poll {i+1}: HTTP {op_resp.status_code}")
            break
        else:
            print(f"  {json.dumps(resp.json(), indent=2)[:1000]}")
    else:
        print(f"  {resp.text[:200]}")
