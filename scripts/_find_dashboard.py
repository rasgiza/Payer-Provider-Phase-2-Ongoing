"""Find NewRTDashboard_2 across all workspaces."""
import subprocess, json, requests

token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com'],
    capture_output=True, text=True
).stdout)['accessToken']

H = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
API = 'https://api.fabric.microsoft.com/v1'

# List workspaces
r = requests.get(f'{API}/workspaces', headers=H)
workspaces = r.json().get('value', [])

print("=== Workspaces ===")
for ws in workspaces:
    if 'healthcare' in ws['displayName'].lower() or 'demo' in ws['displayName'].lower():
        print(f"  {ws['displayName']}  ->  {ws['id']}")

print("\n=== Searching for dashboard ===")
for ws in workspaces:
    items_r = requests.get(f"{API}/workspaces/{ws['id']}/items", headers=H)
    if items_r.status_code != 200:
        continue
    for item in items_r.json().get('value', []):
        name_lower = item['displayName'].lower()
        if 'newrt' in name_lower or 'rtdashboard' in name_lower:
            print(f"  FOUND: '{item['displayName']}'")
            print(f"    type = {item['type']}")
            print(f"    id   = {item['id']}")
            print(f"    ws   = {ws['displayName']} ({ws['id']})")
