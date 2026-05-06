"""Check RTI notebooks deployed in the target workspace."""
import subprocess, json, requests

token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com'],
    capture_output=True, text=True
).stdout)['accessToken']

H = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
API = 'https://api.fabric.microsoft.com/v1'

# Check both workspaces
workspaces = {
    'healthcare-project-demo': 'e0f2c894-7f63-4301-b2e1-2b8c6ae8c40c',
    'Healthcare-demo-final2': '1cee1797-ef2f-4911-b4e5-577eb64bd452',
}

for ws_name, ws_id in workspaces.items():
    print(f"\n=== {ws_name} ===")
    r = requests.get(f"{API}/workspaces/{ws_id}/items?type=Notebook", headers=H)
    if r.status_code != 200:
        print(f"  HTTP {r.status_code}")
        continue
    for item in sorted(r.json().get('value', []), key=lambda x: x['displayName']):
        if 'RTI' in item['displayName'] or 'rti' in item['displayName'].lower():
            print(f"  {item['displayName']}  id={item['id']}")
