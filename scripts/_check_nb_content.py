"""Check if deployed RTI notebooks have content by fetching their definitions."""
import subprocess, json, base64, requests, time

token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com'],
    capture_output=True, text=True
).stdout)['accessToken']

H = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
API = 'https://api.fabric.microsoft.com/v1'

# Check Healthcare-demo-final2 (the main deployment target)
WS_ID = '1cee1797-ef2f-4911-b4e5-577eb64bd452'

notebooks = {
    'NB_RTI_Event_Simulator': 'b63f90df-8395-4a2d-a268-b93f777bbd2f',
    'NB_RTI_Fraud_Detection': '27ab8c90-b236-4c8f-a64f-dcfb7df3539b',
    'NB_RTI_Care_Gap_Alerts': '2d2bb987-13a0-4c53-9659-ab3fcfdd77ef',
    'NB_RTI_HighCost_Trajectory': 'f283683a-c4c6-432a-9fde-ec9e44e837b6',
    'NB_RTI_Setup_Eventhouse': '462fca36-3d7b-4742-b300-49e0b8c2de85',
}

for name, nb_id in notebooks.items():
    url = f"{API}/workspaces/{WS_ID}/items/{nb_id}/getDefinition"
    resp = requests.post(url, headers=H)
    
    if resp.status_code == 200:
        parts = resp.json().get('definition', {}).get('parts', [])
        for p in parts:
            if p['path'].endswith('.py') or p['path'].endswith('.ipynb'):
                decoded = base64.b64decode(p['payload']).decode('utf-8')
                print(f"{name}: {p['path']} = {len(decoded)} chars, {decoded.count(chr(10))} lines")
                if len(decoded) < 50:
                    print(f"  CONTENT: {decoded!r}")
    elif resp.status_code == 202:
        # LRO
        op_url = resp.headers.get('Location')
        retry_after = int(resp.headers.get('Retry-After', 3))
        for i in range(20):
            time.sleep(retry_after)
            op_resp = requests.get(op_url, headers=H)
            if op_resp.status_code == 200:
                result = op_resp.json()
                if result.get('status') == 'Succeeded':
                    parts = result.get('definition', {}).get('parts', [])
                    for p in parts:
                        if p['path'].endswith('.py') or p['path'].endswith('.ipynb'):
                            decoded = base64.b64decode(p['payload']).decode('utf-8')
                            print(f"{name}: {p['path']} = {len(decoded)} chars, {decoded.count(chr(10))} lines")
                            if len(decoded) < 50:
                                print(f"  CONTENT: {decoded!r}")
                    break
                elif result.get('status') in ('Failed', 'Cancelled'):
                    print(f"{name}: FAILED to get definition")
                    break
        else:
            print(f"{name}: TIMEOUT getting definition")
    else:
        print(f"{name}: HTTP {resp.status_code} - {resp.text[:200]}")
