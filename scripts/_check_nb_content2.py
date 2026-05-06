"""Check deployed RTI notebooks - with detailed error output."""
import subprocess, json, base64, requests, time

token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com'],
    capture_output=True, text=True
).stdout)['accessToken']

H = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
API = 'https://api.fabric.microsoft.com/v1'

WS_ID = '1cee1797-ef2f-4911-b4e5-577eb64bd452'

notebooks = {
    'NB_RTI_Fraud_Detection': '27ab8c90-b236-4c8f-a64f-dcfb7df3539b',
}

for name, nb_id in notebooks.items():
    url = f"{API}/workspaces/{WS_ID}/items/{nb_id}/getDefinition"
    resp = requests.post(url, headers=H)
    print(f"{name}: HTTP {resp.status_code}")
    
    if resp.status_code == 202:
        op_url = resp.headers.get('Location')
        retry_after = int(resp.headers.get('Retry-After', 3))
        print(f"  LRO: {op_url}")
        for i in range(30):
            time.sleep(retry_after)
            op_resp = requests.get(op_url, headers=H)
            result = op_resp.json()
            st = result.get('status', op_resp.status_code)
            print(f"  Poll {i+1}: status={st}")
            if st == 'Succeeded':
                parts = result.get('definition', {}).get('parts', [])
                if not parts:
                    # Check nested
                    parts = result.get('response', {}).get('definition', {}).get('parts', [])
                print(f"  Parts: {len(parts)}")
                for p in parts:
                    payload = p.get('payload', '')
                    decoded = base64.b64decode(payload).decode('utf-8') if payload else ''
                    print(f"    {p['path']}: {len(decoded)} chars")
                    # Show first 200 chars
                    print(f"    Preview: {decoded[:200]}")
                break
            elif st in ('Failed', 'Cancelled'):
                print(f"  ERROR: {json.dumps(result, indent=2)[:500]}")
                break
    elif resp.status_code == 200:
        parts = resp.json().get('definition', {}).get('parts', [])
        print(f"  Parts: {len(parts)}")
        for p in parts:
            payload = p.get('payload', '')
            decoded = base64.b64decode(payload).decode('utf-8') if payload else ''
            print(f"    {p['path']}: {len(decoded)} chars")
            print(f"    Preview: {decoded[:200]}")
    else:
        print(f"  {resp.text[:500]}")
