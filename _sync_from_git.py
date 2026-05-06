"""Sync Fabric workspace from git remote."""
import requests, time
from azure.identity import InteractiveBrowserCredential

cred = InteractiveBrowserCredential()
token = cred.get_token("https://api.fabric.microsoft.com/.default").token
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

WS_ID = "49ec0312-014d-4c2c-9857-07b8737a65b9"
api = f"https://api.fabric.microsoft.com/v1/workspaces/{WS_ID}/git"

import subprocess
commit_hash = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
print(f"Latest commit: {commit_hash}")

print("Syncing workspace from git...")
r = requests.post(f"{api}/updateFromGit", headers=headers, json={
    "remoteCommitHash": commit_hash,
    "conflictResolution": {
        "conflictResolutionType": "Workspace",
        "conflictResolutionPolicy": "PreferRemote"
    },
    "options": {"allowOverrideItems": True}
})
print(f"  HTTP {r.status_code}")

if r.status_code == 200:
    print("  [OK] Sync complete!")
elif r.status_code == 202:
    loc = r.headers.get("Location", "")
    retry = int(r.headers.get("Retry-After", "3"))
    print(f"  LRO started, polling...")
    for i in range(40):
        time.sleep(retry)
        poll = requests.get(loc, headers=headers)
        if poll.status_code == 200:
            body = poll.json()
            status = body.get("status", "")
            print(f"    [{i+1}] {status}")
            if status == "Succeeded":
                print("  [OK] Sync complete!")
                break
            elif status in ("Failed", "Cancelled"):
                print(f"  [FAIL] {body}")
                break
        else:
            print(f"    [{i+1}] Poll HTTP {poll.status_code}")
    else:
        print("  [TIMEOUT] Still running after 2 minutes")
else:
    print(f"  Error: {r.text[:500]}")
