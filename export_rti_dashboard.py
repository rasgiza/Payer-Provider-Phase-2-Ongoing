"""
Export RTI Dashboard from Fabric Workspace to Local JSON
========================================================
Downloads the Real-Time Dashboard definition from the Fabric workspace,
tokenizes workspace-specific values (Eventhouse URI, KQL DB ID, etc.),
and saves it to rti_dashboard/healthcare_rti_dashboard.json.

After running this script, commit the JSON to the repo.
The Healthcare_Launcher will deploy it to new workspaces automatically.

Usage:
    python export_rti_dashboard.py
    python export_rti_dashboard.py --dashboard-name "Healthcare_RTI_Dashboard"
"""

import sys
import os
import json
import base64
import argparse
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from fabric_auth import get_fabric_token, get_auth_headers
from config import TENANT_ID, ADMIN_ACCOUNT, WORKSPACE_NAME

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

FABRIC_API = "https://api.fabric.microsoft.com/v1"
RTI_DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "rti_dashboard")

# Known workspace-specific values to tokenize
# These will be replaced with placeholders so the JSON is portable
PLACEHOLDER_MAP = {
    "__EVENTHOUSE_QUERY_URI__": None,   # e.g. https://{guid}.z6.kusto.fabric.microsoft.com
    "__KQL_DB_NAME__": None,            # e.g. Healthcare_RTI_DB
    "__KQL_DB_ID__": None,              # GUID of the KQL database
}


def get_workspace_id(token, workspace_name):
    """Find workspace ID by name."""
    resp = requests.get(f"{FABRIC_API}/workspaces", headers=get_auth_headers(token))
    resp.raise_for_status()
    for ws in resp.json().get("value", []):
        if ws["displayName"] == workspace_name:
            return ws["id"]
    available = [w["displayName"] for w in resp.json().get("value", [])]
    print(f"[FAIL] Workspace '{workspace_name}' not found.")
    print(f"  Available: {available}")
    sys.exit(1)


def find_dashboard(token, workspace_id, dashboard_name=None):
    """Find the RTI Dashboard item in the workspace."""
    headers = get_auth_headers(token)
    # Try different item type names Fabric uses for RTI dashboards
    for item_type in ["RealTimeDashboard", "KQLDashboard", "Dashboard"]:
        url = f"{FABRIC_API}/workspaces/{workspace_id}/items?type={item_type}"
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            continue
        items = resp.json().get("value", [])
        if not items:
            continue

        # If a specific name given, match it
        if dashboard_name:
            for item in items:
                if item["displayName"] == dashboard_name:
                    return item["id"], item["displayName"], item_type
        else:
            # Return first RTI dashboard found (prefer ones with RTI/Healthcare in name)
            for item in items:
                if "RTI" in item["displayName"] or "Healthcare" in item["displayName"]:
                    return item["id"], item["displayName"], item_type
            # Fallback to first dashboard
            if items:
                return items[0]["id"], items[0]["displayName"], item_type

    # List all items to help user find it
    print("[FAIL] No Real-Time Dashboard found in workspace.")
    resp = requests.get(f"{FABRIC_API}/workspaces/{workspace_id}/items", headers=headers)
    if resp.status_code == 200:
        all_items = resp.json().get("value", [])
        dash_like = [i for i in all_items if "dashboard" in i.get("type", "").lower()
                     or "kql" in i.get("type", "").lower()
                     or "realtime" in i.get("type", "").lower()]
        if dash_like:
            print("  Found these potentially relevant items:")
            for i in dash_like:
                print(f"    - {i['displayName']} (type={i['type']}, id={i['id']})")
        else:
            print("  All items in workspace:")
            for i in all_items:
                print(f"    - {i['displayName']} (type={i['type']})")
    sys.exit(1)


def get_kql_database_info(token, workspace_id):
    """Get KQL database name, ID, and Eventhouse query URI."""
    headers = get_auth_headers(token)

    # Find KQL Database
    resp = requests.get(f"{FABRIC_API}/workspaces/{workspace_id}/items?type=KQLDatabase",
                        headers=headers)
    kql_db_id = None
    kql_db_name = None
    if resp.status_code == 200:
        for item in resp.json().get("value", []):
            if "Healthcare" in item["displayName"] or "RTI" in item["displayName"]:
                kql_db_id = item["id"]
                kql_db_name = item["displayName"]
                break
        if not kql_db_id:
            items = resp.json().get("value", [])
            if items:
                kql_db_id = items[0]["id"]
                kql_db_name = items[0]["displayName"]

    # Get query URI from KQL database properties
    query_uri = None
    if kql_db_id:
        resp = requests.get(f"{FABRIC_API}/workspaces/{workspace_id}/kqlDatabases/{kql_db_id}",
                            headers=headers)
        if resp.status_code == 200:
            props = resp.json().get("properties", {})
            query_uri = props.get("queryUri") or props.get("parentEventhouseUri")

    return kql_db_name, kql_db_id, query_uri


def export_dashboard_definition(token, workspace_id, dashboard_id):
    """Download dashboard definition via getDefinition API."""
    headers = get_auth_headers(token)
    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{dashboard_id}/getDefinition"

    resp = requests.post(url, headers=headers, json={})

    if resp.status_code == 200:
        return resp.json()
    elif resp.status_code == 202:
        # Long-running operation
        location = resp.headers.get("Location", "")
        retry_after = int(resp.headers.get("Retry-After", 5))
        print(f"  Waiting for definition export (LRO)...")
        import time
        for _ in range(60):
            time.sleep(retry_after)
            poll = requests.get(location, headers=headers)
            if poll.status_code == 200:
                body = poll.json()
                status = body.get("status", "")
                if status == "Succeeded":
                    # Try /result endpoint
                    result_resp = requests.get(f"{location}/result", headers=headers)
                    if result_resp.status_code == 200:
                        return result_resp.json()
                    # Try resourceLocation
                    res_loc = body.get("resourceLocation", "")
                    if res_loc:
                        res_resp = requests.get(res_loc, headers=headers)
                        if res_resp.status_code == 200:
                            return res_resp.json()
                    return body
                elif status in ("Failed", "Cancelled"):
                    err = body.get("error", {}).get("message", "")
                    print(f"  [FAIL] Export failed: {status} -- {err}")
                    sys.exit(1)
        print("  [FAIL] Export timed out")
        sys.exit(1)
    else:
        print(f"  [FAIL] getDefinition returned HTTP {resp.status_code}")
        print(f"  {resp.text[:500]}")
        sys.exit(1)


def tokenize_dashboard(raw_json, kql_db_name, kql_db_id, query_uri):
    """Replace workspace-specific values with portable placeholders."""
    replacements = 0
    if query_uri and query_uri in raw_json:
        raw_json = raw_json.replace(query_uri, "__EVENTHOUSE_QUERY_URI__")
        replacements += raw_json.count("__EVENTHOUSE_QUERY_URI__")
        print(f"  Tokenized query URI: {query_uri} -> __EVENTHOUSE_QUERY_URI__")

    if kql_db_id and kql_db_id in raw_json:
        raw_json = raw_json.replace(kql_db_id, "__KQL_DB_ID__")
        replacements += raw_json.count("__KQL_DB_ID__")
        print(f"  Tokenized DB ID: {kql_db_id} -> __KQL_DB_ID__")

    if kql_db_name and kql_db_name in raw_json:
        raw_json = raw_json.replace(kql_db_name, "__KQL_DB_NAME__")
        replacements += raw_json.count("__KQL_DB_NAME__")
        print(f"  Tokenized DB name: {kql_db_name} -> __KQL_DB_NAME__")

    if replacements == 0:
        print("  [WARN] No workspace-specific values found to tokenize.")
        print("         The exported JSON may contain hardcoded IDs.")

    return raw_json


def main():
    parser = argparse.ArgumentParser(description="Export RTI Dashboard from Fabric")
    parser.add_argument("--dashboard-name", default=None,
                        help="Exact display name of the dashboard (auto-detects if omitted)")
    parser.add_argument("--workspace", default=None,
                        help="Workspace name (defaults to WORKSPACE_NAME from .env)")
    args = parser.parse_args()

    workspace_name = args.workspace or WORKSPACE_NAME
    print("=" * 60)
    print("  EXPORT RTI DASHBOARD")
    print("=" * 60)
    print(f"  Workspace: {workspace_name}")
    print()

    # Authenticate
    token = get_fabric_token(TENANT_ID, ADMIN_ACCOUNT)

    # Find workspace
    workspace_id = get_workspace_id(token, workspace_name)
    print(f"  Workspace ID: {workspace_id}")

    # Find dashboard
    dashboard_id, dashboard_name, item_type = find_dashboard(
        token, workspace_id, args.dashboard_name
    )
    print(f"  Dashboard: {dashboard_name} (type={item_type}, id={dashboard_id})")

    # Get KQL DB info for tokenization
    kql_db_name, kql_db_id, query_uri = get_kql_database_info(token, workspace_id)
    print(f"  KQL DB: {kql_db_name} (id={kql_db_id})")
    print(f"  Query URI: {query_uri}")
    print()

    # Export definition
    print("Exporting dashboard definition...")
    result = export_dashboard_definition(token, workspace_id, dashboard_id)

    # Extract the dashboard JSON from the definition parts
    definition = result.get("definition", result)
    parts = definition.get("parts", [])

    if not parts:
        print("[FAIL] No definition parts returned.")
        print(f"  Response keys: {list(result.keys())}")
        sys.exit(1)

    print(f"  Got {len(parts)} definition part(s):")
    for p in parts:
        print(f"    - {p.get('path', '?')}")

    # Find the main dashboard content part
    dashboard_content = None
    dashboard_path = None
    for part in parts:
        path = part.get("path", "")
        if "dashboard" in path.lower() or path.endswith(".json"):
            payload = part.get("payload", "")
            try:
                raw = base64.b64decode(payload).decode("utf-8")
                # Validate it's JSON
                json.loads(raw)
                dashboard_content = raw
                dashboard_path = path
                break
            except (json.JSONDecodeError, Exception):
                continue

    # Fallback: try first non-.platform part
    if not dashboard_content:
        for part in parts:
            if part.get("path") == ".platform":
                continue
            payload = part.get("payload", "")
            try:
                raw = base64.b64decode(payload).decode("utf-8")
                json.loads(raw)
                dashboard_content = raw
                dashboard_path = part.get("path", "unknown")
                break
            except Exception:
                continue

    if not dashboard_content:
        print("[FAIL] Could not extract dashboard JSON from definition parts.")
        # Save raw response for debugging
        debug_path = os.path.join(RTI_DASHBOARD_DIR, "_raw_export_response.json")
        os.makedirs(RTI_DASHBOARD_DIR, exist_ok=True)
        with open(debug_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"  Raw response saved to: {debug_path}")
        sys.exit(1)

    print(f"  Extracted from: {dashboard_path}")
    print(f"  Size: {len(dashboard_content):,} bytes")

    # Tokenize workspace-specific values
    print("\nTokenizing workspace-specific values...")
    dashboard_content = tokenize_dashboard(
        dashboard_content, kql_db_name, kql_db_id, query_uri
    )

    # Pretty-print and save
    dashboard_json = json.loads(dashboard_content)
    os.makedirs(RTI_DASHBOARD_DIR, exist_ok=True)
    output_path = os.path.join(RTI_DASHBOARD_DIR, "healthcare_rti_dashboard.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(dashboard_json, f, indent=2, ensure_ascii=False)

    print(f"\n  [OK] Dashboard exported to:")
    print(f"       {output_path}")
    print(f"       ({len(json.dumps(dashboard_json)):,} bytes)")
    print()
    print("  Placeholders used:")
    print("    __EVENTHOUSE_QUERY_URI__  — Eventhouse cluster URI")
    print("    __KQL_DB_NAME__           — KQL database display name")
    print("    __KQL_DB_ID__             — KQL database GUID")
    print()
    print("  Next steps:")
    print("    1. git add rti_dashboard/healthcare_rti_dashboard.json")
    print("    2. git commit -m 'Export RTI Dashboard definition from workspace'")
    print("    3. git push")
    print()
    print("  The Healthcare_Launcher will deploy this dashboard automatically")
    print("  when DEPLOY_STREAMING=True.")


if __name__ == "__main__":
    main()
