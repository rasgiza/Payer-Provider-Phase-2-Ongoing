"""
Insert RTI Dashboard deploy code into Healthcare_Launcher.ipynb Cell 12.
Inserts just before the 'RTI deployment complete.' print statement.
"""
import json

NB_PATH = 'Healthcare_Launcher.ipynb'

# The RTI Dashboard deploy code to insert (as Python source lines)
DEPLOY_CODE = '''
    # ── Deploy RTI Dashboard ───────────────────────────────────────
    # KQL Real-Time Dashboards don't sync via Git — deploy via REST API.
    print()
    print("=" * 60)
    print("  DEPLOYING RTI DASHBOARD")
    print("=" * 60)

    _dash_deployed = False
    try:
        import urllib.request, zipfile, io as _io

        # Load dashboard template from repo (downloaded in Cell 3)
        _dash_json_path = None
        for _candidate in [
            os.path.join(workspace_dir, "..", "rti_dashboard", "healthcare_rti_dashboard.json"),
            os.path.join("/tmp/healthcare-demo", "rti_dashboard", "healthcare_rti_dashboard.json"),
        ]:
            if os.path.exists(_candidate):
                _dash_json_path = _candidate
                break

        # Also try fetching from repo URL if not found locally
        if not _dash_json_path:
            _repo_url = "https://raw.githubusercontent.com/rasgiza/Fabric-Payer-Provider-HealthCare-Demo/main/rti_dashboard/healthcare_rti_dashboard.json"
            try:
                _dash_resp = requests.get(_repo_url, timeout=30)
                if _dash_resp.status_code == 200:
                    _dash_json_path = "/tmp/_rti_dashboard.json"
                    with open(_dash_json_path, "w", encoding="utf-8") as _f:
                        _f.write(_dash_resp.text)
                    print("  Downloaded dashboard template from GitHub")
            except Exception:
                pass

        if not _dash_json_path:
            print("  [SKIP] Dashboard template not found in extracted repo")
            print("         Create manually in the Fabric portal using RTI_DASHBOARD_GUIDE.md")
        else:
            with open(_dash_json_path, "r", encoding="utf-8") as _f:
                _dash_raw = _f.read()

            # Resolve placeholders using discovered values from this cell
            _kql_db_id_val = kql_db_id if kql_db_id else ""
            _kql_db_name_val = "Healthcare_RTI_DB"

            # Get Eventhouse query URI
            _query_uri_val = ""
            if _kql_db_id_val:
                _kql_detail = requests.get(
                    f"{api_base}/kqlDatabases/{_kql_db_id_val}",
                    headers=headers
                )
                if _kql_detail.status_code == 200:
                    _kql_props = _kql_detail.json().get("properties", {})
                    _query_uri_val = (_kql_props.get("queryUri")
                                      or _kql_props.get("parentEventhouseUri")
                                      or "")

            if not _query_uri_val:
                # Try from Eventhouse item
                _eh_resp = requests.get(f"{api_base}/items?type=Eventhouse", headers=headers)
                if _eh_resp.status_code == 200:
                    for _eh in _eh_resp.json().get("value", []):
                        if "Healthcare" in _eh["displayName"] or "RTI" in _eh["displayName"]:
                            _eh_detail = requests.get(
                                f"{api_base}/eventhouses/{_eh['id']}",
                                headers=headers
                            )
                            if _eh_detail.status_code == 200:
                                _query_uri_val = _eh_detail.json().get("properties", {}).get("queryServiceUri", "")
                            break

            print(f"  KQL DB ID:   {_kql_db_id_val[:8]}...")
            print(f"  KQL DB Name: {_kql_db_name_val}")
            print(f"  Query URI:   {_query_uri_val[:50]}..." if _query_uri_val else "  Query URI:   (not found)")

            # Patch placeholders
            _dash_raw = _dash_raw.replace("__KQL_DB_ID__", _kql_db_id_val)
            _dash_raw = _dash_raw.replace("__KQL_DB_NAME__", _kql_db_name_val)
            if _query_uri_val:
                _dash_raw = _dash_raw.replace("__EVENTHOUSE_QUERY_URI__", _query_uri_val)

            # Validate JSON
            _dash_def = json.loads(_dash_raw)
            _pages = _dash_def.get("pages", [])
            print(f"  Dashboard: {len(_pages)} pages, auto-refresh 30s")

            # Check if dashboard already exists
            _existing_dash_id = None
            for _dtype in ["RealTimeDashboard", "KQLDashboard"]:
                _dr = requests.get(f"{api_base}/items?type={_dtype}", headers=headers)
                if _dr.status_code == 200:
                    for _d in _dr.json().get("value", []):
                        if _d["displayName"] == "Healthcare RTI Dashboard":
                            _existing_dash_id = _d["id"]
                            break
                if _existing_dash_id:
                    break

            # Encode definition
            _dash_b64 = base64.b64encode(_dash_raw.encode("utf-8")).decode("utf-8")
            _def_parts = [{"path": "RealTimeDashboard.json", "payload": _dash_b64, "payloadType": "InlineBase64"}]

            if _existing_dash_id:
                # Update existing dashboard
                print(f"  Updating existing dashboard ({_existing_dash_id[:8]}...)...")
                _ur = requests.post(
                    f"{api_base}/items/{_existing_dash_id}/updateDefinition",
                    headers=headers,
                    json={"definition": {"parts": _def_parts}}
                )
                if _ur.status_code in (200, 202):
                    if _ur.status_code == 202:
                        _uloc = _ur.headers.get("Location", "")
                        _retry_after = int(_ur.headers.get("Retry-After", 5))
                        for _ in range(30):
                            time.sleep(_retry_after)
                            _pr = requests.get(_uloc, headers=headers)
                            if _pr.status_code == 200 and _pr.json().get("status") == "Succeeded":
                                break
                    print("  [OK] RTI Dashboard updated")
                    _dash_deployed = True
                else:
                    print(f"  [WARN] Update failed: HTTP {_ur.status_code} {_ur.text[:200]}")
            else:
                # Create new dashboard
                print("  Creating Healthcare RTI Dashboard...")
                _create_body = {
                    "displayName": "Healthcare RTI Dashboard",
                    "type": "KQLDashboard",
                    "definition": {"parts": _def_parts}
                }
                _cr = requests.post(f"{api_base}/items", headers=headers, json=_create_body)

                if _cr.status_code in (200, 201):
                    _new_id = _cr.json().get("id", "")
                    print(f"  [OK] Created: Healthcare RTI Dashboard ({_new_id[:8]}...)")
                    _dash_deployed = True
                elif _cr.status_code == 202:
                    _cloc = _cr.headers.get("Location", "")
                    _retry_after = int(_cr.headers.get("Retry-After", 5))
                    for _ in range(30):
                        time.sleep(_retry_after)
                        _pr = requests.get(_cloc, headers=headers)
                        if _pr.status_code == 200:
                            _pstatus = _pr.json().get("status", "")
                            if _pstatus == "Succeeded":
                                print("  [OK] Created: Healthcare RTI Dashboard (LRO)")
                                _dash_deployed = True
                                break
                            elif _pstatus in ("Failed", "Cancelled"):
                                _perr = _pr.json().get("error", {}).get("message", "")
                                print(f"  [FAIL] {_pstatus}: {_perr[:200]}")
                                break
                elif _cr.status_code == 409:
                    print("  [OK] Dashboard already exists (409 conflict)")
                    _dash_deployed = True
                else:
                    # Try alternate type name
                    _create_body["type"] = "RealTimeDashboard"
                    _cr2 = requests.post(f"{api_base}/items", headers=headers, json=_create_body)
                    if _cr2.status_code in (200, 201, 202):
                        print("  [OK] Created with type RealTimeDashboard")
                        _dash_deployed = True
                    else:
                        print(f"  [FAIL] Create failed: HTTP {_cr.status_code} {_cr.text[:200]}")

    except Exception as _dash_err:
        print(f"  [WARN] Dashboard deploy failed: {_dash_err}")
        print("  You can create it manually — see RTI_DASHBOARD_GUIDE.md")

    if not _dash_deployed:
        print("  Dashboard not deployed. Create manually via Fabric portal.")
        print("  Reference: RTI_DASHBOARD_GUIDE.md in the repo")

'''

def main():
    with open(NB_PATH, 'r', encoding='utf-8') as f:
        nb = json.load(f)

    cell = nb['cells'][14]  # index 14 = CELL 12
    src = cell['source']

    # Find the insertion point: just before "    print()\n" + "    print("RTI deployment complete.")\n"
    insert_idx = None
    for i, line in enumerate(src):
        if 'print("RTI deployment complete.")' in line:
            # Go back to the blank line before it
            insert_idx = i - 1  # the blank line before the print
            break

    if insert_idx is None:
        print("ERROR: Could not find insertion point")
        return

    print(f"Inserting at source line {insert_idx}")

    # Convert deploy code to source lines (as list of strings with \n)
    deploy_lines = [line + '\n' for line in DEPLOY_CODE.split('\n')]
    # Remove trailing empty line
    if deploy_lines and deploy_lines[-1].strip() == '':
        deploy_lines = deploy_lines[:-1]

    # Insert
    new_src = src[:insert_idx] + deploy_lines + src[insert_idx:]
    cell['source'] = new_src

    print(f"Cell 12 source: {len(src)} -> {len(new_src)} lines")

    with open(NB_PATH, 'w', encoding='utf-8') as f:
        json.dump(nb, f, indent=1, ensure_ascii=False)
        f.write('\n')

    print("Done — Healthcare_Launcher.ipynb updated")


if __name__ == '__main__':
    main()
