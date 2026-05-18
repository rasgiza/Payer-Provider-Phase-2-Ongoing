# Fabric notebook source

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# # RTI: Payer Operational Alerts (CFO / Revenue Cycle Director / Billing)
#
# Generates real-time alerts for the **Payer Agent** persona group:
#   1. **DENIAL_SPIKE**       — payer denial rate exceeds 12% in last hour
#   2. **HIGH_DOLLAR_DENIAL** — single denial >$5K (or preventable >$2K)
#   3. **AR_AGING_BREACH**    — payer avg days-in-AR crosses 45-day threshold
#   4. **AUTH_EXPIRING**      — prior auth expires ≤7 days, procedure not done
#
# **Inputs** (from Eventstream → Eventhouse, set up by NB_RTI_Setup_Eventhouse):
#   - `denial_adjudication_events` (KQL)
#   - `ar_snapshot_events` (KQL)
#   - `auth_lifecycle_events` (KQL)
#
# **Outputs**:
#   - Delta: `lh_gold_curated.rti_payer_alerts`
#   - KQL: `payer_alerts` (consumed by Activator → Teams / Power Automate)
#
# **Default lakehouse:** `lh_gold_curated`

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

print("NB_RTI_Payer_Alerts: Starting...")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Attach default lakehouse (self-healing) ----------
import requests as _req
_ws_id = notebookutils.runtime.context.get("currentWorkspaceId", "")
_tok = notebookutils.credentials.getToken("pbi")
_hdr = {"Authorization": f"Bearer {_tok}"}
_lh_resp = _req.get(f"https://api.fabric.microsoft.com/v1/workspaces/{_ws_id}/lakehouses", headers=_hdr)
_lh_map = {}
if _lh_resp.status_code == 200:
    for _lh in _lh_resp.json().get("value", []):
        _lh_map[_lh["displayName"]] = f"abfss://{_ws_id}@onelake.dfs.fabric.microsoft.com/{_lh['id']}/Tables"

    _gold_id = next((_lh["id"] for _lh in _lh_resp.json().get("value", []) if _lh["displayName"] == "lh_gold_curated"), None)
    if _gold_id:
        try:
            notebookutils.lakehouse.setDefaultLakehouse(_ws_id, _gold_id)
            print(f"  Attached lh_gold_curated ({_gold_id[:8]}...)")
        except Exception:
            import re as _re_mod
            _lh_names = sorted(_lh_map.keys(), key=len, reverse=True)
            _lh_pattern = r'\b(' + '|'.join(_re_mod.escape(n) for n in _lh_names) + r')\.(\w+)\b'
            _orig_sql = spark.sql
            def _patched_sql(query, _pat=_lh_pattern, _m=_lh_map, _orig=_orig_sql):
                query = _re_mod.sub(_pat, lambda m: f'delta.`{_m[m.group(1)]}/{m.group(2)}`', query)
                return _orig(query)
            spark.sql = _patched_sql
            from pyspark.sql import DataFrameWriter as _DFW
            _orig_sat = _DFW.saveAsTable
            def _patched_sat(self, name, _m=_lh_map, _orig=_orig_sat, **kwargs):
                parts = name.split('.', 1)
                if len(parts) == 2 and parts[0] in _m:
                    self.save(f'{_m[parts[0]]}/{parts[1]}'); return
                return _orig(self, name, **kwargs)
            _DFW.saveAsTable = _patched_sat
            print(f"  Registered ABFSS rewriter for: {', '.join(_lh_map.keys())}")
del _req, _ws_id, _tok, _hdr, _lh_resp

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

%pip install azure-kusto-data azure-kusto-ingest azure-core>=1.31.0 --quiet

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

from pyspark.sql import functions as F
import time as _wait_time
import requests as _kql_req
import subprocess, sys
try:
    from azure.kusto.data import KustoConnectionStringBuilder, KustoClient
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "azure-kusto-data", "azure-kusto-ingest"])
    for _m in list(sys.modules.keys()):
        if _m.startswith("azure"): del sys.modules[_m]
    from azure.kusto.data import KustoConnectionStringBuilder, KustoClient

# Discover Eventhouse query URI
_ws_id = notebookutils.runtime.context.get("currentWorkspaceId", "")
_fabric_tok = notebookutils.credentials.getToken("https://analysis.windows.net/powerbi/api")
_hdr = {"Authorization": f"Bearer {_fabric_tok}", "Content-Type": "application/json"}
_KUSTO_QUERY_URI = None
_KUSTO_INGEST_URI = None
_resp = _kql_req.get(f"https://api.fabric.microsoft.com/v1/workspaces/{_ws_id}/items?type=Eventhouse", headers=_hdr)
if _resp.status_code == 200:
    for _item in _resp.json().get("value", []):
        if "Healthcare" in _item.get("displayName", ""):
            _props_resp = _kql_req.get(f"https://api.fabric.microsoft.com/v1/workspaces/{_ws_id}/eventhouses/{_item['id']}", headers=_hdr)
            if _props_resp.status_code == 200:
                _props = _props_resp.json().get("properties", _props_resp.json())
                _KUSTO_QUERY_URI = _props.get("queryServiceUri", "")
                _KUSTO_INGEST_URI = _props.get("ingestionServiceUri", "") or _KUSTO_QUERY_URI.replace("https://", "https://ingest-")
            break

if not _KUSTO_QUERY_URI:
    raise RuntimeError("Healthcare_RTI_Eventhouse not found. Run NB_RTI_Setup_Eventhouse first.")

_KQL_DB_NAME = "Healthcare_RTI_DB"

def _kql_query(query):
    _tok = notebookutils.credentials.getToken("kusto")
    _k = KustoConnectionStringBuilder.with_aad_user_token_authentication(_KUSTO_QUERY_URI, _tok)
    result = KustoClient(_k).execute(_KQL_DB_NAME, query)
    primary = result.primary_results[0] if result.primary_results else None
    if not primary: return [], []
    return [c.column_name for c in primary.columns], [list(r) for r in primary]

def _kql_mgmt(cmd):
    _tok = notebookutils.credentials.getToken("kusto")
    _k = KustoConnectionStringBuilder.with_aad_user_token_authentication(_KUSTO_QUERY_URI, _tok)
    return KustoClient(_k).execute_mgmt(_KQL_DB_NAME, cmd)

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Wait for source events + backfill typed tables ----------
print("Loading payer events from KQL Eventhouse...")

_backfill_map = {
    "denial_adjudication_events": "ExtractDenialAdjudicationEvents()",
    "ar_snapshot_events":         "ExtractArSnapshotEvents()",
    "auth_lifecycle_events":      "ExtractAuthLifecycleEvents()",
}

# Wait for landing table
_max_wait = 18  # 180s
for _i in range(1, _max_wait + 1):
    _, _r = _kql_query("rti_all_events | count")
    if _r and int(_r[0][0]) > 0:
        print(f"  rti_all_events: {int(_r[0][0])} rows (source ready)")
        break
    print(f"  [{_i}/{_max_wait}] waiting for rti_all_events...")
    _wait_time.sleep(10)

for _tbl, _fn in _backfill_map.items():
    _, _r = _kql_query(f"{_tbl} | count")
    _cnt = int(_r[0][0]) if _r and _r[0] else 0
    if _cnt == 0:
        print(f"  Backfilling {_tbl} from rti_all_events via {_fn}...")
        try:
            _kql_mgmt(f".set-or-append {_tbl} <| {_fn}")
            _, _r2 = _kql_query(f"{_tbl} | count")
            print(f"    → {_tbl}: {int(_r2[0][0])} rows after backfill")
        except Exception as _e:
            print(f"    [WARN] backfill failed: {_e}")
    else:
        print(f"  {_tbl}: {_cnt} rows (already populated)")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# PAYER ALERT 1: DENIAL_SPIKE (rolling 1-hour denial rate by payer)
# ============================================================================
# Trigger: payer denial rate >12% over last hour, with at least 20 claims
# Severity: ≥20% = CRITICAL, 15-20% = HIGH, 12-15% = MEDIUM
# Recipient: Revenue Cycle Director (James) + CFO if CRITICAL
# ============================================================================

_cols, _rows = _kql_query("""
    denial_adjudication_events
    | where event_timestamp > ago(1h)
    | summarize total = count(),
                denied = countif(adjudication_result == "DENIED"),
                denied_dollars = sumif(billed_amount, adjudication_result == "DENIED"),
                top_reason = arg_max(case(adjudication_result == "DENIED", denial_reason, ""), denial_reason)
        by payer_id, payer_name
    | extend denial_rate = todouble(denied) / todouble(total)
    | where total >= 20 and denial_rate > 0.12
""")
print(f"  Payers with denial spike: {len(_rows)}")

_denial_alerts = []
if _rows:
    import uuid as _uuid
    from datetime import datetime as _dt
    _now_iso = _dt.utcnow().isoformat()
    for r in _rows:
        rec = dict(zip(_cols, r))
        rate = float(rec.get("denial_rate", 0))
        if rate >= 0.20:
            sev = "CRITICAL"
            action = "CFO escalation. Pause submissions to this payer + emergency contract review."
        elif rate >= 0.15:
            sev = "HIGH"
            action = "Convene denial management team. Root-cause top denial reason within 24h."
        else:
            sev = "MEDIUM"
            action = "Add payer to weekly denial review. Monitor next-hour trend."
        _denial_alerts.append({
            "alert_id": str(_uuid.uuid4()),
            "alert_timestamp": _now_iso,
            "alert_type": "DENIAL_SPIKE",
            "payer_id": rec.get("payer_id", ""),
            "payer_name": rec.get("payer_name", ""),
            "claim_id": "",
            "patient_id": "",
            "severity": sev,
            "alert_text": (f"DENIAL SPIKE: {rec.get('payer_name','')} denied {int(rec.get('denied',0))}/"
                           f"{int(rec.get('total',0))} claims (={rate*100:.1f}%) in last hour. "
                           f"Top reason: {rec.get('top_reason','') or 'mixed'}. "
                           f"Denied dollars: ${float(rec.get('denied_dollars',0)):,.0f}."),
            "recommended_action": action,
            "dollar_impact": float(rec.get("denied_dollars", 0) or 0),
            "denial_reason": rec.get("top_reason", "") or "",
            "days_until_expiry": 0,
        })

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# PAYER ALERT 2: HIGH_DOLLAR_DENIAL (single high-value denied claim)
# ============================================================================
# Trigger: any DENIED claim where billed >$5K, OR preventable denial >$2K
# Severity: >$10K = CRITICAL, $5K-$10K = HIGH, preventable $2K-$5K = MEDIUM
# Recipient: Billing Specialist (Sarah) for action; CFO for CRITICAL
# ============================================================================

_cols, _rows = _kql_query("""
    denial_adjudication_events
    | where event_timestamp > ago(24h) and adjudication_result == "DENIED"
    | where billed_amount > 5000 or (is_preventable == true and billed_amount > 2000)
    | project event_id, event_timestamp, claim_id, patient_id, payer_id, payer_name,
              billed_amount, denial_reason, is_preventable, recommended_action
""")
print(f"  High-dollar / preventable denials: {len(_rows)}")

_hi_alerts = []
if _rows:
    import uuid as _uuid2
    from datetime import datetime as _dt2
    _now_iso2 = _dt2.utcnow().isoformat()
    for r in _rows:
        rec = dict(zip(_cols, r))
        billed = float(rec.get("billed_amount", 0) or 0)
        is_prev = bool(rec.get("is_preventable", False))
        if billed > 10000:
            sev = "CRITICAL"
        elif billed > 5000:
            sev = "HIGH"
        else:
            sev = "MEDIUM"
        prev_tag = " [PREVENTABLE]" if is_prev else ""
        _hi_alerts.append({
            "alert_id": str(_uuid2.uuid4()),
            "alert_timestamp": _now_iso2,
            "alert_type": "HIGH_DOLLAR_DENIAL",
            "payer_id": rec.get("payer_id", ""),
            "payer_name": rec.get("payer_name", ""),
            "claim_id": rec.get("claim_id", ""),
            "patient_id": rec.get("patient_id", ""),
            "severity": sev,
            "alert_text": (f"HIGH-$ DENIAL{prev_tag}: Claim {rec.get('claim_id','')} "
                           f"({rec.get('payer_name','')}) denied for ${billed:,.0f}. "
                           f"Reason: {rec.get('denial_reason','')}."),
            "recommended_action": rec.get("recommended_action", "Review and appeal within filing deadline.") or "Review and appeal.",
            "dollar_impact": billed,
            "denial_reason": rec.get("denial_reason", "") or "",
            "days_until_expiry": 0,
        })

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# PAYER ALERT 3: AR_AGING_BREACH (payer crosses 45-day AR threshold)
# ============================================================================
# Trigger: latest AR snapshot per payer where avg_days_in_ar > 45
# Severity: >60 = CRITICAL, 50-60 = HIGH, 45-50 = MEDIUM
# Recipient: CFO + payer relations team
# ============================================================================

_cols, _rows = _kql_query("""
    ar_snapshot_events
    | summarize arg_max(event_timestamp, *) by payer_id
    | where avg_days_in_ar > 45
    | project event_timestamp, payer_id, payer_name, open_claims_count,
              total_ar_balance, avg_days_in_ar, claims_over_45_days,
              claims_over_90_days, net_collection_rate, trend_direction
""")
print(f"  Payers breaching 45-day AR threshold: {len(_rows)}")

_ar_alerts = []
if _rows:
    import uuid as _uuid3
    from datetime import datetime as _dt3
    _now_iso3 = _dt3.utcnow().isoformat()
    for r in _rows:
        rec = dict(zip(_cols, r))
        days = float(rec.get("avg_days_in_ar", 0) or 0)
        bal = float(rec.get("total_ar_balance", 0) or 0)
        trend = rec.get("trend_direction", "") or ""
        if days > 60:
            sev = "CRITICAL"
            action = "CFO escalation. Convene payer JOC. Hold balance with finance."
        elif days > 50:
            sev = "HIGH"
            action = "Payer relations escalation letter. Audit claim submission process."
        else:
            sev = "MEDIUM"
            action = "Add to monthly AR review. Track next-week trend."
        if trend == "INCREASING":
            action = "[TREND ↑] " + action
        _ar_alerts.append({
            "alert_id": str(_uuid3.uuid4()),
            "alert_timestamp": _now_iso3,
            "alert_type": "AR_AGING_BREACH",
            "payer_id": rec.get("payer_id", ""),
            "payer_name": rec.get("payer_name", ""),
            "claim_id": "",
            "patient_id": "",
            "severity": sev,
            "alert_text": (f"AR AGING: {rec.get('payer_name','')} avg days-in-AR = {days:.1f} "
                           f"(target <45). Open claims: {int(rec.get('open_claims_count',0))}, "
                           f"AR balance: ${bal:,.0f}, "
                           f">90 days: {int(rec.get('claims_over_90_days',0))}, trend: {trend}."),
            "recommended_action": action,
            "dollar_impact": bal,
            "denial_reason": "",
            "days_until_expiry": int(days),
        })

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# PAYER ALERT 4: AUTH_EXPIRING (prior auth expires soon, procedure pending)
# ============================================================================
# Trigger: latest auth lifecycle status per auth_id == EXPIRING_SOON or
#          (APPROVED with days_until_expiry ≤ 7 and scheduled_procedure_date in future)
# Severity: ≤2 days = CRITICAL, ≤5 days = HIGH, ≤7 days = MEDIUM
# Recipient: Scheduling team + Billing Specialist + provider office
# ============================================================================

_cols, _rows = _kql_query("""
    auth_lifecycle_events
    | summarize arg_max(event_timestamp, *) by auth_id
    | where (auth_status == "EXPIRING_SOON")
         or (auth_status == "APPROVED" and days_until_expiry between (1 .. 7))
    | project event_timestamp, auth_id, patient_id, provider_id, payer_id, payer_name,
              procedure_code, procedure_description, auth_status,
              days_until_expiry, scheduled_procedure_date, estimated_charge
""")
print(f"  Prior auths expiring soon: {len(_rows)}")

_auth_alerts = []
if _rows:
    import uuid as _uuid4
    from datetime import datetime as _dt4
    _now_iso4 = _dt4.utcnow().isoformat()
    for r in _rows:
        rec = dict(zip(_cols, r))
        days = int(rec.get("days_until_expiry", 0) or 0)
        charge = float(rec.get("estimated_charge", 0) or 0)
        if days <= 2:
            sev = "CRITICAL"
            action = "Same-day auth renewal request to payer + reschedule warning to scheduling team."
        elif days <= 5:
            sev = "HIGH"
            action = "Auto-submit auth renewal via Power Automate. Confirm with provider office."
        else:
            sev = "MEDIUM"
            action = "Add to weekly auth-expiry queue. Verify procedure still scheduled."
        _auth_alerts.append({
            "alert_id": str(_uuid4.uuid4()),
            "alert_timestamp": _now_iso4,
            "alert_type": "AUTH_EXPIRING",
            "payer_id": rec.get("payer_id", ""),
            "payer_name": rec.get("payer_name", ""),
            "claim_id": rec.get("auth_id", ""),
            "patient_id": rec.get("patient_id", ""),
            "severity": sev,
            "alert_text": (f"AUTH EXPIRING: Auth {rec.get('auth_id','')} ({rec.get('procedure_code','')}) "
                           f"with {rec.get('payer_name','')} expires in {days} days "
                           f"(scheduled {rec.get('scheduled_procedure_date','')[:10]}). "
                           f"At-risk charge: ${charge:,.0f}."),
            "recommended_action": action,
            "dollar_impact": charge,
            "denial_reason": "",
            "days_until_expiry": days,
        })

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Union all + write to Delta ----------
_all_alerts = _denial_alerts + _hi_alerts + _ar_alerts + _auth_alerts
print(f"\n  Total payer alerts to write: {len(_all_alerts)}")

if _all_alerts:
    import pandas as _pd
    _df_pd = _pd.DataFrame(_all_alerts)
    df_payer_alerts = spark.createDataFrame(_df_pd)
    df_payer_alerts = df_payer_alerts.withColumn("alert_timestamp", F.to_timestamp("alert_timestamp"))
    df_payer_alerts.write.format("delta").mode("overwrite").saveAsTable("lh_gold_curated.rti_payer_alerts")
    print(f"  rti_payer_alerts (Delta): {df_payer_alerts.count()} rows written")
else:
    df_payer_alerts = None
    print("  No payer alerts generated this run.")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Push payer_alerts to KQL (for Activator → Teams / Power Automate)
# ============================================================================
print("\nPushing payer_alerts to KQL...")

if df_payer_alerts is not None and df_payer_alerts.count() > 0 and _KUSTO_QUERY_URI and _KUSTO_INGEST_URI:
    try:
        from azure.kusto.ingest import ManagedStreamingIngestClient, IngestionProperties
        from azure.kusto.data import DataFormat
        import io as _io

        _df_kql = df_payer_alerts.toPandas()
        _tok2 = notebookutils.credentials.getToken("kusto")
        _eng = KustoConnectionStringBuilder.with_aad_user_token_authentication(_KUSTO_QUERY_URI, _tok2)
        _dm = KustoConnectionStringBuilder.with_aad_user_token_authentication(_KUSTO_INGEST_URI, _tok2)

        _client = ManagedStreamingIngestClient(_eng, _dm)
        _props = IngestionProperties(
            database=_KQL_DB_NAME, table="payer_alerts",
            data_format=DataFormat.JSON,
            ingestion_mapping_reference="payer_alerts_mapping"
        )
        _json_data = _df_kql.to_json(orient="records", lines=True, date_format="iso")
        _client.ingest_from_stream(_io.StringIO(_json_data), ingestion_properties=_props)
        print(f"  KQL: {len(_df_kql)} payer alerts streamed → payer_alerts")
    except Exception as _e:
        print(f"  KQL WARN: payer_alerts ingestion failed: {_e}")
else:
    print("  KQL: nothing to push (no alerts or no Eventhouse)")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Summary for demo ----------
print("\n" + "=" * 60)
print("PAYER OPERATIONAL ALERTS SUMMARY")
print("=" * 60)
print(f"  Denial spike alerts:        {len(_denial_alerts)}")
print(f"  High-dollar denial alerts:  {len(_hi_alerts)}")
print(f"  AR aging breach alerts:     {len(_ar_alerts)}")
print(f"  Prior auth expiring alerts: {len(_auth_alerts)}")
print(f"  Total written:              {len(_all_alerts)}")
print()
_total_dollars = sum(a.get("dollar_impact", 0) for a in _all_alerts)
print(f"  Total dollar impact in flight: ${_total_dollars:,.0f}")
print()
print("Downstream consumers (configure in Fabric Activator):")
print("  • DENIAL_SPIKE (CRITICAL)        → Teams card to CFO + RCM Director + RCA Planner task")
print("  • HIGH_DOLLAR_DENIAL (CRITICAL)  → Power Automate → appeal task auto-created in queue")
print("  • AR_AGING_BREACH (CRITICAL)     → Email to CFO + payer JOC scheduling flow")
print("  • AUTH_EXPIRING (CRITICAL/HIGH)  → Power Automate → payer portal renewal + Teams DM scheduler")
print("=" * 60)
print("NB_RTI_Payer_Alerts: COMPLETE")
