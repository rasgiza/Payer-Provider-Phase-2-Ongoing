# Fabric notebook source

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# # RTI: Alert Closure / MTTR Tracker
#
# Consumes alert closure events written back from Power Automate / Activator
# and computes Mean-Time-To-Resolve (MTTR) by alert_type and persona.
#
# **Inputs**:
#   - `alert_closure_events` (KQL — populated by Power Automate flow when an
#      operator clicks "Acknowledge" or "Resolve" on a Teams adaptive card)
#   - All `*_alerts` KQL tables (provider, payer, coo, cto, fraud, etc.)
#
# **Outputs**:
#   - Delta: `lh_gold_curated.rti_alert_mttr`  (rolling 24h MTTR by persona/type)
#   - KQL ingestion endpoint guidance for Power Automate flow callback
#
# **Default lakehouse:** `lh_gold_curated`

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

print("NB_RTI_Alert_Closure: Starting...")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

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
del _req, _ws_id, _tok, _hdr, _lh_resp

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

%pip install azure-kusto-data azure-kusto-ingest azure-core>=1.31.0 --quiet

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

from pyspark.sql import functions as F
import requests as _kql_req
import subprocess, sys
try:
    from azure.kusto.data import KustoConnectionStringBuilder, KustoClient
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "azure-kusto-data", "azure-kusto-ingest"])
    for _m in list(sys.modules.keys()):
        if _m.startswith("azure"): del sys.modules[_m]
    from azure.kusto.data import KustoConnectionStringBuilder, KustoClient

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

# METADATA **{"language":"python"}**

# CELL **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# ## Power Automate Closure Flow — Receiver Endpoint
#
# Configure your Power Automate flow (triggered by Teams adaptive card actions
# **Acknowledge** / **Resolve**) to **POST** a JSON body to the Eventstream HTTP
# endpoint, OR call the KQL streaming ingest endpoint directly.
#
# **JSON shape expected by `alert_closure_events`:**
#
# ```json
# {
#   "_table": "alert_closure_events",
#   "event_id": "<guid>",
#   "event_timestamp": "2025-10-31T14:22:00Z",
#   "alert_id": "<original alert_id>",
#   "alert_type": "READMISSION_30DAY",
#   "persona": "CMO",
#   "facility_id": "FAC001",
#   "user_principal": "lisa.carter@contoso.com",
#   "action_type": "RESOLVED",
#   "severity": "CRITICAL",
#   "notes": "Care manager outreach completed; follow-up scheduled."
# }
# ```
#
# Push to Eventstream HTTP source `eventstream-rti-source` (single source-of-truth
# pipeline). Update policies will auto-route this row to `alert_closure_events`.

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Compute MTTR by joining alerts to closures
# ============================================================================
_mttr_query = """
let closures = alert_closure_events
    | where event_timestamp > ago(24h)
    | summarize
        ack_time   = minif(event_timestamp, action_type == "ACKNOWLEDGED"),
        close_time = minif(event_timestamp, action_type == "RESOLVED")
        by alert_id, alert_type, persona;
let alerts_union = union
    (provider_alerts | extend persona = "CMO"),
    (payer_alerts    | extend persona = "CFO"),
    (coo_alerts      | extend persona = "COO"),
    (cto_alerts      | extend persona = "CTO")
    | where alert_timestamp > ago(24h)
    | project alert_id, alert_type, persona, alert_timestamp, severity;
alerts_union
| join kind=leftouter closures on alert_id
| extend
    mttr_minutes_ack   = iif(isnotnull(ack_time),   datetime_diff('minute', ack_time,   alert_timestamp), int(null)),
    mttr_minutes_close = iif(isnotnull(close_time), datetime_diff('minute', close_time, alert_timestamp), int(null)),
    status = case(isnotnull(close_time), "RESOLVED", isnotnull(ack_time), "ACKNOWLEDGED", "OPEN")
| summarize
    open_count          = countif(status == "OPEN"),
    ack_count           = countif(status == "ACKNOWLEDGED"),
    resolved_count      = countif(status == "RESOLVED"),
    p50_ack_minutes     = percentile(mttr_minutes_ack, 50),
    p90_ack_minutes     = percentile(mttr_minutes_ack, 90),
    p50_close_minutes   = percentile(mttr_minutes_close, 50),
    p90_close_minutes   = percentile(mttr_minutes_close, 90),
    avg_close_minutes   = avg(mttr_minutes_close)
    by persona, alert_type
| order by persona asc, alert_type asc
"""

_cols, _rows = _kql_query(_mttr_query)
print(f"  MTTR rows computed: {len(_rows)}")

if _rows:
    df_mttr = spark.createDataFrame([dict(zip(_cols, r)) for r in _rows])
    df_mttr = df_mttr.withColumn("computed_at", F.current_timestamp())
    df_mttr.write.format("delta").mode("overwrite").saveAsTable("lh_gold_curated.rti_alert_mttr")
    print(f"  rti_alert_mttr (Delta): {df_mttr.count()} rows written")
    df_mttr.show(50, truncate=False)
else:
    print("  No alerts in last 24h — MTTR table empty.")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

print("\n" + "=" * 60)
print("ALERT CLOSURE / MTTR TRACKER SUMMARY")
print("=" * 60)
print("Power Automate flow callback: POST closure events to Eventstream HTTP source")
print("with _table='alert_closure_events' and action_type in (ACKNOWLEDGED, RESOLVED).")
print()
print("Dashboard tile suggestion:")
print("  • SLA Burn-Down Heatmap (persona × alert_type, color = p90_close_minutes)")
print("  • Open vs Resolved gauge (last 24h)")
print("  • MTTR trend line (per persona)")
print("=" * 60)
print("NB_RTI_Alert_Closure: COMPLETE")
