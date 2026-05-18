# Fabric notebook source

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# # RTI: CTO Platform Health Alerts (DQ / PHI / Model Drift)
#
# Generates real-time alerts for the **CTO / Platform / Security** persona:
#   1. **DQ_THRESHOLD_BREACH** — data quality violations exceed configured pct
#   2. **PHI_ANOMALY**         — anomalous PHI access (geo / action / score)
#   3. **MODEL_DRIFT**         — production model drift (PSI ≥ 0.1 / 0.25)
#
# **Inputs** (from Eventstream → Eventhouse, set up by NB_RTI_Setup_Eventhouse):
#   - `dq_violation_events` (KQL)
#   - `audit_access_events` (KQL)
#   - `model_drift_events` (KQL)
#   - `dashboard_thresholds` (KQL config table)
#
# **Outputs**:
#   - Delta: `lh_gold_curated.rti_cto_alerts`
#   - KQL: `cto_alerts`
#
# **Default lakehouse:** `lh_gold_curated`

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

print("NB_RTI_CTO_Alerts: Starting...")

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

# ---------- Wait + backfill typed tables from rti_all_events ----------
print("Loading platform health events from KQL Eventhouse...")
_backfill_map = {
    "dq_violation_events": "ExtractDqViolationEvents()",
    "audit_access_events": "ExtractAuditAccessEvents()",
    "model_drift_events": "ExtractModelDriftEvents()",
}
for _i in range(1, 19):
    _, _r = _kql_query("rti_all_events | count")
    if _r and int(_r[0][0]) > 0:
        print(f"  rti_all_events: {int(_r[0][0])} rows")
        break
    print(f"  [{_i}/18] waiting...")
    _wait_time.sleep(10)

for _tbl, _fn in _backfill_map.items():
    _, _r = _kql_query(f"{_tbl} | count")
    _cnt = int(_r[0][0]) if _r and _r[0] else 0
    if _cnt == 0:
        try:
            _kql_mgmt(f".set-or-append {_tbl} <| {_fn}")
            _, _r2 = _kql_query(f"{_tbl} | count")
            print(f"  Backfilled {_tbl}: {int(_r2[0][0])} rows")
        except Exception as _e:
            print(f"  [WARN] backfill {_tbl}: {_e}")
    else:
        print(f"  {_tbl}: {_cnt} rows (already populated)")

def _load_threshold(key, default_warn, default_crit):
    try:
        _, _r = _kql_query(f"dashboard_thresholds | where threshold_key == '{key}' and active == true | take 1")
        if _r and _r[0]:
            row = _r[0]
            return float(row[3] or default_warn), float(row[4] or default_crit)
    except Exception:
        pass
    return default_warn, default_crit

_dq_warn, _dq_crit       = _load_threshold("dq_violation_pct", 1.0, 5.0)
_phi_warn, _phi_crit     = _load_threshold("phi_anomaly_score", 0.7, 0.9)
_drift_warn, _drift_crit = _load_threshold("model_drift_psi", 0.1, 0.25)
print(f"  Thresholds — DQ:{_dq_warn}/{_dq_crit}  PHI:{_phi_warn}/{_phi_crit}  DRIFT:{_drift_warn}/{_drift_crit}")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# CTO ALERT 1: DQ_THRESHOLD_BREACH
# ============================================================================
_cols, _rows = _kql_query("""
    dq_violation_events
    | where event_timestamp > ago(1h)
    | summarize arg_max(event_timestamp, *) by source_table, rule_name
""")
print(f"  dq_violation_events (latest per rule, 1h): {len(_rows)}")

df_dq_alerts = None
if _rows:
    df_dq = spark.createDataFrame([dict(zip(_cols, r)) for r in _rows])
    df_dq = df_dq.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    df_dq = df_dq.withColumn(
        "severity_calc",
        F.when(F.col("violation_pct") >= F.lit(_dq_crit), F.lit("CRITICAL"))
         .when(F.col("violation_pct") >= F.lit(_dq_warn), F.lit("HIGH"))
         .otherwise(F.lit("OK"))
    )
    df_dq_src = df_dq.filter(F.col("severity_calc") != "OK")
    df_dq_alerts = (df_dq_src
        .withColumn("alert_id", F.expr("uuid()"))
        .withColumn("alert_timestamp", F.current_timestamp())
        .withColumn("alert_type", F.lit("DQ_THRESHOLD_BREACH"))
        .withColumn("source_system", F.col("source_table"))
        .withColumn("severity", F.col("severity_calc"))
        .withColumn("alert_text", F.concat(
            F.lit("DQ BREACH: rule '"), F.col("rule_name"),
            F.lit("' on "), F.col("source_table"),
            F.lit(" — "), F.col("rule_type"),
            F.lit(" violation pct="), F.round(F.col("violation_pct"), 2).cast("string"),
            F.lit("% (count="), F.col("violation_count").cast("string"), F.lit(")")
        ))
        .withColumn("recommended_action",
            F.when(F.col("severity") == "CRITICAL",
                   F.lit("Pause downstream pipelines. Open incident in PagerDuty. Notify data steward."))
             .otherwise(F.lit("Add to data-quality backlog. Investigate within 24h."))
        )
        .withColumn("metric_name", F.lit("violation_pct"))
        .withColumn("metric_value", F.col("violation_pct"))
        .withColumn("threshold_value", F.lit(float(_dq_warn)))
        .select(
            "alert_id", "alert_timestamp", "alert_type", "source_system",
            "severity", "alert_text", "recommended_action",
            "metric_name", "metric_value", "threshold_value",
        )
    )
    print(f"  DQ alerts: {df_dq_alerts.count()}")

# ============================================================================
# CTO ALERT 2: PHI_ANOMALY
# ============================================================================
_cols, _rows = _kql_query("""
    audit_access_events
    | where event_timestamp > ago(1h)
    | where anomaly_score > 0.4
""")
print(f"  audit_access_events (anomaly>0.4, 1h): {len(_rows)}")

df_phi_alerts = None
if _rows:
    df_au = spark.createDataFrame([dict(zip(_cols, r)) for r in _rows])
    df_au = df_au.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    df_au = df_au.withColumn(
        "severity_calc",
        F.when(F.col("anomaly_score") >= F.lit(_phi_crit), F.lit("CRITICAL"))
         .when(F.col("anomaly_score") >= F.lit(_phi_warn), F.lit("HIGH"))
         .otherwise(F.lit("MEDIUM"))
    )
    df_phi_alerts = (df_au
        .withColumn("alert_id", F.expr("uuid()"))
        .withColumn("alert_timestamp", F.current_timestamp())
        .withColumn("alert_type", F.lit("PHI_ANOMALY"))
        .withColumn("source_system", F.lit("audit_access"))
        .withColumn("severity", F.col("severity_calc"))
        .withColumn("alert_text", F.concat(
            F.lit("PHI ANOMALY: "), F.col("user_principal"),
            F.lit(" "), F.col("action_type"),
            F.lit(" on "), F.col("resource_accessed"),
            F.lit(" from "), F.col("geo_country"),
            F.lit(" — anomaly_score="), F.round(F.col("anomaly_score"), 3).cast("string")
        ))
        .withColumn("recommended_action",
            F.when(F.col("severity") == "CRITICAL",
                   F.lit("Suspend user session. Open security incident. Notify CISO + Privacy Office."))
             .when(F.col("severity") == "HIGH",
                   F.lit("Step-up MFA. Notify SOC. Add to access review for next 24h."))
             .otherwise(F.lit("Log to audit review queue."))
        )
        .withColumn("metric_name", F.lit("anomaly_score"))
        .withColumn("metric_value", F.col("anomaly_score"))
        .withColumn("threshold_value", F.lit(float(_phi_warn)))
        .select(
            "alert_id", "alert_timestamp", "alert_type", "source_system",
            "severity", "alert_text", "recommended_action",
            "metric_name", "metric_value", "threshold_value",
        )
    )
    print(f"  PHI alerts: {df_phi_alerts.count()}")

# ============================================================================
# CTO ALERT 3: MODEL_DRIFT
# ============================================================================
_cols, _rows = _kql_query("""
    model_drift_events
    | where event_timestamp > ago(24h)
    | summarize arg_max(event_timestamp, *) by model_name, feature_name
""")
print(f"  model_drift_events (latest per model/feature, 24h): {len(_rows)}")

df_drift_alerts = None
if _rows:
    df_md = spark.createDataFrame([dict(zip(_cols, r)) for r in _rows])
    df_md = df_md.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    df_md = df_md.withColumn(
        "severity_calc",
        F.when(F.col("psi_value") >= F.lit(_drift_crit), F.lit("CRITICAL"))
         .when(F.col("psi_value") >= F.lit(_drift_warn), F.lit("HIGH"))
         .otherwise(F.lit("OK"))
    )
    df_md_src = df_md.filter(F.col("severity_calc") != "OK")
    df_drift_alerts = (df_md_src
        .withColumn("alert_id", F.expr("uuid()"))
        .withColumn("alert_timestamp", F.current_timestamp())
        .withColumn("alert_type", F.lit("MODEL_DRIFT"))
        .withColumn("source_system", F.col("model_name"))
        .withColumn("severity", F.col("severity_calc"))
        .withColumn("alert_text", F.concat(
            F.lit("MODEL DRIFT: "), F.col("model_name"),
            F.lit(" "), F.col("model_version"),
            F.lit(" feature="), F.col("feature_name"),
            F.lit(" PSI="), F.round(F.col("psi_value"), 4).cast("string"),
            F.lit(" KS="), F.round(F.col("ks_value"), 4).cast("string"),
            F.lit(" baseline="), F.col("baseline_period")
        ))
        .withColumn("recommended_action",
            F.when(F.col("severity") == "CRITICAL",
                   F.lit("Hold model promotion. Trigger retraining pipeline. Notify ML platform team."))
             .otherwise(F.lit("Add to weekly model review. Refresh feature monitor dashboards."))
        )
        .withColumn("metric_name", F.lit("psi_value"))
        .withColumn("metric_value", F.col("psi_value"))
        .withColumn("threshold_value", F.lit(float(_drift_warn)))
        .select(
            "alert_id", "alert_timestamp", "alert_type", "source_system",
            "severity", "alert_text", "recommended_action",
            "metric_name", "metric_value", "threshold_value",
        )
    )
    print(f"  Drift alerts: {df_drift_alerts.count()}")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

df_all = None
for _df in [df_dq_alerts, df_phi_alerts, df_drift_alerts]:
    if _df is not None and _df.count() > 0:
        df_all = _df if df_all is None else df_all.unionByName(_df)

if df_all is not None and df_all.count() > 0:
    df_all.write.format("delta").mode("overwrite").saveAsTable("lh_gold_curated.rti_cto_alerts")
    print(f"\n  rti_cto_alerts (Delta): {df_all.count()} rows written")

    if _KUSTO_QUERY_URI and _KUSTO_INGEST_URI:
        try:
            from azure.kusto.ingest import ManagedStreamingIngestClient, IngestionProperties
            from azure.kusto.data import DataFormat
            import io as _io

            _df_kql = df_all.toPandas()
            _tok2 = notebookutils.credentials.getToken("kusto")
            _eng = KustoConnectionStringBuilder.with_aad_user_token_authentication(_KUSTO_QUERY_URI, _tok2)
            _dm = KustoConnectionStringBuilder.with_aad_user_token_authentication(_KUSTO_INGEST_URI, _tok2)
            _client = ManagedStreamingIngestClient(_eng, _dm)
            _props = IngestionProperties(
                database=_KQL_DB_NAME, table="cto_alerts",
                data_format=DataFormat.JSON,
                ingestion_mapping_reference="cto_alerts_mapping"
            )
            _json_data = _df_kql.to_json(orient="records", lines=True, date_format="iso")
            _client.ingest_from_stream(_io.StringIO(_json_data), ingestion_properties=_props)
            print(f"  KQL: {len(_df_kql)} CTO alerts streamed → cto_alerts")
        except Exception as _e:
            print(f"  KQL WARN: cto_alerts ingestion failed: {_e}")
else:
    print("\n  No CTO alerts generated this run.")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

print("\n" + "=" * 60)
print("CTO PLATFORM HEALTH ALERTS SUMMARY")
print("=" * 60)
print(f"  DQ alerts:      {df_dq_alerts.count() if df_dq_alerts else 0}")
print(f"  PHI alerts:     {df_phi_alerts.count() if df_phi_alerts else 0}")
print(f"  Drift alerts:   {df_drift_alerts.count() if df_drift_alerts else 0}")
print()
print("Downstream consumers (configure in Fabric Activator):")
print("  • DQ_THRESHOLD_BREACH (CRITICAL) → Pager + pause downstream pipeline")
print("  • PHI_ANOMALY (CRITICAL)         → Suspend session + page CISO/Privacy")
print("  • MODEL_DRIFT (CRITICAL)         → Hold promotion + trigger retrain pipeline")
print("=" * 60)
print("NB_RTI_CTO_Alerts: COMPLETE")
