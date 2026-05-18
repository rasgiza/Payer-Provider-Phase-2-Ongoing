# Fabric notebook source

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# # RTI: Provider Operational Alerts (CMO / Care Manager / Medical Director)
#
# Generates real-time alerts for the **Provider Agent** persona group:
#   1. **READMISSION_30DAY** — patient re-admitted within 30 days (HRRP penalty risk)
#   2. **QUALITY_DRIFT** — provider's rolling quality metric deviates from benchmark
#
# **Inputs** (from Eventstream → Eventhouse, set up by NB_RTI_Setup_Eventhouse):
#   - `readmission_events` (KQL)
#   - `quality_metric_events` (KQL)
#
# **Outputs**:
#   - Delta: `lh_gold_curated.rti_provider_alerts`
#   - KQL: `provider_alerts` (consumed by Activator → Teams / Power Automate)
#
# **Default lakehouse:** `lh_gold_curated`

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

print("NB_RTI_Provider_Alerts: Starting...")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Attach default lakehouse (self-healing pattern) ----------
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
print("Loading provider events from KQL Eventhouse...")

# Backfill typed tables from rti_all_events landing if needed
_backfill_map = {
    "readmission_events": "ExtractReadmissionEvents()",
    "quality_metric_events": "ExtractQualityMetricEvents()",
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

# Backfill typed tables
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
# PROVIDER ALERT 1: READMISSION_30DAY
# ============================================================================
# Trigger: any readmission_event where days_since_discharge ≤ 30
# Severity:  ≤7 days = CRITICAL, ≤14 days = HIGH, ≤30 days = MEDIUM
# Recipient: Care Manager (Lisa) for HIGH/CRITICAL, Provider for MEDIUM
# ============================================================================

_cols, _rows = _kql_query("""
    readmission_events
    | project event_id, event_timestamp, patient_id, provider_id, facility_id,
              current_encounter_id, prior_discharge_date, days_since_discharge,
              current_diagnosis, prior_diagnosis, readmission_risk_score,
              drg_code, latitude, longitude
""")
print(f"  readmission_events in KQL: {len(_rows)}")

if _rows:
    df_readmit = spark.createDataFrame([dict(zip(_cols, r)) for r in _rows])
    df_readmit = df_readmit.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))

    # Enrich with provider name (current SCD2 row only)
    try:
        df_prov = spark.sql("""
            SELECT provider_id, display_name AS provider_name, specialty
            FROM lh_gold_curated.dim_provider WHERE is_current = true
        """)
        df_readmit = df_readmit.join(df_prov, "provider_id", "left")
    except Exception as _e:
        print(f"  [WARN] could not join dim_provider: {_e}")
        df_readmit = df_readmit.withColumn("provider_name", F.lit("Unknown")).withColumn("specialty", F.lit(""))

    df_readmit_alerts = (df_readmit
        .withColumn("alert_id", F.expr("uuid()"))
        .withColumn("alert_timestamp", F.current_timestamp())
        .withColumn("alert_type", F.lit("READMISSION_30DAY"))
        .withColumn("severity",
            F.when(F.col("days_since_discharge") <= 7, F.lit("CRITICAL"))
             .when(F.col("days_since_discharge") <= 14, F.lit("HIGH"))
             .otherwise(F.lit("MEDIUM"))
        )
        .withColumn("alert_text", F.concat(
            F.lit("READMISSION ALERT: Patient "), F.col("patient_id"),
            F.lit(" re-admitted "), F.col("days_since_discharge").cast("string"),
            F.lit(" days after discharge (DRG "), F.col("drg_code"),
            F.lit(", risk score "), F.col("readmission_risk_score").cast("string"),
            F.lit("). Prior dx: "), F.col("prior_diagnosis"),
            F.lit(", current dx: "), F.col("current_diagnosis"), F.lit(".")
        ))
        .withColumn("recommended_action",
            F.when(F.col("severity") == "CRITICAL",
                F.lit("Immediate care manager review + discharge bundle audit. Notify CMO."))
             .when(F.col("severity") == "HIGH",
                F.lit("Care manager outreach within 24h. Schedule follow-up visit + medication reconciliation."))
             .otherwise(F.lit("Add to weekly readmission review queue. Verify post-discharge plan."))
        )
        .withColumn("metric_name", F.lit("days_since_discharge"))
        .withColumn("metric_value", F.col("days_since_discharge").cast("double"))
        .withColumn("benchmark_value", F.lit(30.0))
        .select(
            "alert_id", "alert_timestamp", "alert_type", "provider_id",
            F.coalesce(F.col("provider_name"), F.lit("Unknown")).alias("provider_name"),
            "patient_id", "facility_id", "severity", "alert_text",
            "recommended_action", "metric_name", "metric_value", "benchmark_value",
            F.coalesce(F.col("latitude"), F.lit(40.84)).alias("latitude"),
            F.coalesce(F.col("longitude"), F.lit(-73.94)).alias("longitude"),
        )
    )
    _readmit_count = df_readmit_alerts.count()
    print(f"  Readmission alerts: {_readmit_count}")
else:
    df_readmit_alerts = None
    _readmit_count = 0

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# PROVIDER ALERT 2: QUALITY_DRIFT
# ============================================================================
# Trigger: provider's metric_value deviates significantly from benchmark
#   - lower_is_better: value > benchmark * 1.30 → CRITICAL, > 1.15 → HIGH
#   - higher_is_better: value < benchmark * 0.70 → CRITICAL, < 0.85 → HIGH
# Recipient: Medical Director (Dr. Patel) + the provider
# ============================================================================

_cols, _rows = _kql_query("""
    quality_metric_events
    | summarize arg_max(event_timestamp, *) by provider_id, metric_name
    | project event_id, event_timestamp, provider_id, provider_name, specialty,
              facility_id, metric_name, metric_value, benchmark_value, direction,
              denominator, numerator
""")
print(f"  quality_metric_events (latest per provider/metric): {len(_rows)}")

if _rows:
    df_qm = spark.createDataFrame([dict(zip(_cols, r)) for r in _rows])
    df_qm = df_qm.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))

    # Compute deviation ratio
    df_qm = df_qm.withColumn(
        "deviation_ratio",
        F.when(F.col("benchmark_value") != 0, F.col("metric_value") / F.col("benchmark_value")).otherwise(F.lit(1.0))
    )

    df_qm = df_qm.withColumn(
        "severity",
        F.when((F.col("direction") == "lower_is_better") & (F.col("deviation_ratio") > 1.30), F.lit("CRITICAL"))
         .when((F.col("direction") == "lower_is_better") & (F.col("deviation_ratio") > 1.15), F.lit("HIGH"))
         .when((F.col("direction") == "higher_is_better") & (F.col("deviation_ratio") < 0.70), F.lit("CRITICAL"))
         .when((F.col("direction") == "higher_is_better") & (F.col("deviation_ratio") < 0.85), F.lit("HIGH"))
         .otherwise(F.lit("OK"))
    )

    # Only emit alerts (filter OK)
    df_qm_alerts_src = df_qm.filter(F.col("severity") != "OK")

    df_qm_alerts = (df_qm_alerts_src
        .withColumn("alert_id", F.expr("uuid()"))
        .withColumn("alert_timestamp", F.current_timestamp())
        .withColumn("alert_type", F.lit("QUALITY_DRIFT"))
        .withColumn("alert_text", F.concat(
            F.lit("QUALITY DRIFT: Provider "), F.col("provider_name"),
            F.lit(" ("), F.col("specialty"), F.lit(") metric '"),
            F.col("metric_name"), F.lit("' = "),
            F.round(F.col("metric_value"), 4).cast("string"),
            F.lit(" vs benchmark "), F.round(F.col("benchmark_value"), 4).cast("string"),
            F.lit(" ("), F.col("direction"), F.lit(", n="),
            F.col("denominator").cast("string"), F.lit(").")
        ))
        .withColumn("recommended_action",
            F.when(F.col("severity") == "CRITICAL",
                F.lit("Schedule peer review with Medical Director. Audit recent cases. Consider PIP enrollment."))
             .otherwise(F.lit("Share scorecard with provider. Add to monthly quality huddle agenda."))
        )
        .withColumn("patient_id", F.lit(""))
        .select(
            "alert_id", "alert_timestamp", "alert_type", "provider_id", "provider_name",
            "patient_id", "facility_id", "severity", "alert_text", "recommended_action",
            "metric_name", "metric_value", "benchmark_value",
            F.lit(40.84).alias("latitude"), F.lit(-73.94).alias("longitude"),
        )
    )
    _qm_count = df_qm_alerts.count()
    print(f"  Quality drift alerts: {_qm_count}")
else:
    df_qm_alerts = None
    _qm_count = 0

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Union + write to Delta ----------
df_all = None
if df_readmit_alerts is not None and df_qm_alerts is not None:
    df_all = df_readmit_alerts.unionByName(df_qm_alerts)
elif df_readmit_alerts is not None:
    df_all = df_readmit_alerts
elif df_qm_alerts is not None:
    df_all = df_qm_alerts

if df_all is not None and df_all.count() > 0:
    df_all.write.format("delta").mode("overwrite").saveAsTable("lh_gold_curated.rti_provider_alerts")
    print(f"\n  rti_provider_alerts (Delta): {df_all.count()} rows written")
else:
    print("\n  No provider alerts generated this run.")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Push provider_alerts to KQL (for Activator → Teams / Power Automate)
# ============================================================================
print("\nPushing provider_alerts to KQL...")

if df_all is not None and df_all.count() > 0 and _KUSTO_QUERY_URI and _KUSTO_INGEST_URI:
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
            database=_KQL_DB_NAME, table="provider_alerts",
            data_format=DataFormat.JSON,
            ingestion_mapping_reference="provider_alerts_mapping"
        )
        _json_data = _df_kql.to_json(orient="records", lines=True, date_format="iso")
        _client.ingest_from_stream(_io.StringIO(_json_data), ingestion_properties=_props)
        print(f"  KQL: {len(_df_kql)} provider alerts streamed → provider_alerts")
    except Exception as _e:
        print(f"  KQL WARN: provider_alerts ingestion failed: {_e}")
else:
    print("  KQL: nothing to push (no alerts or no Eventhouse)")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Summary for demo ----------
print("\n" + "=" * 60)
print("PROVIDER OPERATIONAL ALERTS SUMMARY")
print("=" * 60)
print(f"  Readmission alerts:    {_readmit_count}")
print(f"  Quality drift alerts:  {_qm_count}")
print(f"  Total written:         {(_readmit_count + _qm_count)}")
print()
print("Downstream consumers (configure in Fabric Activator):")
print("  • READMISSION_30DAY (CRITICAL/HIGH) → Teams card to Care Manager + CMO email")
print("  • QUALITY_DRIFT (CRITICAL)         → Power Automate → peer review task in Planner")
print("  • QUALITY_DRIFT (HIGH)             → Teams adaptive card to Medical Director")
print("=" * 60)
print("NB_RTI_Provider_Alerts: COMPLETE")
