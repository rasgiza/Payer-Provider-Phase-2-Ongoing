# Fabric notebook source

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# # RTI: COO Operational Alerts (ED / Bed / OR / Staffing)
#
# Generates real-time alerts for the **COO** persona:
#   1. **ED_BOARDING_BREACH**     — ED boarding count > threshold
#   2. **BED_CAPACITY_CRITICAL**  — bed occupancy ≥ 95%
#   3. **OR_DELAY**               — OR turnover > 60 minutes
#   4. **STAFFING_GAP**           — nurse-to-acuity ratio breach
#
# **Inputs** (from Eventstream → Eventhouse, set up by NB_RTI_Setup_Eventhouse):
#   - `ops_capacity_events` (KQL)
#   - `staffing_acuity_events` (KQL)
#   - `dashboard_thresholds` (KQL config table)
#
# **Outputs**:
#   - Delta: `lh_gold_curated.rti_coo_alerts`
#   - KQL: `coo_alerts` (consumed by Activator → Teams / Power Automate)
#
# **Default lakehouse:** `lh_gold_curated`

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

print("NB_RTI_COO_Alerts: Starting...")

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

# ---------- Wait + backfill typed tables from rti_all_events ----------
print("Loading COO operational events from KQL Eventhouse...")
_backfill_map = {
    "ops_capacity_events": "ExtractOpsCapacityEvents()",
    "staffing_acuity_events": "ExtractStaffingAcuityEvents()",
}
for _i in range(1, 19):
    _, _r = _kql_query("rti_all_events | count")
    if _r and int(_r[0][0]) > 0:
        print(f"  rti_all_events: {int(_r[0][0])} rows (source ready)")
        break
    print(f"  [{_i}/18] waiting for rti_all_events...")
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

# ---------- Load thresholds from KQL config table ----------
def _load_threshold(key, default_warn, default_crit):
    try:
        _, _r = _kql_query(f"dashboard_thresholds | where threshold_key == '{key}' and active == true | take 1")
        if _r and _r[0]:
            row = _r[0]
            # cols: threshold_key, alert_type, persona, warn_value, critical_value, ...
            return float(row[3] or default_warn), float(row[4] or default_crit)
    except Exception:
        pass
    return default_warn, default_crit

_ed_warn, _ed_crit       = _load_threshold("ed_boarding_count", 10.0, 20.0)
_bed_warn, _bed_crit     = _load_threshold("bed_occupancy_pct", 85.0, 95.0)
_or_warn, _or_crit       = _load_threshold("or_turnover_minutes", 45.0, 60.0)
_staff_warn, _staff_crit = _load_threshold("staffing_gap_count", 1.0, 3.0)
print(f"  Thresholds — ED:{_ed_warn}/{_ed_crit}  BED:{_bed_warn}/{_bed_crit}  OR:{_or_warn}/{_or_crit}  STAFF:{_staff_warn}/{_staff_crit}")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# COO ALERT 1-3: capacity-driven (ED / BED / OR)
# ============================================================================
_cols, _rows = _kql_query("""
    ops_capacity_events
    | where event_timestamp > ago(1h)
    | summarize arg_max(event_timestamp, *) by facility_id, capacity_type
""")
print(f"  ops_capacity_events (latest per facility/type, 1h): {len(_rows)}")

df_cap_alerts = None
if _rows:
    df_cap = spark.createDataFrame([dict(zip(_cols, r)) for r in _rows])
    df_cap = df_cap.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))

    df_cap = df_cap.withColumn(
        "alert_type",
        F.when(F.col("capacity_type") == "ED_BOARDING",   F.lit("ED_BOARDING_BREACH"))
         .when(F.col("capacity_type") == "BED_OCCUPANCY", F.lit("BED_CAPACITY_CRITICAL"))
         .when(F.col("capacity_type") == "OR_TURNOVER",   F.lit("OR_DELAY"))
         .otherwise(F.lit("LOS_BREACH"))
    ).withColumn(
        "severity",
        F.when((F.col("capacity_type") == "ED_BOARDING")   & (F.col("current_value") >= F.lit(_ed_crit)),   F.lit("CRITICAL"))
         .when((F.col("capacity_type") == "ED_BOARDING")   & (F.col("current_value") >= F.lit(_ed_warn)),   F.lit("HIGH"))
         .when((F.col("capacity_type") == "BED_OCCUPANCY") & (F.col("current_value") >= F.lit(_bed_crit)),  F.lit("CRITICAL"))
         .when((F.col("capacity_type") == "BED_OCCUPANCY") & (F.col("current_value") >= F.lit(_bed_warn)),  F.lit("HIGH"))
         .when((F.col("capacity_type") == "OR_TURNOVER")   & (F.col("current_value") >= F.lit(_or_crit)),   F.lit("CRITICAL"))
         .when((F.col("capacity_type") == "OR_TURNOVER")   & (F.col("current_value") >= F.lit(_or_warn)),   F.lit("HIGH"))
         .otherwise(F.lit("OK"))
    )

    df_cap_alerts_src = df_cap.filter(F.col("severity") != "OK")
    df_cap_alerts = (df_cap_alerts_src
        .withColumn("alert_id", F.expr("uuid()"))
        .withColumn("alert_timestamp", F.current_timestamp())
        .withColumn("alert_text", F.concat(
            F.col("alert_type"), F.lit(": "), F.col("facility_name"),
            F.lit(" "), F.col("ward"), F.lit(" - "),
            F.col("capacity_type"), F.lit(" = "),
            F.round(F.col("current_value"), 1).cast("string"),
            F.lit(" (threshold "), F.round(F.col("threshold_value"), 1).cast("string"),
            F.lit(", utilization "), F.round(F.col("utilization_pct"), 1).cast("string"),
            F.lit("%).")
        ))
        .withColumn("recommended_action",
            F.when(F.col("alert_type") == "ED_BOARDING_BREACH",
                   F.lit("Activate surge protocol. Open escalation huddle. Page bed coordinator + ED charge."))
             .when(F.col("alert_type") == "BED_CAPACITY_CRITICAL",
                   F.lit("Run discharge readiness review. Hold elective admits. Notify house supervisor."))
             .when(F.col("alert_type") == "OR_DELAY",
                   F.lit("Audit turnover bottleneck. Review case-cart readiness. Notify perioperative director."))
             .otherwise(F.lit("Capacity review required."))
        )
        .withColumn("metric_name", F.col("capacity_type"))
        .withColumn("metric_value", F.col("current_value"))
        .select(
            "alert_id", "alert_timestamp", "alert_type", "facility_id",
            F.coalesce(F.col("ward"), F.lit("")).alias("ward"),
            "severity", "alert_text", "recommended_action",
            "metric_name", "metric_value", "threshold_value",
            F.coalesce(F.col("latitude"), F.lit(0.0)).alias("latitude"),
            F.coalesce(F.col("longitude"), F.lit(0.0)).alias("longitude"),
        )
    )
    print(f"  Capacity alerts: {df_cap_alerts.count()}")

# ============================================================================
# COO ALERT 4: STAFFING_GAP
# ============================================================================
_cols, _rows = _kql_query("""
    staffing_acuity_events
    | where event_timestamp > ago(30m)
    | summarize arg_max(event_timestamp, *) by facility_id, ward
""")
print(f"  staffing_acuity_events (latest per ward, 30m): {len(_rows)}")

df_staff_alerts = None
if _rows:
    df_st = spark.createDataFrame([dict(zip(_cols, r)) for r in _rows])
    df_st = df_st.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))

    df_st = df_st.withColumn(
        "severity",
        F.when(F.col("staffing_gap") >= F.lit(_staff_crit), F.lit("CRITICAL"))
         .when(F.col("staffing_gap") >= F.lit(_staff_warn), F.lit("HIGH"))
         .otherwise(F.lit("OK"))
    )
    df_staff_alerts_src = df_st.filter(F.col("severity") != "OK")

    df_staff_alerts = (df_staff_alerts_src
        .withColumn("alert_id", F.expr("uuid()"))
        .withColumn("alert_timestamp", F.current_timestamp())
        .withColumn("alert_type", F.lit("STAFFING_GAP"))
        .withColumn("alert_text", F.concat(
            F.lit("STAFFING GAP: "), F.col("ward"),
            F.lit(" — nurses="), F.col("nurse_count").cast("string"),
            F.lit(", patients="), F.col("patient_count").cast("string"),
            F.lit(", acuity="), F.round(F.col("acuity_score"), 2).cast("string"),
            F.lit(", ratio="), F.round(F.col("ratio_actual"), 2).cast("string"),
            F.lit(" vs target "), F.round(F.col("ratio_target"), 2).cast("string"),
            F.lit(" (gap="), F.col("staffing_gap").cast("string"), F.lit(").")
        ))
        .withColumn("recommended_action",
            F.when(F.col("severity") == "CRITICAL",
                   F.lit("Activate float pool. Page nursing supervisor. Hold transfers in. Consider agency."))
             .otherwise(F.lit("Reassign acuity. Notify charge nurse. Add to next-shift handoff."))
        )
        .withColumn("metric_name", F.lit("staffing_gap"))
        .withColumn("metric_value", F.col("staffing_gap").cast("double"))
        .withColumn("threshold_value", F.lit(float(_staff_warn)))
        .select(
            "alert_id", "alert_timestamp", "alert_type", "facility_id",
            "ward", "severity", "alert_text", "recommended_action",
            "metric_name", "metric_value", "threshold_value",
            F.lit(0.0).alias("latitude"), F.lit(0.0).alias("longitude"),
        )
    )
    print(f"  Staffing alerts: {df_staff_alerts.count()}")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Union + write to Delta + KQL ----------
df_all = None
for _df in [df_cap_alerts, df_staff_alerts]:
    if _df is not None and _df.count() > 0:
        df_all = _df if df_all is None else df_all.unionByName(_df)

if df_all is not None and df_all.count() > 0:
    df_all.write.format("delta").mode("overwrite").saveAsTable("lh_gold_curated.rti_coo_alerts")
    print(f"\n  rti_coo_alerts (Delta): {df_all.count()} rows written")

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
                database=_KQL_DB_NAME, table="coo_alerts",
                data_format=DataFormat.JSON,
                ingestion_mapping_reference="coo_alerts_mapping"
            )
            _json_data = _df_kql.to_json(orient="records", lines=True, date_format="iso")
            _client.ingest_from_stream(_io.StringIO(_json_data), ingestion_properties=_props)
            print(f"  KQL: {len(_df_kql)} COO alerts streamed → coo_alerts")
        except Exception as _e:
            print(f"  KQL WARN: coo_alerts ingestion failed: {_e}")
else:
    print("\n  No COO alerts generated this run.")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

print("\n" + "=" * 60)
print("COO OPERATIONAL ALERTS SUMMARY")
print("=" * 60)
print(f"  Capacity alerts (ED/BED/OR): {df_cap_alerts.count() if df_cap_alerts else 0}")
print(f"  Staffing gap alerts:         {df_staff_alerts.count() if df_staff_alerts else 0}")
print()
print("Downstream consumers (configure in Fabric Activator):")
print("  • ED_BOARDING_BREACH (CRITICAL)    → Teams card + page bed coordinator")
print("  • BED_CAPACITY_CRITICAL (CRITICAL) → Power Automate → House supervisor SMS")
print("  • OR_DELAY (HIGH)                  → Teams adaptive card → Periop director")
print("  • STAFFING_GAP (CRITICAL)          → Power Automate → Nursing supervisor + float pool")
print("=" * 60)
print("NB_RTI_COO_Alerts: COMPLETE")
