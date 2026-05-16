# Fabric notebook source

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# # RTI Post-Deploy Setup
# 
# **Prerequisites:** The Eventhouse and KQL Database must already be deployed as Git artifacts
# (via `fabric-launcher` or `fabric-cicd`). This notebook performs post-deploy wiring:
# 
# 1. **Discover** Eventhouse + KQL Database by display name (zero hardcoded IDs)
# 2. **Execute** schema -- creates 7 tables + streaming ingestion policies + JSON mappings
# 3. **Discover** Kusto ingestion URI for downstream notebooks
# 
# > **Why direct Kusto ingestion?** This demo deploys everything programmatically
# > (zero portal clicks). Eventstream Custom Endpoints and their wiring to KQL destinations
# > cannot be fully configured via public API today, which would require manual portal steps.
# > Direct Kusto ingestion (`azure-kusto-ingest`) achieves the same streaming result with
# > zero user configuration.
# >
# > **In production**, you would typically ingest via:
# > - **Eventstream Custom Endpoint** -- for application-generated events
# > - **IoT Hub / IoT Central** -- for medical device telemetry (vitals, wearables)
# > - **Azure Event Hub** -- for high-throughput enterprise event buses
# > - **Change Data Capture** -- for database-sourced real-time feeds
# >
# > All of these route through Eventstream into the same KQL tables created here.
# 
# **Default lakehouse:** `lh_gold_curated`

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# NB_RTI_Setup_Eventhouse -- Post-Deploy Wiring
# ============================================================================
# The Eventhouse + KQL Database are deployed as Git artifacts by the launcher.
# This notebook discovers them, executes the schema, and outputs the Kusto
# ingestion URI for direct ingestion (no Eventstream needed).
#
# Zero hardcoded IDs -- everything resolved by displayName lookup.
# Default lakehouse: lh_gold_curated
# ============================================================================

print("NB_RTI_Setup_Eventhouse: Starting post-deploy setup...")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Parameters ----------
WORKSPACE_ID = ""                               # Auto-detected if blank
EVENTHOUSE_NAME = "Healthcare_RTI_Eventhouse"    # Must match Git artifact displayName
KQL_DB_NAME = "Healthcare_RTI_DB"                # Must match Git artifact displayName

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

import requests
import json
import time

def get_fabric_token():
    """Get Fabric API token from notebook context."""
    return notebookutils.credentials.getToken("https://analysis.windows.net/powerbi/api")

def get_kusto_token():
    """Get Kusto/ADX token for direct ingestion."""
    return notebookutils.credentials.getToken("kusto")

def get_headers():
    return {
        "Authorization": f"Bearer {get_fabric_token()}",
        "Content-Type": "application/json"
    }

BASE_URL = "https://api.fabric.microsoft.com/v1"

if not WORKSPACE_ID:
    WORKSPACE_ID = notebookutils.runtime.context.get("currentWorkspaceId", "")
    if not WORKSPACE_ID:
        raise ValueError("WORKSPACE_ID must be set -- could not auto-detect")

print(f"Workspace ID: {WORKSPACE_ID}")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Step 1: Discover Eventhouse + KQL Database by displayName
# ============================================================================
print("Step 1: Discovering deployed RTI artifacts...")

def find_item(item_type, display_name):
    """Find a workspace item by type and displayName. Returns item dict or None."""
    url = f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items?type={item_type}"
    resp = requests.get(url, headers=get_headers())
    resp.raise_for_status()
    for item in resp.json().get("value", []):
        if item["displayName"] == display_name:
            return item
    return None

# Discover Eventhouse
eventhouse = find_item("Eventhouse", EVENTHOUSE_NAME)
if not eventhouse:
    raise RuntimeError(
        f"Eventhouse '{EVENTHOUSE_NAME}' not found in workspace.\n"
        f"Ensure Healthcare_Launcher.ipynb deployed the Eventhouse artifact first.\n"
        f"Check the workspace for the item or re-run the launcher."
    )
eventhouse_id = eventhouse["id"]
print(f"  Eventhouse: {EVENTHOUSE_NAME} ({eventhouse_id})")

# Discover KQL Database
kql_db = find_item("KQLDatabase", KQL_DB_NAME)
if not kql_db:
    kql_db = find_item("KQLDatabase", EVENTHOUSE_NAME)
    if kql_db:
        KQL_DB_NAME = EVENTHOUSE_NAME
        print(f"  KQL Database found with Eventhouse name: {KQL_DB_NAME}")

if not kql_db:
    raise RuntimeError(
        f"KQL Database '{KQL_DB_NAME}' not found in workspace.\n"
        f"The KQL Database should be deployed as a Git artifact alongside the Eventhouse."
    )
kql_db_id = kql_db["id"]
print(f"  KQL Database: {KQL_DB_NAME} ({kql_db_id})")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Step 2: Discover Kusto ingestion URI
# ============================================================================
print("Step 2: Discovering Kusto cluster URI...")

# Eventhouse properties contain the query and ingestion URIs
props_url = f"{BASE_URL}/workspaces/{WORKSPACE_ID}/eventhouses/{eventhouse_id}"
resp = requests.get(props_url, headers=get_headers())

KUSTO_QUERY_URI = ""
KUSTO_INGEST_URI = ""

if resp.status_code == 200:
    props = resp.json().get("properties", resp.json())
    KUSTO_QUERY_URI = props.get("queryServiceUri", "")
    KUSTO_INGEST_URI = props.get("ingestionServiceUri", "")
    if not KUSTO_INGEST_URI and KUSTO_QUERY_URI:
        # Derive ingestion URI from query URI
        KUSTO_INGEST_URI = KUSTO_QUERY_URI.replace("https://", "https://ingest-")

if not KUSTO_QUERY_URI:
    # Fallback: discover via KQL management command
    cmd_url = f"{BASE_URL}/workspaces/{WORKSPACE_ID}/kqlDatabases/{kql_db_id}/runCommand"
    cmd_resp = requests.post(cmd_url, headers=get_headers(),
                             json={"script": ".show cluster"})
    if cmd_resp.status_code == 200:
        try:
            rows = cmd_resp.json().get("results", [{}])[0].get("rows", [])
            if rows:
                # First column is typically the cluster URI
                KUSTO_QUERY_URI = rows[0][0] if isinstance(rows[0], list) else str(rows[0])
                KUSTO_INGEST_URI = KUSTO_QUERY_URI.replace("https://", "https://ingest-")
        except Exception as e:
            print(f"  WARN: Could not parse cluster info: {e}")

if KUSTO_QUERY_URI:
    print(f"  Query URI:     {KUSTO_QUERY_URI}")
    print(f"  Ingestion URI: {KUSTO_INGEST_URI}")
else:
    print("  WARN: Could not discover Kusto URIs -- Kusto ingestion will be skipped")
    print("  KQL tables will still be created for manual use")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Step 3: Execute KQL schema -- create tables + streaming policies + mappings
# ============================================================================
print("Step 3: Executing KQL schema commands...")

KQL_COMMANDS = [
    # --- LANDING TABLE (all events from Eventstream land here) ---
    # The Eventstream writes ALL event types into this single table.
    # KQL update policies (defined below) automatically route rows
    # into the typed per-event tables based on the _table field.
    #
    # IMPORTANT: event_timestamp is STRING (not datetime) because Eventstream
    # ingests the ISO-format timestamp as a string. The Extract functions
    # convert it to datetime for the typed tables. quantity/days_supply are
    # LONG because that's what Eventstream infers from JSON integers.
    # If we use different types here (e.g. datetime, int), .create-merge will
    # FAIL when the table already exists (from Eventstream auto-creation),
    # and ALL new columns (claim_id, facility_id, etc.) will NOT be added.
    """.create-merge table rti_all_events (
        event_id: string, event_timestamp: string, event_type: string,
        _table: string,
        claim_id: string, patient_id: string, provider_id: string,
        facility_id: string, facility_name: string, payer_id: string,
        diagnosis_code: string, procedure_code: string,
        claim_type: string, claim_amount: real,
        admission_type: string, primary_diagnosis: string,
        medication_code: string, medication_name: string, drug_class: string,
        quantity: long, days_supply: long,
        latitude: real, longitude: real,
        injected_fraud_flags: string,
        has_open_care_gaps: bool, open_gap_measures: string,
        current_encounter_id: string, prior_discharge_date: string,
        days_since_discharge: long, current_diagnosis: string,
        prior_diagnosis: string, readmission_risk_score: real, drg_code: string
    )""",
    # DEFENSIVE: If .create-merge failed due to any residual type conflict,
    # this .alter-merge adds ONLY the string/real/bool columns that Eventstream
    # definitely didn't create (claims/ADT/readmission-specific columns). These
    # types cannot conflict because these columns won't exist yet.
    """.alter-merge table rti_all_events (
        claim_id: string, facility_id: string, facility_name: string,
        payer_id: string, diagnosis_code: string, procedure_code: string,
        claim_type: string, claim_amount: real,
        admission_type: string, primary_diagnosis: string,
        has_open_care_gaps: bool, open_gap_measures: string,
        injected_fraud_flags: string, _table: string,
        current_encounter_id: string, prior_discharge_date: string,
        current_diagnosis: string, prior_diagnosis: string,
        readmission_risk_score: real, drg_code: string
    )""",
    # --- INPUT TABLES ---
    """.create-merge table claims_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        claim_id: string, patient_id: string, provider_id: string,
        facility_id: string, payer_id: string, diagnosis_code: string,
        procedure_code: string, claim_type: string, claim_amount: real,
        latitude: real, longitude: real, injected_fraud_flags: string
    )""",
    """.create-merge table adt_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        patient_id: string, facility_id: string, facility_name: string,
        admission_type: string, primary_diagnosis: string,
        latitude: real, longitude: real,
        has_open_care_gaps: bool, open_gap_measures: string
    )""",
    """.create-merge table rx_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        patient_id: string, provider_id: string, medication_code: string,
        medication_name: string, drug_class: string,
        quantity: int, days_supply: int, latitude: real, longitude: real
    )""",
    # --- OUTPUT / SCORED TABLES ---
    """.create-merge table fraud_scores (
        score_id: string, score_timestamp: datetime, claim_id: string,
        patient_id: string, provider_id: string, facility_id: string,
        claim_amount: real, fraud_score: real, fraud_flags: string,
        risk_tier: string, latitude: real, longitude: real
    )""",
    """.create-merge table care_gap_alerts (
        alert_id: string, alert_timestamp: datetime, patient_id: string,
        facility_id: string, facility_name: string, measure_id: string,
        measure_name: string, gap_days_overdue: int, alert_priority: string,
        alert_text: string, latitude: real, longitude: real
    )""",
    """.create-merge table highcost_alerts (
        alert_id: string, alert_timestamp: datetime, patient_id: string,
        rolling_spend_30d: real, rolling_spend_90d: real, ed_visits_30d: int,
        readmission_flag: bool, risk_tier: string, cost_trend: string,
        latitude: real, longitude: real
    )""",
    # Readmission events — emitted directly by NB_RTI_Event_Simulator with
    # pre-computed readmission_risk_score (0-100). No separate scoring
    # notebook needed; the source IS the score (mirrors how production
    # EHR ADT feeds would arrive with risk already computed upstream).
    """.create-merge table readmission_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        patient_id: string, provider_id: string, facility_id: string,
        current_encounter_id: string, prior_discharge_date: datetime,
        days_since_discharge: int, current_diagnosis: string,
        prior_diagnosis: string, readmission_risk_score: real,
        drg_code: string, latitude: real, longitude: real
    )""",
    # --- STREAMING INGESTION POLICIES (landing + 7 typed tables) ---
    ".alter table rti_all_events policy streamingingestion enable",
    ".alter table claims_events policy streamingingestion enable",
    ".alter table adt_events policy streamingingestion enable",
    ".alter table rx_events policy streamingingestion enable",
    ".alter table fraud_scores policy streamingingestion enable",
    ".alter table care_gap_alerts policy streamingingestion enable",
    ".alter table highcost_alerts policy streamingingestion enable",
    ".alter table readmission_events policy streamingingestion enable",
    # --- UPDATE POLICIES (auto-route from rti_all_events → typed tables) ---
    # These server-side policies fire on every ingestion batch into rti_all_events,
    # extracting rows by _table field and appending them to the target tables.
    # Note: todatetime() is needed because event_timestamp arrives as string from Eventstream.
    # toint()/coalesce() handle columns that may be null in the landing table.
    # coalesce() on claim-specific fields handles the case where the Eventstream
    # created rti_all_events before these columns were added.
    """.create-or-alter function ExtractClaimsEvents() {
        rti_all_events
        | where _table == "claims_events"
        | project event_id,
                  event_timestamp = todatetime(event_timestamp),
                  event_type,
                  claim_id = coalesce(claim_id, ""),
                  patient_id,
                  provider_id = coalesce(provider_id, ""),
                  facility_id = coalesce(facility_id, ""),
                  payer_id = coalesce(payer_id, ""),
                  diagnosis_code = coalesce(diagnosis_code, ""),
                  procedure_code = coalesce(procedure_code, ""),
                  claim_type = coalesce(claim_type, ""),
                  claim_amount = coalesce(claim_amount, 0.0),
                  latitude,
                  longitude,
                  injected_fraud_flags = coalesce(injected_fraud_flags, "")
    }""",
    """.create-or-alter function ExtractAdtEvents() {
        rti_all_events
        | where _table == "adt_events"
        | project event_id,
                  event_timestamp = todatetime(event_timestamp),
                  event_type, patient_id,
                  facility_id = coalesce(facility_id, ""),
                  facility_name = coalesce(facility_name, ""),
                  admission_type = coalesce(admission_type, ""),
                  primary_diagnosis = coalesce(primary_diagnosis, ""),
                  latitude, longitude,
                  has_open_care_gaps = coalesce(has_open_care_gaps, false),
                  open_gap_measures = coalesce(open_gap_measures, "")
    }""",
    """.create-or-alter function ExtractRxEvents() {
        rti_all_events
        | where _table == "rx_events"
        | project event_id,
                  event_timestamp = todatetime(event_timestamp),
                  event_type, patient_id,
                  provider_id = coalesce(provider_id, ""),
                  medication_code = coalesce(medication_code, ""),
                  medication_name = coalesce(medication_name, ""),
                  drug_class = coalesce(drug_class, ""),
                  quantity = toint(coalesce(quantity, 0)),
                  days_supply = toint(coalesce(days_supply, 0)),
                  latitude, longitude
    }""",
    """.create-or-alter function ExtractReadmissionEvents() {
        rti_all_events
        | where _table == "readmission_events"
        | project event_id,
                  event_timestamp = todatetime(event_timestamp),
                  event_type, patient_id,
                  provider_id = coalesce(provider_id, ""),
                  facility_id = coalesce(facility_id, ""),
                  current_encounter_id = coalesce(current_encounter_id, ""),
                  prior_discharge_date = todatetime(prior_discharge_date),
                  days_since_discharge = toint(coalesce(days_since_discharge, 0)),
                  current_diagnosis = coalesce(current_diagnosis, ""),
                  prior_diagnosis = coalesce(prior_diagnosis, ""),
                  readmission_risk_score = coalesce(readmission_risk_score, 0.0),
                  drg_code = coalesce(drg_code, ""),
                  latitude, longitude
    }""",
    ".alter table claims_events policy update @'[{\"IsEnabled\": true, \"Source\": \"rti_all_events\", \"Query\": \"ExtractClaimsEvents()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": true}]'",
    ".alter table adt_events policy update @'[{\"IsEnabled\": true, \"Source\": \"rti_all_events\", \"Query\": \"ExtractAdtEvents()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": true}]'",
    ".alter table rx_events policy update @'[{\"IsEnabled\": true, \"Source\": \"rti_all_events\", \"Query\": \"ExtractRxEvents()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": true}]'",
    ".alter table readmission_events policy update @'[{\"IsEnabled\": true, \"Source\": \"rti_all_events\", \"Query\": \"ExtractReadmissionEvents()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": true}]'",
    # --- ONELAKE MIRRORING POLICIES (5-minute flush for demos) ---
    # OneLake Availability must be enabled on the KQL DB first (portal toggle).
    # These policies set the delta-table flush to 5 minutes instead of the
    # default 3 hours, so scoring notebooks see data quickly after streaming.
    ".alter-merge table claims_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table adt_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table rx_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table readmission_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    # --- JSON INGESTION MAPPINGS ---
    """.create-or-alter table rti_all_events ingestion json mapping 'rti_all_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"string"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"_table","path":"$._table","datatype":"string"},{"column":"claim_id","path":"$.claim_id","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"provider_id","path":"$.provider_id","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"},{"column":"facility_name","path":"$.facility_name","datatype":"string"},{"column":"payer_id","path":"$.payer_id","datatype":"string"},{"column":"diagnosis_code","path":"$.diagnosis_code","datatype":"string"},{"column":"procedure_code","path":"$.procedure_code","datatype":"string"},{"column":"claim_type","path":"$.claim_type","datatype":"string"},{"column":"claim_amount","path":"$.claim_amount","datatype":"real"},{"column":"admission_type","path":"$.admission_type","datatype":"string"},{"column":"primary_diagnosis","path":"$.primary_diagnosis","datatype":"string"},{"column":"medication_code","path":"$.medication_code","datatype":"string"},{"column":"medication_name","path":"$.medication_name","datatype":"string"},{"column":"drug_class","path":"$.drug_class","datatype":"string"},{"column":"quantity","path":"$.quantity","datatype":"long"},{"column":"days_supply","path":"$.days_supply","datatype":"long"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"},{"column":"injected_fraud_flags","path":"$.injected_fraud_flags","datatype":"string"},{"column":"has_open_care_gaps","path":"$.has_open_care_gaps","datatype":"bool"},{"column":"open_gap_measures","path":"$.open_gap_measures","datatype":"string"},{"column":"current_encounter_id","path":"$.current_encounter_id","datatype":"string"},{"column":"prior_discharge_date","path":"$.prior_discharge_date","datatype":"string"},{"column":"days_since_discharge","path":"$.days_since_discharge","datatype":"long"},{"column":"current_diagnosis","path":"$.current_diagnosis","datatype":"string"},{"column":"prior_diagnosis","path":"$.prior_diagnosis","datatype":"string"},{"column":"readmission_risk_score","path":"$.readmission_risk_score","datatype":"real"},{"column":"drg_code","path":"$.drg_code","datatype":"string"}]'""",
    """.create-or-alter table claims_events ingestion json mapping 'claims_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"claim_id","path":"$.claim_id","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"provider_id","path":"$.provider_id","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"},{"column":"payer_id","path":"$.payer_id","datatype":"string"},{"column":"diagnosis_code","path":"$.diagnosis_code","datatype":"string"},{"column":"procedure_code","path":"$.procedure_code","datatype":"string"},{"column":"claim_type","path":"$.claim_type","datatype":"string"},{"column":"claim_amount","path":"$.claim_amount","datatype":"real"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"},{"column":"injected_fraud_flags","path":"$.injected_fraud_flags","datatype":"string"}]'""",
    """.create-or-alter table adt_events ingestion json mapping 'adt_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"},{"column":"facility_name","path":"$.facility_name","datatype":"string"},{"column":"admission_type","path":"$.admission_type","datatype":"string"},{"column":"primary_diagnosis","path":"$.primary_diagnosis","datatype":"string"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"},{"column":"has_open_care_gaps","path":"$.has_open_care_gaps","datatype":"bool"},{"column":"open_gap_measures","path":"$.open_gap_measures","datatype":"string"}]'""",
    """.create-or-alter table rx_events ingestion json mapping 'rx_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"provider_id","path":"$.provider_id","datatype":"string"},{"column":"medication_code","path":"$.medication_code","datatype":"string"},{"column":"medication_name","path":"$.medication_name","datatype":"string"},{"column":"drug_class","path":"$.drug_class","datatype":"string"},{"column":"quantity","path":"$.quantity","datatype":"int"},{"column":"days_supply","path":"$.days_supply","datatype":"int"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"}]'""",
    """.create-or-alter table fraud_scores ingestion json mapping 'fraud_scores_mapping'
    '[{"column":"score_id","path":"$.score_id","datatype":"string"},{"column":"score_timestamp","path":"$.score_timestamp","datatype":"datetime"},{"column":"claim_id","path":"$.claim_id","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"provider_id","path":"$.provider_id","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"},{"column":"claim_amount","path":"$.claim_amount","datatype":"real"},{"column":"fraud_score","path":"$.fraud_score","datatype":"real"},{"column":"fraud_flags","path":"$.fraud_flags","datatype":"string"},{"column":"risk_tier","path":"$.risk_tier","datatype":"string"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"}]'""",
    """.create-or-alter table care_gap_alerts ingestion json mapping 'care_gap_alerts_mapping'
    '[{"column":"alert_id","path":"$.alert_id","datatype":"string"},{"column":"alert_timestamp","path":"$.alert_timestamp","datatype":"datetime"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"},{"column":"facility_name","path":"$.facility_name","datatype":"string"},{"column":"measure_id","path":"$.measure_id","datatype":"string"},{"column":"measure_name","path":"$.measure_name","datatype":"string"},{"column":"gap_days_overdue","path":"$.gap_days_overdue","datatype":"int"},{"column":"alert_priority","path":"$.alert_priority","datatype":"string"},{"column":"alert_text","path":"$.alert_text","datatype":"string"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"}]'""",
    """.create-or-alter table highcost_alerts ingestion json mapping 'highcost_alerts_mapping'
    '[{"column":"alert_id","path":"$.alert_id","datatype":"string"},{"column":"alert_timestamp","path":"$.alert_timestamp","datatype":"datetime"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"rolling_spend_30d","path":"$.rolling_spend_30d","datatype":"real"},{"column":"rolling_spend_90d","path":"$.rolling_spend_90d","datatype":"real"},{"column":"ed_visits_30d","path":"$.ed_visits_30d","datatype":"int"},{"column":"readmission_flag","path":"$.readmission_flag","datatype":"bool"},{"column":"risk_tier","path":"$.risk_tier","datatype":"string"},{"column":"cost_trend","path":"$.cost_trend","datatype":"string"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"}]'""",
    """.create-or-alter table readmission_events ingestion json mapping 'readmission_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"provider_id","path":"$.provider_id","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"},{"column":"current_encounter_id","path":"$.current_encounter_id","datatype":"string"},{"column":"prior_discharge_date","path":"$.prior_discharge_date","datatype":"datetime"},{"column":"days_since_discharge","path":"$.days_since_discharge","datatype":"int"},{"column":"current_diagnosis","path":"$.current_diagnosis","datatype":"string"},{"column":"prior_diagnosis","path":"$.prior_diagnosis","datatype":"string"},{"column":"readmission_risk_score","path":"$.readmission_risk_score","datatype":"real"},{"column":"drg_code","path":"$.drg_code","datatype":"string"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"}]'""",

    # ========================================================================
    # PROVIDER + PAYER RTI EXTENSION (5 new typed event tables + alert outputs)
    # ========================================================================
    # Add new fields to landing rti_all_events table for new event types.
    # These are additive .alter-merge calls (won't conflict with existing cols).
    """.alter-merge table rti_all_events (
        current_encounter_id: string, prior_discharge_date: string,
        days_since_discharge: long, current_diagnosis: string,
        prior_diagnosis: string, readmission_risk_score: real, drg_code: string,
        provider_name: string, specialty: string, metric_name: string,
        metric_value: real, benchmark_value: real, direction: string,
        rolling_window_days: long, denominator: long, numerator: long,
        payer_name: string, adjudication_result: string, denial_reason: string,
        billed_amount: real, allowed_amount: real, paid_amount: real,
        is_preventable: bool, recommended_action: string,
        open_claims_count: long, total_ar_balance: real, avg_days_in_ar: real,
        claims_over_45_days: long, claims_over_90_days: long,
        net_collection_rate: real, prior_period_avg_days: real,
        trend_direction: string,
        auth_id: string, procedure_description: string, auth_status: string,
        auth_expiry_date: string, days_until_expiry: long,
        scheduled_procedure_date: string, estimated_charge: real
    )""",

    # --- PROVIDER: readmission_events (real-time 30-day readmit detection) ---
    """.create-merge table readmission_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        patient_id: string, provider_id: string, facility_id: string,
        current_encounter_id: string, prior_discharge_date: string,
        days_since_discharge: int, current_diagnosis: string,
        prior_diagnosis: string, readmission_risk_score: real,
        drg_code: string, latitude: real, longitude: real
    )""",

    # --- PROVIDER: quality_metric_events (rolling provider KPI snapshots) ---
    """.create-merge table quality_metric_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        provider_id: string, provider_name: string, specialty: string,
        facility_id: string, metric_name: string, metric_value: real,
        benchmark_value: real, direction: string, rolling_window_days: int,
        denominator: int, numerator: int
    )""",

    # --- PAYER: denial_adjudication_events (real-time approve/deny decisions) ---
    """.create-merge table denial_adjudication_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        claim_id: string, patient_id: string, provider_id: string,
        payer_id: string, payer_name: string, adjudication_result: string,
        denial_reason: string, billed_amount: real, allowed_amount: real,
        paid_amount: real, is_preventable: bool, recommended_action: string
    )""",

    # --- PAYER: ar_snapshot_events (periodic AR aging checkpoint per payer) ---
    """.create-merge table ar_snapshot_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        payer_id: string, payer_name: string, open_claims_count: int,
        total_ar_balance: real, avg_days_in_ar: real,
        claims_over_45_days: int, claims_over_90_days: int,
        net_collection_rate: real, prior_period_avg_days: real,
        trend_direction: string
    )""",

    # --- PAYER: auth_lifecycle_events (prior auth submit/approve/expire) ---
    """.create-merge table auth_lifecycle_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        auth_id: string, patient_id: string, provider_id: string,
        payer_id: string, payer_name: string, procedure_code: string,
        procedure_description: string, auth_status: string,
        auth_expiry_date: string, days_until_expiry: int,
        scheduled_procedure_date: string, estimated_charge: real
    )""",

    # --- ALERT OUTPUT TABLES (written by Provider/Payer scoring notebooks) ---
    """.create-merge table provider_alerts (
        alert_id: string, alert_timestamp: datetime, alert_type: string,
        provider_id: string, provider_name: string, patient_id: string,
        facility_id: string, severity: string, alert_text: string,
        recommended_action: string, metric_name: string,
        metric_value: real, benchmark_value: real,
        latitude: real, longitude: real
    )""",
    """.create-merge table payer_alerts (
        alert_id: string, alert_timestamp: datetime, alert_type: string,
        payer_id: string, payer_name: string, claim_id: string,
        patient_id: string, severity: string, alert_text: string,
        recommended_action: string, dollar_impact: real,
        denial_reason: string, days_until_expiry: int
    )""",

    # --- STREAMING POLICIES for new tables ---
    ".alter table readmission_events policy streamingingestion enable",
    ".alter table quality_metric_events policy streamingingestion enable",
    ".alter table denial_adjudication_events policy streamingingestion enable",
    ".alter table ar_snapshot_events policy streamingingestion enable",
    ".alter table auth_lifecycle_events policy streamingingestion enable",
    ".alter table provider_alerts policy streamingingestion enable",
    ".alter table payer_alerts policy streamingingestion enable",

    # --- EXTRACT FUNCTIONS (route from rti_all_events landing → typed tables) ---
    """.create-or-alter function ExtractReadmissionEvents() {
        rti_all_events
        | where _table == "readmission_events"
        | project event_id, event_timestamp = todatetime(event_timestamp),
                  event_type, patient_id,
                  provider_id = coalesce(provider_id, ""),
                  facility_id = coalesce(facility_id, ""),
                  current_encounter_id = coalesce(current_encounter_id, ""),
                  prior_discharge_date = coalesce(prior_discharge_date, ""),
                  days_since_discharge = toint(coalesce(days_since_discharge, 0)),
                  current_diagnosis = coalesce(current_diagnosis, ""),
                  prior_diagnosis = coalesce(prior_diagnosis, ""),
                  readmission_risk_score = coalesce(readmission_risk_score, 0.0),
                  drg_code = coalesce(drg_code, ""),
                  latitude, longitude
    }""",
    """.create-or-alter function ExtractQualityMetricEvents() {
        rti_all_events
        | where _table == "quality_metric_events"
        | project event_id, event_timestamp = todatetime(event_timestamp),
                  event_type,
                  provider_id = coalesce(provider_id, ""),
                  provider_name = coalesce(provider_name, ""),
                  specialty = coalesce(specialty, ""),
                  facility_id = coalesce(facility_id, ""),
                  metric_name = coalesce(metric_name, ""),
                  metric_value = coalesce(metric_value, 0.0),
                  benchmark_value = coalesce(benchmark_value, 0.0),
                  direction = coalesce(direction, ""),
                  rolling_window_days = toint(coalesce(rolling_window_days, 0)),
                  denominator = toint(coalesce(denominator, 0)),
                  numerator = toint(coalesce(numerator, 0))
    }""",
    """.create-or-alter function ExtractDenialAdjudicationEvents() {
        rti_all_events
        | where _table == "denial_adjudication_events"
        | project event_id, event_timestamp = todatetime(event_timestamp),
                  event_type,
                  claim_id = coalesce(claim_id, ""), patient_id,
                  provider_id = coalesce(provider_id, ""),
                  payer_id = coalesce(payer_id, ""),
                  payer_name = coalesce(payer_name, ""),
                  adjudication_result = coalesce(adjudication_result, ""),
                  denial_reason = coalesce(denial_reason, ""),
                  billed_amount = coalesce(billed_amount, 0.0),
                  allowed_amount = coalesce(allowed_amount, 0.0),
                  paid_amount = coalesce(paid_amount, 0.0),
                  is_preventable = coalesce(is_preventable, false),
                  recommended_action = coalesce(recommended_action, "")
    }""",
    """.create-or-alter function ExtractArSnapshotEvents() {
        rti_all_events
        | where _table == "ar_snapshot_events"
        | project event_id, event_timestamp = todatetime(event_timestamp),
                  event_type,
                  payer_id = coalesce(payer_id, ""),
                  payer_name = coalesce(payer_name, ""),
                  open_claims_count = toint(coalesce(open_claims_count, 0)),
                  total_ar_balance = coalesce(total_ar_balance, 0.0),
                  avg_days_in_ar = coalesce(avg_days_in_ar, 0.0),
                  claims_over_45_days = toint(coalesce(claims_over_45_days, 0)),
                  claims_over_90_days = toint(coalesce(claims_over_90_days, 0)),
                  net_collection_rate = coalesce(net_collection_rate, 0.0),
                  prior_period_avg_days = coalesce(prior_period_avg_days, 0.0),
                  trend_direction = coalesce(trend_direction, "")
    }""",
    """.create-or-alter function ExtractAuthLifecycleEvents() {
        rti_all_events
        | where _table == "auth_lifecycle_events"
        | project event_id, event_timestamp = todatetime(event_timestamp),
                  event_type,
                  auth_id = coalesce(auth_id, ""), patient_id,
                  provider_id = coalesce(provider_id, ""),
                  payer_id = coalesce(payer_id, ""),
                  payer_name = coalesce(payer_name, ""),
                  procedure_code = coalesce(procedure_code, ""),
                  procedure_description = coalesce(procedure_description, ""),
                  auth_status = coalesce(auth_status, ""),
                  auth_expiry_date = coalesce(auth_expiry_date, ""),
                  days_until_expiry = toint(coalesce(days_until_expiry, 0)),
                  scheduled_procedure_date = coalesce(scheduled_procedure_date, ""),
                  estimated_charge = coalesce(estimated_charge, 0.0)
    }""",

    # --- UPDATE POLICIES for new tables ---
    ".alter table readmission_events policy update @'[{\"IsEnabled\": true, \"Source\": \"rti_all_events\", \"Query\": \"ExtractReadmissionEvents()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": true}]'",
    ".alter table quality_metric_events policy update @'[{\"IsEnabled\": true, \"Source\": \"rti_all_events\", \"Query\": \"ExtractQualityMetricEvents()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": true}]'",
    ".alter table denial_adjudication_events policy update @'[{\"IsEnabled\": true, \"Source\": \"rti_all_events\", \"Query\": \"ExtractDenialAdjudicationEvents()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": true}]'",
    ".alter table ar_snapshot_events policy update @'[{\"IsEnabled\": true, \"Source\": \"rti_all_events\", \"Query\": \"ExtractArSnapshotEvents()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": true}]'",
    ".alter table auth_lifecycle_events policy update @'[{\"IsEnabled\": true, \"Source\": \"rti_all_events\", \"Query\": \"ExtractAuthLifecycleEvents()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": true}]'",

    # --- ONELAKE MIRRORING (5-min flush so Spark notebooks can query fast) ---
    ".alter-merge table readmission_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table quality_metric_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table denial_adjudication_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table ar_snapshot_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table auth_lifecycle_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table provider_alerts policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table payer_alerts policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",

    # --- INGESTION MAPPINGS for typed tables (used when Eventstream routes by _table) ---
    """.create-or-alter table readmission_events ingestion json mapping 'readmission_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"provider_id","path":"$.provider_id","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"},{"column":"current_encounter_id","path":"$.current_encounter_id","datatype":"string"},{"column":"prior_discharge_date","path":"$.prior_discharge_date","datatype":"string"},{"column":"days_since_discharge","path":"$.days_since_discharge","datatype":"int"},{"column":"current_diagnosis","path":"$.current_diagnosis","datatype":"string"},{"column":"prior_diagnosis","path":"$.prior_diagnosis","datatype":"string"},{"column":"readmission_risk_score","path":"$.readmission_risk_score","datatype":"real"},{"column":"drg_code","path":"$.drg_code","datatype":"string"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"}]'""",
    """.create-or-alter table quality_metric_events ingestion json mapping 'quality_metric_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"provider_id","path":"$.provider_id","datatype":"string"},{"column":"provider_name","path":"$.provider_name","datatype":"string"},{"column":"specialty","path":"$.specialty","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"},{"column":"metric_name","path":"$.metric_name","datatype":"string"},{"column":"metric_value","path":"$.metric_value","datatype":"real"},{"column":"benchmark_value","path":"$.benchmark_value","datatype":"real"},{"column":"direction","path":"$.direction","datatype":"string"},{"column":"rolling_window_days","path":"$.rolling_window_days","datatype":"int"},{"column":"denominator","path":"$.denominator","datatype":"int"},{"column":"numerator","path":"$.numerator","datatype":"int"}]'""",
    """.create-or-alter table denial_adjudication_events ingestion json mapping 'denial_adjudication_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"claim_id","path":"$.claim_id","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"provider_id","path":"$.provider_id","datatype":"string"},{"column":"payer_id","path":"$.payer_id","datatype":"string"},{"column":"payer_name","path":"$.payer_name","datatype":"string"},{"column":"adjudication_result","path":"$.adjudication_result","datatype":"string"},{"column":"denial_reason","path":"$.denial_reason","datatype":"string"},{"column":"billed_amount","path":"$.billed_amount","datatype":"real"},{"column":"allowed_amount","path":"$.allowed_amount","datatype":"real"},{"column":"paid_amount","path":"$.paid_amount","datatype":"real"},{"column":"is_preventable","path":"$.is_preventable","datatype":"bool"},{"column":"recommended_action","path":"$.recommended_action","datatype":"string"}]'""",
    """.create-or-alter table ar_snapshot_events ingestion json mapping 'ar_snapshot_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"payer_id","path":"$.payer_id","datatype":"string"},{"column":"payer_name","path":"$.payer_name","datatype":"string"},{"column":"open_claims_count","path":"$.open_claims_count","datatype":"int"},{"column":"total_ar_balance","path":"$.total_ar_balance","datatype":"real"},{"column":"avg_days_in_ar","path":"$.avg_days_in_ar","datatype":"real"},{"column":"claims_over_45_days","path":"$.claims_over_45_days","datatype":"int"},{"column":"claims_over_90_days","path":"$.claims_over_90_days","datatype":"int"},{"column":"net_collection_rate","path":"$.net_collection_rate","datatype":"real"},{"column":"prior_period_avg_days","path":"$.prior_period_avg_days","datatype":"real"},{"column":"trend_direction","path":"$.trend_direction","datatype":"string"}]'""",
    """.create-or-alter table auth_lifecycle_events ingestion json mapping 'auth_lifecycle_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"auth_id","path":"$.auth_id","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"provider_id","path":"$.provider_id","datatype":"string"},{"column":"payer_id","path":"$.payer_id","datatype":"string"},{"column":"payer_name","path":"$.payer_name","datatype":"string"},{"column":"procedure_code","path":"$.procedure_code","datatype":"string"},{"column":"procedure_description","path":"$.procedure_description","datatype":"string"},{"column":"auth_status","path":"$.auth_status","datatype":"string"},{"column":"auth_expiry_date","path":"$.auth_expiry_date","datatype":"string"},{"column":"days_until_expiry","path":"$.days_until_expiry","datatype":"int"},{"column":"scheduled_procedure_date","path":"$.scheduled_procedure_date","datatype":"string"},{"column":"estimated_charge","path":"$.estimated_charge","datatype":"real"}]'""",
    """.create-or-alter table provider_alerts ingestion json mapping 'provider_alerts_mapping'
    '[{"column":"alert_id","path":"$.alert_id","datatype":"string"},{"column":"alert_timestamp","path":"$.alert_timestamp","datatype":"datetime"},{"column":"alert_type","path":"$.alert_type","datatype":"string"},{"column":"provider_id","path":"$.provider_id","datatype":"string"},{"column":"provider_name","path":"$.provider_name","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"},{"column":"severity","path":"$.severity","datatype":"string"},{"column":"alert_text","path":"$.alert_text","datatype":"string"},{"column":"recommended_action","path":"$.recommended_action","datatype":"string"},{"column":"metric_name","path":"$.metric_name","datatype":"string"},{"column":"metric_value","path":"$.metric_value","datatype":"real"},{"column":"benchmark_value","path":"$.benchmark_value","datatype":"real"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"}]'""",
    """.create-or-alter table payer_alerts ingestion json mapping 'payer_alerts_mapping'
    '[{"column":"alert_id","path":"$.alert_id","datatype":"string"},{"column":"alert_timestamp","path":"$.alert_timestamp","datatype":"datetime"},{"column":"alert_type","path":"$.alert_type","datatype":"string"},{"column":"payer_id","path":"$.payer_id","datatype":"string"},{"column":"payer_name","path":"$.payer_name","datatype":"string"},{"column":"claim_id","path":"$.claim_id","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"severity","path":"$.severity","datatype":"string"},{"column":"alert_text","path":"$.alert_text","datatype":"string"},{"column":"recommended_action","path":"$.recommended_action","datatype":"string"},{"column":"dollar_impact","path":"$.dollar_impact","datatype":"real"},{"column":"denial_reason","path":"$.denial_reason","datatype":"string"},{"column":"days_until_expiry","path":"$.days_until_expiry","datatype":"int"}]'""",

    # ========================================================================
    # PHASE 2 EXTENSION — close CMO/CFO/COO/CTO gaps
    #   CMO: clinical_deterioration_events, mortality_hai_events
    #   CFO: net_revenue_variance_events
    #   COO: ops_capacity_events, staffing_acuity_events  + coo_alerts
    #   CTO: dq_violation_events, audit_access_events, model_drift_events + cto_alerts
    #   Shared: alert_closure_events (MTTR), dashboard_thresholds (config)
    # All ride the SAME single Eventstream → rti_all_events landing table.
    # All edits below are ADDITIVE (.alter-merge / .create-merge) — safe to re-run.
    # ========================================================================

    # --- Extend rti_all_events with new fields used by phase-2 event types ---
    """.alter-merge table rti_all_events (
        encounter_id: string, news2_score: real, sirs_score: real,
        mews_score: real, deterioration_type: string, requires_rrt: bool,
        hai_type: string, organism: string, ward: string,
        cluster_size: long, infection_count_72h: long,
        contract_id: string, contract_type: string,
        expected_payment: real, actual_payment: real,
        variance_pct: real, variance_dollars: real,
        capacity_type: string, current_value: real, threshold_value: real,
        utilization_pct: real, los_hours: real,
        nurse_count: long, patient_count: long, acuity_score: real,
        ratio_actual: real, ratio_target: real, staffing_gap: long,
        source_table: string, rule_name: string, rule_type: string,
        expected_value: real, actual_value: real,
        violation_count: long, violation_pct: real,
        user_principal: string, action_type: string, resource_accessed: string,
        anomaly_score: real, geo_country: string,
        model_name: string, model_version: string, feature_name: string,
        drift_score: real, psi_value: real, ks_value: real,
        baseline_period: string,
        severity: string
    )""",

    # --- CMO: clinical_deterioration_events (sepsis / NEWS2 / MEWS) ---
    """.create-merge table clinical_deterioration_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        patient_id: string, encounter_id: string, facility_id: string,
        ward: string, news2_score: real, sirs_score: real, mews_score: real,
        deterioration_type: string, requires_rrt: bool,
        primary_diagnosis: string, latitude: real, longitude: real
    )""",

    # --- CMO: mortality_hai_events (HAI surveillance / cluster detection) ---
    """.create-merge table mortality_hai_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        facility_id: string, ward: string, hai_type: string,
        organism: string, cluster_size: int, infection_count_72h: int,
        severity: string, latitude: real, longitude: real
    )""",

    # --- CFO: net_revenue_variance_events (expected vs paid by contract) ---
    """.create-merge table net_revenue_variance_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        claim_id: string, payer_id: string, payer_name: string,
        contract_id: string, contract_type: string,
        expected_payment: real, actual_payment: real,
        variance_pct: real, variance_dollars: real,
        provider_id: string, facility_id: string
    )""",

    # --- COO: ops_capacity_events (ED throughput / bed / OR / LOS) ---
    """.create-merge table ops_capacity_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        facility_id: string, facility_name: string,
        capacity_type: string, current_value: real, threshold_value: real,
        utilization_pct: real, los_hours: real,
        ward: string, latitude: real, longitude: real
    )""",

    # --- COO: staffing_acuity_events (nurse-to-acuity ratio) ---
    """.create-merge table staffing_acuity_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        facility_id: string, ward: string,
        nurse_count: int, patient_count: int, acuity_score: real,
        ratio_actual: real, ratio_target: real, staffing_gap: int
    )""",

    # --- CTO: dq_violation_events (real-time data quality) ---
    """.create-merge table dq_violation_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        source_table: string, rule_name: string, rule_type: string,
        expected_value: real, actual_value: real,
        violation_count: int, violation_pct: real, severity: string
    )""",

    # --- CTO: audit_access_events (PHI access anomaly detection) ---
    """.create-merge table audit_access_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        user_principal: string, action_type: string,
        resource_accessed: string, anomaly_score: real,
        geo_country: string, severity: string
    )""",

    # --- CTO: model_drift_events (PSI/KS for fraud + readmit models) ---
    """.create-merge table model_drift_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        model_name: string, model_version: string, feature_name: string,
        drift_score: real, psi_value: real, ks_value: real,
        baseline_period: string, severity: string
    )""",

    # --- COO + CTO alert outputs + shared closure log ---
    """.create-merge table coo_alerts (
        alert_id: string, alert_timestamp: datetime, alert_type: string,
        facility_id: string, ward: string, severity: string,
        alert_text: string, recommended_action: string,
        metric_name: string, metric_value: real, threshold_value: real,
        latitude: real, longitude: real
    )""",
    """.create-merge table cto_alerts (
        alert_id: string, alert_timestamp: datetime, alert_type: string,
        source_system: string, severity: string,
        alert_text: string, recommended_action: string,
        metric_name: string, metric_value: real, threshold_value: real
    )""",
    """.create-merge table alert_closure_events (
        closure_id: string, closure_timestamp: datetime,
        alert_id: string, alert_source_table: string, alert_type: string,
        persona: string, action_taken: string, resolved_by: string,
        resolution_notes: string, mttr_seconds: long, outcome: string
    )""",
    """.create-merge table dashboard_thresholds (
        threshold_key: string, alert_type: string, persona: string,
        warn_value: real, critical_value: real, unit: string,
        active: bool, updated_at: datetime, owner: string
    )""",

    # --- streaming policies (phase 2) ---
    ".alter table clinical_deterioration_events policy streamingingestion enable",
    ".alter table mortality_hai_events policy streamingingestion enable",
    ".alter table net_revenue_variance_events policy streamingingestion enable",
    ".alter table ops_capacity_events policy streamingingestion enable",
    ".alter table staffing_acuity_events policy streamingingestion enable",
    ".alter table dq_violation_events policy streamingingestion enable",
    ".alter table audit_access_events policy streamingingestion enable",
    ".alter table model_drift_events policy streamingingestion enable",
    ".alter table coo_alerts policy streamingingestion enable",
    ".alter table cto_alerts policy streamingingestion enable",
    ".alter table alert_closure_events policy streamingingestion enable",

    # --- Extract functions (route landing → typed) ---
    """.create-or-alter function ExtractClinicalDeteriorationEvents() {
        rti_all_events
        | where _table == "clinical_deterioration_events"
        | project event_id, event_timestamp = todatetime(event_timestamp), event_type,
                  patient_id,
                  encounter_id = coalesce(encounter_id, ""),
                  facility_id = coalesce(facility_id, ""),
                  ward = coalesce(ward, ""),
                  news2_score = coalesce(news2_score, 0.0),
                  sirs_score = coalesce(sirs_score, 0.0),
                  mews_score = coalesce(mews_score, 0.0),
                  deterioration_type = coalesce(deterioration_type, ""),
                  requires_rrt = coalesce(requires_rrt, false),
                  primary_diagnosis = coalesce(primary_diagnosis, ""),
                  latitude, longitude
    }""",
    """.create-or-alter function ExtractMortalityHaiEvents() {
        rti_all_events
        | where _table == "mortality_hai_events"
        | project event_id, event_timestamp = todatetime(event_timestamp), event_type,
                  facility_id = coalesce(facility_id, ""),
                  ward = coalesce(ward, ""),
                  hai_type = coalesce(hai_type, ""),
                  organism = coalesce(organism, ""),
                  cluster_size = toint(coalesce(cluster_size, 0)),
                  infection_count_72h = toint(coalesce(infection_count_72h, 0)),
                  severity = coalesce(severity, ""),
                  latitude, longitude
    }""",
    """.create-or-alter function ExtractNetRevenueVarianceEvents() {
        rti_all_events
        | where _table == "net_revenue_variance_events"
        | project event_id, event_timestamp = todatetime(event_timestamp), event_type,
                  claim_id = coalesce(claim_id, ""),
                  payer_id = coalesce(payer_id, ""),
                  payer_name = coalesce(payer_name, ""),
                  contract_id = coalesce(contract_id, ""),
                  contract_type = coalesce(contract_type, ""),
                  expected_payment = coalesce(expected_payment, 0.0),
                  actual_payment = coalesce(actual_payment, 0.0),
                  variance_pct = coalesce(variance_pct, 0.0),
                  variance_dollars = coalesce(variance_dollars, 0.0),
                  provider_id = coalesce(provider_id, ""),
                  facility_id = coalesce(facility_id, "")
    }""",
    """.create-or-alter function ExtractOpsCapacityEvents() {
        rti_all_events
        | where _table == "ops_capacity_events"
        | project event_id, event_timestamp = todatetime(event_timestamp), event_type,
                  facility_id = coalesce(facility_id, ""),
                  facility_name = coalesce(facility_name, ""),
                  capacity_type = coalesce(capacity_type, ""),
                  current_value = coalesce(current_value, 0.0),
                  threshold_value = coalesce(threshold_value, 0.0),
                  utilization_pct = coalesce(utilization_pct, 0.0),
                  los_hours = coalesce(los_hours, 0.0),
                  ward = coalesce(ward, ""),
                  latitude, longitude
    }""",
    """.create-or-alter function ExtractStaffingAcuityEvents() {
        rti_all_events
        | where _table == "staffing_acuity_events"
        | project event_id, event_timestamp = todatetime(event_timestamp), event_type,
                  facility_id = coalesce(facility_id, ""),
                  ward = coalesce(ward, ""),
                  nurse_count = toint(coalesce(nurse_count, 0)),
                  patient_count = toint(coalesce(patient_count, 0)),
                  acuity_score = coalesce(acuity_score, 0.0),
                  ratio_actual = coalesce(ratio_actual, 0.0),
                  ratio_target = coalesce(ratio_target, 0.0),
                  staffing_gap = toint(coalesce(staffing_gap, 0))
    }""",
    """.create-or-alter function ExtractDqViolationEvents() {
        rti_all_events
        | where _table == "dq_violation_events"
        | project event_id, event_timestamp = todatetime(event_timestamp), event_type,
                  source_table = coalesce(source_table, ""),
                  rule_name = coalesce(rule_name, ""),
                  rule_type = coalesce(rule_type, ""),
                  expected_value = coalesce(expected_value, 0.0),
                  actual_value = coalesce(actual_value, 0.0),
                  violation_count = toint(coalesce(violation_count, 0)),
                  violation_pct = coalesce(violation_pct, 0.0),
                  severity = coalesce(severity, "")
    }""",
    """.create-or-alter function ExtractAuditAccessEvents() {
        rti_all_events
        | where _table == "audit_access_events"
        | project event_id, event_timestamp = todatetime(event_timestamp), event_type,
                  user_principal = coalesce(user_principal, ""),
                  action_type = coalesce(action_type, ""),
                  resource_accessed = coalesce(resource_accessed, ""),
                  anomaly_score = coalesce(anomaly_score, 0.0),
                  geo_country = coalesce(geo_country, ""),
                  severity = coalesce(severity, "")
    }""",
    """.create-or-alter function ExtractModelDriftEvents() {
        rti_all_events
        | where _table == "model_drift_events"
        | project event_id, event_timestamp = todatetime(event_timestamp), event_type,
                  model_name = coalesce(model_name, ""),
                  model_version = coalesce(model_version, ""),
                  feature_name = coalesce(feature_name, ""),
                  drift_score = coalesce(drift_score, 0.0),
                  psi_value = coalesce(psi_value, 0.0),
                  ks_value = coalesce(ks_value, 0.0),
                  baseline_period = coalesce(baseline_period, ""),
                  severity = coalesce(severity, "")
    }""",

    # --- update policies (phase 2) ---
    ".alter table clinical_deterioration_events policy update @'[{\"IsEnabled\": true, \"Source\": \"rti_all_events\", \"Query\": \"ExtractClinicalDeteriorationEvents()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": true}]'",
    ".alter table mortality_hai_events policy update @'[{\"IsEnabled\": true, \"Source\": \"rti_all_events\", \"Query\": \"ExtractMortalityHaiEvents()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": true}]'",
    ".alter table net_revenue_variance_events policy update @'[{\"IsEnabled\": true, \"Source\": \"rti_all_events\", \"Query\": \"ExtractNetRevenueVarianceEvents()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": true}]'",
    ".alter table ops_capacity_events policy update @'[{\"IsEnabled\": true, \"Source\": \"rti_all_events\", \"Query\": \"ExtractOpsCapacityEvents()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": true}]'",
    ".alter table staffing_acuity_events policy update @'[{\"IsEnabled\": true, \"Source\": \"rti_all_events\", \"Query\": \"ExtractStaffingAcuityEvents()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": true}]'",
    ".alter table dq_violation_events policy update @'[{\"IsEnabled\": true, \"Source\": \"rti_all_events\", \"Query\": \"ExtractDqViolationEvents()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": true}]'",
    ".alter table audit_access_events policy update @'[{\"IsEnabled\": true, \"Source\": \"rti_all_events\", \"Query\": \"ExtractAuditAccessEvents()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": true}]'",
    ".alter table model_drift_events policy update @'[{\"IsEnabled\": true, \"Source\": \"rti_all_events\", \"Query\": \"ExtractModelDriftEvents()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": true}]'",

    # --- OneLake mirroring (5-min flush) for phase-2 tables ---
    ".alter-merge table clinical_deterioration_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table mortality_hai_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table net_revenue_variance_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table ops_capacity_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table staffing_acuity_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table dq_violation_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table audit_access_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table model_drift_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table coo_alerts policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table cto_alerts policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",
    ".alter-merge table alert_closure_events policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)",

    # --- retention / caching policies (production-hardening) ---
    # Hot cache 7 days, retention 90 days for events; alerts retained 365 days.
    ".alter-merge table clinical_deterioration_events policy retention softdelete = 90d",
    ".alter-merge table mortality_hai_events policy retention softdelete = 365d",
    ".alter-merge table net_revenue_variance_events policy retention softdelete = 365d",
    ".alter-merge table ops_capacity_events policy retention softdelete = 90d",
    ".alter-merge table staffing_acuity_events policy retention softdelete = 90d",
    ".alter-merge table dq_violation_events policy retention softdelete = 90d",
    ".alter-merge table audit_access_events policy retention softdelete = 365d",
    ".alter-merge table model_drift_events policy retention softdelete = 180d",
    ".alter-merge table coo_alerts policy retention softdelete = 365d",
    ".alter-merge table cto_alerts policy retention softdelete = 365d",
    ".alter-merge table alert_closure_events policy retention softdelete = 730d",
    ".alter-merge table clinical_deterioration_events policy caching hot = 7d",
    ".alter-merge table mortality_hai_events policy caching hot = 30d",
    ".alter-merge table net_revenue_variance_events policy caching hot = 30d",
    ".alter-merge table ops_capacity_events policy caching hot = 7d",
    ".alter-merge table staffing_acuity_events policy caching hot = 7d",
    ".alter-merge table dq_violation_events policy caching hot = 7d",
    ".alter-merge table audit_access_events policy caching hot = 30d",
    ".alter-merge table model_drift_events policy caching hot = 30d",

    # --- ingestion json mappings (phase 2) ---
    """.create-or-alter table clinical_deterioration_events ingestion json mapping 'clinical_deterioration_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"encounter_id","path":"$.encounter_id","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"},{"column":"ward","path":"$.ward","datatype":"string"},{"column":"news2_score","path":"$.news2_score","datatype":"real"},{"column":"sirs_score","path":"$.sirs_score","datatype":"real"},{"column":"mews_score","path":"$.mews_score","datatype":"real"},{"column":"deterioration_type","path":"$.deterioration_type","datatype":"string"},{"column":"requires_rrt","path":"$.requires_rrt","datatype":"bool"},{"column":"primary_diagnosis","path":"$.primary_diagnosis","datatype":"string"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"}]'""",
    """.create-or-alter table mortality_hai_events ingestion json mapping 'mortality_hai_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"},{"column":"ward","path":"$.ward","datatype":"string"},{"column":"hai_type","path":"$.hai_type","datatype":"string"},{"column":"organism","path":"$.organism","datatype":"string"},{"column":"cluster_size","path":"$.cluster_size","datatype":"int"},{"column":"infection_count_72h","path":"$.infection_count_72h","datatype":"int"},{"column":"severity","path":"$.severity","datatype":"string"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"}]'""",
    """.create-or-alter table net_revenue_variance_events ingestion json mapping 'net_revenue_variance_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"claim_id","path":"$.claim_id","datatype":"string"},{"column":"payer_id","path":"$.payer_id","datatype":"string"},{"column":"payer_name","path":"$.payer_name","datatype":"string"},{"column":"contract_id","path":"$.contract_id","datatype":"string"},{"column":"contract_type","path":"$.contract_type","datatype":"string"},{"column":"expected_payment","path":"$.expected_payment","datatype":"real"},{"column":"actual_payment","path":"$.actual_payment","datatype":"real"},{"column":"variance_pct","path":"$.variance_pct","datatype":"real"},{"column":"variance_dollars","path":"$.variance_dollars","datatype":"real"},{"column":"provider_id","path":"$.provider_id","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"}]'""",
    """.create-or-alter table ops_capacity_events ingestion json mapping 'ops_capacity_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"},{"column":"facility_name","path":"$.facility_name","datatype":"string"},{"column":"capacity_type","path":"$.capacity_type","datatype":"string"},{"column":"current_value","path":"$.current_value","datatype":"real"},{"column":"threshold_value","path":"$.threshold_value","datatype":"real"},{"column":"utilization_pct","path":"$.utilization_pct","datatype":"real"},{"column":"los_hours","path":"$.los_hours","datatype":"real"},{"column":"ward","path":"$.ward","datatype":"string"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"}]'""",
    """.create-or-alter table staffing_acuity_events ingestion json mapping 'staffing_acuity_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"},{"column":"ward","path":"$.ward","datatype":"string"},{"column":"nurse_count","path":"$.nurse_count","datatype":"int"},{"column":"patient_count","path":"$.patient_count","datatype":"int"},{"column":"acuity_score","path":"$.acuity_score","datatype":"real"},{"column":"ratio_actual","path":"$.ratio_actual","datatype":"real"},{"column":"ratio_target","path":"$.ratio_target","datatype":"real"},{"column":"staffing_gap","path":"$.staffing_gap","datatype":"int"}]'""",
    """.create-or-alter table dq_violation_events ingestion json mapping 'dq_violation_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"source_table","path":"$.source_table","datatype":"string"},{"column":"rule_name","path":"$.rule_name","datatype":"string"},{"column":"rule_type","path":"$.rule_type","datatype":"string"},{"column":"expected_value","path":"$.expected_value","datatype":"real"},{"column":"actual_value","path":"$.actual_value","datatype":"real"},{"column":"violation_count","path":"$.violation_count","datatype":"int"},{"column":"violation_pct","path":"$.violation_pct","datatype":"real"},{"column":"severity","path":"$.severity","datatype":"string"}]'""",
    """.create-or-alter table audit_access_events ingestion json mapping 'audit_access_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"user_principal","path":"$.user_principal","datatype":"string"},{"column":"action_type","path":"$.action_type","datatype":"string"},{"column":"resource_accessed","path":"$.resource_accessed","datatype":"string"},{"column":"anomaly_score","path":"$.anomaly_score","datatype":"real"},{"column":"geo_country","path":"$.geo_country","datatype":"string"},{"column":"severity","path":"$.severity","datatype":"string"}]'""",
    """.create-or-alter table model_drift_events ingestion json mapping 'model_drift_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"model_name","path":"$.model_name","datatype":"string"},{"column":"model_version","path":"$.model_version","datatype":"string"},{"column":"feature_name","path":"$.feature_name","datatype":"string"},{"column":"drift_score","path":"$.drift_score","datatype":"real"},{"column":"psi_value","path":"$.psi_value","datatype":"real"},{"column":"ks_value","path":"$.ks_value","datatype":"real"},{"column":"baseline_period","path":"$.baseline_period","datatype":"string"},{"column":"severity","path":"$.severity","datatype":"string"}]'""",
    """.create-or-alter table coo_alerts ingestion json mapping 'coo_alerts_mapping'
    '[{"column":"alert_id","path":"$.alert_id","datatype":"string"},{"column":"alert_timestamp","path":"$.alert_timestamp","datatype":"datetime"},{"column":"alert_type","path":"$.alert_type","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"},{"column":"ward","path":"$.ward","datatype":"string"},{"column":"severity","path":"$.severity","datatype":"string"},{"column":"alert_text","path":"$.alert_text","datatype":"string"},{"column":"recommended_action","path":"$.recommended_action","datatype":"string"},{"column":"metric_name","path":"$.metric_name","datatype":"string"},{"column":"metric_value","path":"$.metric_value","datatype":"real"},{"column":"threshold_value","path":"$.threshold_value","datatype":"real"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"}]'""",
    """.create-or-alter table cto_alerts ingestion json mapping 'cto_alerts_mapping'
    '[{"column":"alert_id","path":"$.alert_id","datatype":"string"},{"column":"alert_timestamp","path":"$.alert_timestamp","datatype":"datetime"},{"column":"alert_type","path":"$.alert_type","datatype":"string"},{"column":"source_system","path":"$.source_system","datatype":"string"},{"column":"severity","path":"$.severity","datatype":"string"},{"column":"alert_text","path":"$.alert_text","datatype":"string"},{"column":"recommended_action","path":"$.recommended_action","datatype":"string"},{"column":"metric_name","path":"$.metric_name","datatype":"string"},{"column":"metric_value","path":"$.metric_value","datatype":"real"},{"column":"threshold_value","path":"$.threshold_value","datatype":"real"}]'""",
    """.create-or-alter table alert_closure_events ingestion json mapping 'alert_closure_events_mapping'
    '[{"column":"closure_id","path":"$.closure_id","datatype":"string"},{"column":"closure_timestamp","path":"$.closure_timestamp","datatype":"datetime"},{"column":"alert_id","path":"$.alert_id","datatype":"string"},{"column":"alert_source_table","path":"$.alert_source_table","datatype":"string"},{"column":"alert_type","path":"$.alert_type","datatype":"string"},{"column":"persona","path":"$.persona","datatype":"string"},{"column":"action_taken","path":"$.action_taken","datatype":"string"},{"column":"resolved_by","path":"$.resolved_by","datatype":"string"},{"column":"resolution_notes","path":"$.resolution_notes","datatype":"string"},{"column":"mttr_seconds","path":"$.mttr_seconds","datatype":"long"},{"column":"outcome","path":"$.outcome","datatype":"string"}]'""",

    # --- seed default thresholds (idempotent: .set-or-replace replaces row set) ---
    """.set-or-replace dashboard_thresholds <|
        datatable(threshold_key:string, alert_type:string, persona:string,
                  warn_value:real, critical_value:real, unit:string,
                  active:bool, updated_at:datetime, owner:string)
        [
          "readmission_risk",        "READMISSION_30DAY",       "CMO", 0.40, 0.65, "score",   true, datetime(2026-01-01), "clinical-ops",
          "quality_drift_pct",       "QUALITY_DRIFT",           "CMO", 5.0,  10.0, "pct",     true, datetime(2026-01-01), "clinical-ops",
          "sepsis_news2",            "SEPSIS_RISK",             "CMO", 5.0,  7.0,  "score",   true, datetime(2026-01-01), "clinical-ops",
          "hai_cluster_size",        "HAI_CLUSTER",             "CMO", 3.0,  5.0,  "count",   true, datetime(2026-01-01), "infection-prev",
          "denial_rate_pct",         "DENIAL_SPIKE",            "CFO", 8.0,  12.0, "pct",     true, datetime(2026-01-01), "rcm",
          "high_dollar_denial",      "HIGH_DOLLAR_DENIAL",      "CFO", 5000.0,10000.0,"usd",  true, datetime(2026-01-01), "rcm",
          "ar_aging_days",           "AR_AGING_BREACH",         "CFO", 35.0, 45.0, "days",    true, datetime(2026-01-01), "rcm",
          "auth_expiring_days",      "AUTH_EXPIRING",           "CFO", 14.0, 7.0,  "days",    true, datetime(2026-01-01), "rcm",
          "underpayment_pct",        "UNDERPAYMENT_DETECTED",   "CFO", 5.0,  10.0, "pct",     true, datetime(2026-01-01), "contract-mgmt",
          "ed_boarding_count",       "ED_BOARDING_BREACH",      "COO", 10.0, 20.0, "count",   true, datetime(2026-01-01), "ops",
          "bed_occupancy_pct",       "BED_CAPACITY_CRITICAL",   "COO", 85.0, 95.0, "pct",     true, datetime(2026-01-01), "ops",
          "or_turnover_minutes",     "OR_DELAY",                "COO", 45.0, 60.0, "minutes", true, datetime(2026-01-01), "ops",
          "staffing_gap_count",      "STAFFING_GAP",            "COO", 1.0,  3.0,  "count",   true, datetime(2026-01-01), "ops",
          "dq_violation_pct",        "DQ_THRESHOLD_BREACH",     "CTO", 1.0,  5.0,  "pct",     true, datetime(2026-01-01), "platform"  ,
          "phi_anomaly_score",       "PHI_ANOMALY",             "CTO", 0.7,  0.9,  "score",   true, datetime(2026-01-01), "security",
          "model_drift_psi",         "MODEL_DRIFT",             "CTO", 0.1,  0.25, "psi",     true, datetime(2026-01-01), "ml-platform"
        ]
    """,

    # ========================================================================
    # DEMO VIEWS — unified alert shape, cross-stream worst-patient, provider
    # scorecard, and MTTD/MTTR. Consumed by dashboard tiles + Operations Agent
    # + the lightweight frontend. All are idempotent KQL functions.
    # ========================================================================
    """.create-or-alter function with (folder='views', docstring='Unified shape across all alert streams (fraud, care gap, high-cost, readmission, clinical deterioration). 7-day lookback baked in.') vw_all_alerts() {
        let fraud = fraud_scores
            | where score_timestamp >= ago(7d) and fraud_score >= 50
            | project alert_id = score_id, alert_timestamp = score_timestamp,
                      source_table = 'fraud_scores', alert_type = 'FRAUD',
                      patient_id, provider_id, facility_id,
                      severity = risk_tier, score = fraud_score,
                      alert_text = strcat('Fraud ', tostring(toint(fraud_score)), '/100 flags=', fraud_flags);
        let cgap = care_gap_alerts
            | where alert_timestamp >= ago(7d) and gap_days_overdue > 90
            | project alert_id, alert_timestamp,
                      source_table = 'care_gap_alerts', alert_type = 'CARE_GAP',
                      patient_id, provider_id = '', facility_id,
                      severity = alert_priority, score = todouble(gap_days_overdue),
                      alert_text;
        let hcost = highcost_alerts
            | where alert_timestamp >= ago(7d)
            | project alert_id, alert_timestamp,
                      source_table = 'highcost_alerts', alert_type = 'HIGH_COST',
                      patient_id, provider_id = '', facility_id = '',
                      severity = risk_tier, score = rolling_spend_30d,
                      alert_text = strcat('Spend30d $', tostring(toint(rolling_spend_30d)), ' ED=', tostring(ed_visits_30d));
        let readm = readmission_events
            | where event_timestamp >= ago(7d) and (readmission_risk_score >= 70 or (days_since_discharge <= 7 and readmission_risk_score >= 50))
            | project alert_id = event_id, alert_timestamp = event_timestamp,
                      source_table = 'readmission_events', alert_type = 'READMISSION',
                      patient_id, provider_id, facility_id,
                      severity = case(readmission_risk_score >= 80, 'CRITICAL', readmission_risk_score >= 70, 'HIGH', 'MEDIUM'),
                      score = readmission_risk_score,
                      alert_text = strcat('Readmit ', tostring(days_since_discharge), 'd DRG=', drg_code, ' risk=', tostring(toint(readmission_risk_score)));
        let clin = clinical_deterioration_events
            | where event_timestamp >= ago(7d) and news2_score >= 5
            | project alert_id = event_id, alert_timestamp = event_timestamp,
                      source_table = 'clinical_deterioration_events', alert_type = 'DETERIORATION',
                      patient_id, provider_id = '', facility_id,
                      severity = case(news2_score >= 7, 'CRITICAL', 'HIGH'),
                      score = news2_score,
                      alert_text = strcat(deterioration_type, ' NEWS2=', tostring(news2_score), ' ward=', ward);
        union fraud, cgap, hcost, readm, clin
    }""",
    """.create-or-alter function with (folder='views', docstring='Cross-stream worst-patient ranking: members appearing in >=2 alert streams in the lookback window. Default 24h. This is the killer provider triage view.') vw_cross_stream_patients(lookback:timespan=24h) {
        vw_all_alerts()
        | where alert_timestamp >= ago(lookback)
        | summarize stream_count = dcount(source_table),
                    streams = strcat_array(make_set(source_table), ', '),
                    total_alerts = count(),
                    max_score = max(score),
                    last_alert = max(alert_timestamp),
                    sample_text = take_any(alert_text)
          by patient_id
        | where stream_count >= 2
        | extend severity_rank = case(stream_count >= 3, 'CRITICAL', 'HIGH')
        | order by stream_count desc, max_score desc, last_alert desc
    }""",
    """.create-or-alter function with (folder='views', docstring='Provider scorecard: fraud p95, denial rate, readmit count over the lookback window. Composite_risk ranks the CMO coaching list. Default 7d.') vw_provider_scorecard(lookback:timespan=7d) {
        let provider_ids = union
            (fraud_scores | where score_timestamp >= ago(lookback) and isnotempty(provider_id) | distinct provider_id),
            (denial_adjudication_events | where event_timestamp >= ago(lookback) and isnotempty(provider_id) | distinct provider_id),
            (readmission_events | where event_timestamp >= ago(lookback) and isnotempty(provider_id) | distinct provider_id)
            | distinct provider_id;
        let fraud_agg = fraud_scores
            | where score_timestamp >= ago(lookback) and isnotempty(provider_id)
            | summarize fraud_score_p95 = round(percentile(fraud_score, 95), 1),
                        fraud_alerts = countif(fraud_score >= 50)
              by provider_id;
        let denial_agg = denial_adjudication_events
            | where event_timestamp >= ago(lookback) and isnotempty(provider_id)
            | summarize total_claims = count(),
                        denied_claims = countif(adjudication_result == 'DENIED')
              by provider_id
            | extend denial_rate_pct = round(100.0 * denied_claims / total_claims, 1);
        let readm_agg = readmission_events
            | where event_timestamp >= ago(lookback) and isnotempty(provider_id)
            | summarize readmit_count = count() by provider_id;
        provider_ids
        | join kind=leftouter fraud_agg on provider_id
        | join kind=leftouter denial_agg on provider_id
        | join kind=leftouter readm_agg on provider_id
        | project provider_id,
                  fraud_score_p95 = coalesce(fraud_score_p95, 0.0),
                  fraud_alerts = coalesce(fraud_alerts, toint(0)),
                  total_claims = coalesce(total_claims, toint(0)),
                  denied_claims = coalesce(denied_claims, toint(0)),
                  denial_rate_pct = coalesce(denial_rate_pct, 0.0),
                  readmit_count = coalesce(readmit_count, toint(0))
        | extend composite_risk = round((fraud_score_p95 * 0.4) + (denial_rate_pct * 2.0) + (toreal(readmit_count) * 5.0), 1)
        | order by composite_risk desc
    }""",
    """.create-or-alter function with (folder='views', docstring='MTTD + MTTR per alert. Joins unified alerts to alert_closure_events. status=OPEN means no Power Automate Acknowledge click yet.') vw_alert_mttr(lookback:timespan=7d) {
        vw_all_alerts()
        | where alert_timestamp >= ago(lookback)
        | join kind=leftouter alert_closure_events on alert_id
        | project alert_id, alert_timestamp, source_table, alert_type, patient_id,
                  severity, score, alert_text,
                  closure_timestamp, resolved_by, action_taken,
                  mttr_seconds = case(isnotnull(closure_timestamp),
                                      toint(datetime_diff('second', closure_timestamp, alert_timestamp)),
                                      toint(0)),
                  status = case(isnotnull(closure_timestamp), 'CLOSED', 'OPEN')
    }""",
    """.create-or-alter function with (folder='views', docstring='Seeded demo scenarios status. Filters by injected_fraud_flags startswith SEED: to surface the four named scenarios for presenter cues.') vw_seeded_scenarios(lookback:timespan=2h) {
        let claims_seed = claims_events
            | where event_timestamp >= ago(lookback) and injected_fraud_flags startswith 'SEED:'
            | project alert_timestamp = event_timestamp, source = 'claims_events',
                      seed_label = injected_fraud_flags, patient_id, provider_id, facility_id,
                      detail = strcat('Claim $', tostring(toint(claim_amount)), ' ', procedure_code);
        let readm_seed = readmission_events
            | where event_timestamp >= ago(lookback) and current_encounter_id startswith 'SEED-'
            | project alert_timestamp = event_timestamp, source = 'readmission_events',
                      seed_label = current_encounter_id, patient_id, provider_id, facility_id,
                      detail = strcat('Readmit ', tostring(days_since_discharge), 'd DRG=', drg_code);
        let cgap_seed = care_gap_alerts
            | where alert_timestamp >= ago(lookback) and alert_id startswith 'SEED-'
            | project alert_timestamp, source = 'care_gap_alerts',
                      seed_label = alert_id, patient_id, provider_id = '', facility_id,
                      detail = alert_text;
        let hcost_seed = highcost_alerts
            | where alert_timestamp >= ago(lookback) and alert_id startswith 'SEED-'
            | project alert_timestamp, source = 'highcost_alerts',
                      seed_label = alert_id, patient_id, provider_id = '', facility_id = '',
                      detail = strcat('Spend30d $', tostring(toint(rolling_spend_30d)), ' ED=', tostring(ed_visits_30d));
        let ops_seed = ops_capacity_events
            | where event_timestamp >= ago(lookback) and event_type startswith 'SEED_'
            | project alert_timestamp = event_timestamp, source = 'ops_capacity_events',
                      seed_label = event_type, patient_id = '', provider_id = '', facility_id,
                      detail = strcat(capacity_type, ' = ', tostring(current_value), ' / ', tostring(threshold_value));
        union claims_seed, readm_seed, cgap_seed, hcost_seed, ops_seed
        | order by alert_timestamp desc
    }""",
    """.create-or-alter function with (folder='views', docstring='Patient + provider 360 enrichment. Requires OneLake table shortcuts to lh_gold_curated.dim_patient and dim_provider (created once via Eventhouse portal). Function deploys safely even if shortcuts are absent -- it only fails at QUERY time, not create time.') vw_alerts_enriched(lookback:timespan=7d) {
        let _patients = materialize(
            table('dim_patient')
            | where is_current == true
            | project patient_id,
                      patient_name = strcat(first_name, ' ', last_name),
                      patient_dob = date_of_birth,
                      patient_gender = gender,
                      patient_payer_id = payer_id,
                      patient_risk_tier = risk_tier
        );
        let _providers = materialize(
            table('dim_provider')
            | where is_current == true
            | project provider_id,
                      provider_name = display_name,
                      provider_specialty = specialty,
                      provider_npi = npi,
                      provider_home_facility = facility_id
        );
        let _facilities = materialize(
            table('dim_facility')
            | project facility_id,
                      facility_name,
                      facility_region = region,
                      facility_lat = latitude,
                      facility_lon = longitude
        );
        vw_all_alerts()
        | where alert_timestamp >= ago(lookback)
        | join kind=leftouter _patients   on patient_id
        | join kind=leftouter _providers  on provider_id
        | join kind=leftouter _facilities on facility_id
        | project alert_id, alert_timestamp, source_table, alert_type, severity, score, alert_text,
                  patient_id, patient_name, patient_dob, patient_gender, patient_payer_id, patient_risk_tier,
                  provider_id, provider_name, provider_specialty, provider_npi, provider_home_facility,
                  facility_id, facility_name, facility_region, facility_lat, facility_lon
        | order by alert_timestamp desc
    }""",
]

# --- Install Kusto SDK if not already present ---
import subprocess, sys
try:
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
    from azure.kusto.data.exceptions import KustoServiceError
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "azure-kusto-data"])
    # Purge stale azure.* module refs so fresh import works
    import importlib
    for mod_name in list(sys.modules.keys()):
        if mod_name.startswith("azure"):
            del sys.modules[mod_name]
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
    from azure.kusto.data.exceptions import KustoServiceError

_kql_token = get_kusto_token()
_kcs = KustoConnectionStringBuilder.with_aad_user_token_authentication(KUSTO_QUERY_URI, _kql_token)
_kusto_client = KustoClient(_kcs)

def run_kql_mgmt(command):
    """Execute a KQL management command via Kusto SDK."""
    return _kusto_client.execute_mgmt(KQL_DB_NAME, command.strip())

def run_kql_query(query):
    """Execute a KQL query via Kusto SDK."""
    return _kusto_client.execute(KQL_DB_NAME, query.strip())

success_count = 0
fail_count = 0
for cmd in KQL_COMMANDS:
    cmd_clean = cmd.strip()
    if not cmd_clean:
        continue
    if "create-merge table" in cmd_clean or "alter-merge table" in cmd_clean:
        label = cmd_clean.split("table ")[-1].split(" ")[0].split("(")[0].strip()
    elif "alter table" in cmd_clean and "policy" in cmd_clean:
        label = "streaming policy: " + cmd_clean.split("table ")[-1].split(" ")[0]
    elif "ingestion json mapping" in cmd_clean:
        label = "mapping: " + cmd_clean.split("table ")[-1].split(" ")[0]
    else:
        label = cmd_clean[:60]
    try:
        run_kql_mgmt(cmd_clean)
        print(f"  OK: {label}")
        success_count += 1
    except KustoServiceError as e:
        print(f"  WARN: {label} -- {str(e)[:200]}")
        fail_count += 1
    except Exception as e:
        print(f"  WARN: {label} -- {str(e)[:200]}")
        fail_count += 1

print(f"\nSchema execution complete: {success_count} succeeded, {fail_count} failed")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Step 3b: Backfill typed tables from existing rti_all_events data
# ============================================================================
# KQL update policies only process NEW ingestion. If rti_all_events already
# has data (e.g. simulator ran before policies were created), we must backfill
# the typed tables once. This is idempotent: checks if target is empty first.
# ============================================================================
print("Step 3b: Checking if backfill is needed...")

BACKFILL_COMMANDS = [
    # Check counts and backfill if needed
    ("claims_events", ".set-or-append claims_events <| ExtractClaimsEvents()"),
    ("adt_events", ".set-or-append adt_events <| ExtractAdtEvents()"),
    ("rx_events", ".set-or-append rx_events <| ExtractRxEvents()"),
]

# First check if rti_all_events has any data
_landing_count = 0
try:
    _result = run_kql_query("rti_all_events | count")
    for row in _result.primary_results[0]:
        _landing_count = int(row[0])
except Exception:
    pass

if _landing_count > 0:
    print(f"  rti_all_events has {_landing_count} rows — checking typed tables...")
    for _tbl_name, _backfill_cmd in BACKFILL_COMMANDS:
        # Check if target already has data
        _tbl_count = 0
        try:
            _cnt_result = run_kql_query(f"{_tbl_name} | count")
            for row in _cnt_result.primary_results[0]:
                _tbl_count = int(row[0])
        except Exception:
            pass
        if _tbl_count == 0:
            print(f"  Backfilling {_tbl_name} from rti_all_events...")
            try:
                run_kql_mgmt(_backfill_cmd)
                print(f"    OK: {_tbl_name} backfilled")
            except Exception as e:
                print(f"    WARN: {_tbl_name} -- {str(e)[:200]}")
        else:
            print(f"  {_tbl_name}: already has {_tbl_count} rows — skipping backfill")
else:
    print("  rti_all_events is empty — no backfill needed (data will flow via update policies)")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Step 4: Verify setup
# ============================================================================
print("\nStep 4: Verifying RTI setup...")

try:
    _verify_result = run_kql_mgmt(".show tables | project TableName | order by TableName asc")
    print("  KQL Database tables:")
    for row in _verify_result.primary_results[0]:
        print(f"    - {row[0]}")
except Exception as e:
    print(f"  Could not verify tables: {str(e)[:200]}")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Step 5: Store Kusto URIs for downstream notebooks
# ============================================================================
# The Event Simulator and scoring notebooks use these to push data to KQL.
# notebookutils.notebook.run() shares the mssparkutils context automatically,
# but we also store them as notebook exit values for explicit passing.

print("Step 5: Storing Kusto configuration...")

kusto_config = {
    "kusto_query_uri": KUSTO_QUERY_URI,
    "kusto_ingest_uri": KUSTO_INGEST_URI,
    "kql_db_name": KQL_DB_NAME,
    "eventhouse_id": eventhouse_id,
    "kql_db_id": kql_db_id,
}

# Store as notebook output for downstream consumption
notebookutils.notebook.exit(json.dumps(kusto_config))

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Summary ----------
print("\n" + "=" * 70)
print("  NB_RTI_Setup_Eventhouse: COMPLETE")
print("=" * 70)
print()
print(f"  Workspace:     {WORKSPACE_ID}")
print(f"  Eventhouse:    {EVENTHOUSE_NAME} ({eventhouse_id})")
print(f"  KQL DB:        {KQL_DB_NAME} ({kql_db_id})")
print(f"  Query URI:     {KUSTO_QUERY_URI}")
print(f"  Ingestion URI: {KUSTO_INGEST_URI}")
print()
print("  KQL Tables (7):")
print("    LANDING: rti_all_events (Eventstream → update policies split to typed tables)")
print("    INPUT:  claims_events, adt_events, rx_events")
print("    OUTPUT: fraud_scores, care_gap_alerts, highcost_alerts")
print()
print("  Ingestion: Direct Kusto (azure-kusto-ingest, pre-installed)")
print("    - Zero config: token from notebookutils.credentials.getToken('kusto')")
print("    - No Eventstream or connection strings needed")
print()
print("  Next Steps:")
print("    1. Run NB_RTI_Event_Simulator (batch mode) to seed events")
print("    2. Run NB_RTI_Fraud_Detection to score claims")
print("    3. Run NB_RTI_Care_Gap_Alerts for point-of-care alerts")
print("    4. Run NB_RTI_HighCost_Trajectory for cost trend analysis")
print("    5. For live streaming: set MODE='stream' in Event Simulator")
print("       (uses direct Kusto ingestion -- no manual config needed)")
print("=" * 70)

# METADATA **{"language":"python"}**
