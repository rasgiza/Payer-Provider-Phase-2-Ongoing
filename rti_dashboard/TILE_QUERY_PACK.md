# RTI Dashboard — Phase 2 Tile Query Pack

**Purpose:** Copy-paste KQL queries to extend `healthcare_rti_dashboard` (Real-Time Intelligence Dashboard) in the Fabric portal, then re-export via `export_rti_dashboard.py`.

**Lane rule (anti-duplication with Power BI):**
> If the answer doesn't change in ≥1 hour → it belongs in **Power BI** (strategic / historical).
> If the answer changes in seconds-to-minutes → it belongs **here** (operational / right-now).

---

## Workflow

1. Open `Healthcare_RTI_DB` Eventhouse → `healthcare_rti_dashboard` (RTI dashboard).
2. **Edit** mode → add new pages from the layout below.
3. For each tile: click **+ Add tile** → paste the KQL → set visual + title.
4. Save dashboard.
5. From repo root run:
   ```powershell
   python .\export_rti_dashboard.py
   ```
   This rewrites `rti_dashboard/healthcare_rti_dashboard.json` with tokens for redeploy.
6. Commit and let `Healthcare_Launcher` Cell 12 redeploy on demo.

---

## Page 1 — Executive Pulse (CEO / All-personas overview)

> **Audience:** Anyone walking into the war room. 6 KPI tiles + 1 alert feed.

### Tile 1.1 — Open Critical Alerts (KPI card)
```kql
union provider_alerts, payer_alerts, coo_alerts, cto_alerts
| where alert_timestamp > ago(1h)
| where severity == "CRITICAL"
| summarize OpenCritical = count()
```
**Visual:** Stat card · color = red if > 0

### Tile 1.2 — Alerts by Persona (Donut, 1h)
```kql
union
    (provider_alerts | extend persona = "CMO"),
    (payer_alerts    | extend persona = "CFO"),
    (coo_alerts      | extend persona = "COO"),
    (cto_alerts      | extend persona = "CTO")
| where alert_timestamp > ago(1h)
| summarize Count = count() by persona
```
**Visual:** Pie / Donut

### Tile 1.3 — Alert Velocity (last 60m, 5-min bins)
```kql
union provider_alerts, payer_alerts, coo_alerts, cto_alerts
| where alert_timestamp > ago(1h)
| summarize Alerts = count() by bin(alert_timestamp, 5m), severity
| render timechart
```
**Visual:** Time chart, stacked

### Tile 1.4 — MTTR (last 24h, by persona)
```kql
external_table("rti_alert_mttr") // OneLake shortcut OR query KQL directly
| project persona, alert_type, p50_close_minutes, p90_close_minutes, open_count, resolved_count
| order by p90_close_minutes desc
```
**Visual:** Table (heatmap on p90_close_minutes column)

### Tile 1.5 — Live Event Throughput (events/sec, 15m)
```kql
rti_all_events
| where event_timestamp > ago(15m)
| summarize EventsPerSec = count() / 1.0 by bin(event_timestamp, 1m), _table
| render timechart
```

### Tile 1.6 — Alert Inbox (last 60m, all personas)
```kql
union
    (provider_alerts | extend persona = "CMO"),
    (payer_alerts    | extend persona = "CFO"),
    (coo_alerts      | extend persona = "COO"),
    (cto_alerts      | extend persona = "CTO")
| where alert_timestamp > ago(1h)
| project alert_timestamp, persona, severity, alert_type, alert_text, recommended_action
| order by alert_timestamp desc
| take 50
```
**Visual:** Table · conditional format severity column

---

## Page 2 — Clinical Ops (CMO)

### Tile 2.1 — Active Deterioration Patients (NEWS2 ≥ 5)
```kql
clinical_deterioration_events
| where event_timestamp > ago(2h)
| summarize arg_max(event_timestamp, *) by patient_id
| where news2_score >= 5 or sirs_score >= 2 or mews_score >= 4
| project event_timestamp, facility_id, ward, patient_id, deterioration_type,
          news2_score, sirs_score, mews_score, requires_rrt
| order by news2_score desc
```
**Visual:** Table · color requires_rrt true=red

### Tile 2.2 — Deterioration Heatmap (ward × hour)
```kql
clinical_deterioration_events
| where event_timestamp > ago(24h)
| where news2_score >= 5
| summarize Cases = count() by ward, bin(event_timestamp, 1h)
| render columnchart kind=stacked
```

### Tile 2.3 — HAI Cluster Surveillance
```kql
mortality_hai_events
| where event_timestamp > ago(72h)
| summarize Cluster = max(cluster_size), Count = sum(infection_count_72h),
            LastSeen = max(event_timestamp)
            by facility_id, ward, hai_type, organism
| where Cluster >= 2
| order by Cluster desc, Count desc
```
**Visual:** Table · highlight Cluster ≥ 3

### Tile 2.4 — Readmissions in Last 24h
```kql
readmission_events
| where event_timestamp > ago(24h)
| summarize Count = count() by bin(event_timestamp, 1h), severity = case(
        days_since_discharge <= 7,  "≤7 days",
        days_since_discharge <= 14, "≤14 days",
        "≤30 days")
| render columnchart
```

### Tile 2.5 — Quality Drift Watchlist
```kql
provider_alerts
| where alert_type == "QUALITY_DRIFT" and alert_timestamp > ago(24h)
| project alert_timestamp, provider_name, metric_name, metric_value, benchmark_value, severity
| order by alert_timestamp desc
```

---

## Page 3 — Revenue Ops (CFO)

### Tile 3.1 — Net Revenue Variance (rolling 24h $)
```kql
net_revenue_variance_events
| where event_timestamp > ago(24h)
| summarize TotalVariance = sum(variance_dollars), AvgPct = avg(variance_pct)
            by contract_type
| order by TotalVariance asc
```
**Visual:** Bar (negative = red)

### Tile 3.2 — Underpayment Alerts (variance < -5%)
```kql
net_revenue_variance_events
| where event_timestamp > ago(6h) and variance_pct < -5
| project event_timestamp, payer_name, contract_id, contract_type,
          expected_payment, actual_payment, variance_pct, variance_dollars
| order by variance_dollars asc
| take 50
```

### Tile 3.3 — Fraud Score Heatmap (last 24h)
```kql
fraud_scores
| where event_timestamp > ago(24h)
| summarize MaxScore = max(fraud_score), Cases = count()
            by provider_id, scheme_type
| where MaxScore > 0.7
| order by MaxScore desc
```

### Tile 3.4 — High-Cost Trajectory Watchlist
```kql
high_cost_alerts
| where alert_timestamp > ago(7d)
| project alert_timestamp, member_id, current_cost_ytd, projected_annual_cost,
          trajectory_pct, severity
| order by projected_annual_cost desc
| take 25
```

### Tile 3.5 — Care Gap Volume (by gap type, 24h)
```kql
care_gap_alerts
| where alert_timestamp > ago(24h)
| summarize Count = count() by gap_type, severity
| render barchart kind=stacked
```

---

## Page 4 — Operations (COO)

### Tile 4.1 — ED Boarding (live, 30m)
```kql
ops_capacity_events
| where capacity_type == "ED_BOARDING" and event_timestamp > ago(30m)
| summarize arg_max(event_timestamp, *) by facility_id
| project event_timestamp, facility_id, current_value, threshold_value, utilization_pct
| order by current_value desc
```
**Visual:** Table · bar chart on current_value

### Tile 4.2 — Bed Occupancy (gauge per facility)
```kql
ops_capacity_events
| where capacity_type == "BED_OCCUPANCY" and event_timestamp > ago(15m)
| summarize arg_max(event_timestamp, *) by facility_id
| project facility_id, utilization_pct
```
**Visual:** Multi-row gauge (red ≥ 95, amber 85-95)

### Tile 4.3 — OR Turnover Time (last 4h)
```kql
ops_capacity_events
| where capacity_type == "OR_TURNOVER" and event_timestamp > ago(4h)
| summarize p50 = percentile(current_value, 50), p90 = percentile(current_value, 90)
            by bin(event_timestamp, 30m)
| render timechart
```

### Tile 4.4 — Staffing Gap by Ward (right-now)
```kql
staffing_acuity_events
| where event_timestamp > ago(30m)
| summarize arg_max(event_timestamp, *) by facility_id, ward
| project facility_id, ward, nurse_count, patient_count, acuity_score,
          ratio_actual, ratio_target, staffing_gap
| where staffing_gap >= 1
| order by staffing_gap desc
```

### Tile 4.5 — LOS Outliers (active patients > 7 days)
```kql
ops_capacity_events
| where capacity_type == "LOS" and event_timestamp > ago(15m)
| where current_value > 168  // hours = 7 days
| summarize arg_max(event_timestamp, *) by patient_id = tostring(extra_metadata.patient_id)
| project patient_id, facility_id, ward, los_hours = current_value
| order by los_hours desc
| take 25
```

### Tile 4.6 — Open COO Alerts feed
```kql
coo_alerts
| where alert_timestamp > ago(2h)
| project alert_timestamp, severity, alert_type, ward, alert_text, recommended_action
| order by alert_timestamp desc
| take 30
```

---

## Page 5 — Platform Health (CTO)

### Tile 5.1 — DQ Violations by Source (24h)
```kql
dq_violation_events
| where event_timestamp > ago(24h)
| summarize Violations = sum(violation_count), MaxPct = max(violation_pct)
            by source_table, rule_type
| order by MaxPct desc
```

### Tile 5.2 — Live Event Lag (ingestion latency)
```kql
rti_all_events
| where event_timestamp > ago(15m)
| extend lag_sec = datetime_diff('second', ingestion_time(), event_timestamp)
| summarize p50 = percentile(lag_sec, 50), p90 = percentile(lag_sec, 90),
            p99 = percentile(lag_sec, 99)
            by bin(event_timestamp, 1m), _table
| render timechart
```

### Tile 5.3 — PHI Anomaly Map
```kql
audit_access_events
| where event_timestamp > ago(24h) and anomaly_score > 0.6
| project event_timestamp, user_principal, action_type, resource_accessed,
          geo_country, anomaly_score
| order by anomaly_score desc
| take 50
```

### Tile 5.4 — Model Drift Watchlist
```kql
model_drift_events
| where event_timestamp > ago(24h)
| summarize arg_max(event_timestamp, *) by model_name, feature_name
| where psi_value >= 0.1
| project model_name, model_version, feature_name, psi_value, ks_value, drift_score, severity
| order by psi_value desc
```

### Tile 5.5 — Open CTO Alerts feed
```kql
cto_alerts
| where alert_timestamp > ago(6h)
| project alert_timestamp, severity, alert_type, source_system, alert_text, recommended_action
| order by alert_timestamp desc
| take 30
```

---

## Page 6 — Alert Inbox & MTTR (cross-persona)

### Tile 6.1 — All Open Alerts (last 60m)
```kql
let closures = alert_closure_events
    | where event_timestamp > ago(24h)
    | summarize close_time = minif(event_timestamp, action_type == "RESOLVED") by alert_id;
union
    (provider_alerts | extend persona = "CMO"),
    (payer_alerts    | extend persona = "CFO"),
    (coo_alerts      | extend persona = "COO"),
    (cto_alerts      | extend persona = "CTO")
| where alert_timestamp > ago(1h)
| join kind=leftouter closures on alert_id
| where isnull(close_time)
| project alert_timestamp, persona, severity, alert_type, alert_text, recommended_action
| order by alert_timestamp desc
```

### Tile 6.2 — MTTR Heatmap (persona × alert_type)
```kql
external_table("rti_alert_mttr")
| project persona, alert_type, p50_close_minutes, p90_close_minutes
```
**Visual:** Heatmap

### Tile 6.3 — SLA Burn-down (open count over time)
```kql
let closures = alert_closure_events
    | summarize close_time = minif(event_timestamp, action_type == "RESOLVED") by alert_id;
union provider_alerts, payer_alerts, coo_alerts, cto_alerts
| where alert_timestamp > ago(24h)
| join kind=leftouter closures on alert_id
| extend status = iif(isnotnull(close_time), "RESOLVED", "OPEN")
| summarize Open = countif(status == "OPEN"),
            Resolved = countif(status == "RESOLVED")
            by bin(alert_timestamp, 15m)
| render timechart
```

---

## Parameters (recommended dashboard-level)

Add these parameters in Edit mode → **Manage parameters**:

| Parameter            | Type      | Default     | Used by                            |
|----------------------|-----------|-------------|-------------------------------------|
| `_facility_id`       | string    | (All)       | filter every tile by facility       |
| `_persona`           | string    | (All)       | filter Page 6                       |
| `_severity_min`      | string    | HIGH        | filter alert feed tiles             |
| `_lookback_minutes`  | int       | 60          | parameterize all `ago(...)` calls   |

Replace `ago(1h)` etc. with `ago(_lookback_minutes * 1m)` once parameter exists.

---

## Re-export checklist

After saving the dashboard:

```powershell
# 1. From repo root
python .\export_rti_dashboard.py

# 2. Verify tokens were re-applied
Select-String -Path .\rti_dashboard\healthcare_rti_dashboard.json `
    -Pattern '__KQL_DB_NAME__|__EVENTHOUSE_QUERY_URI__|__KQL_DB_ID__' | Measure-Object

# 3. Commit
git add rti_dashboard\healthcare_rti_dashboard.json TILE_QUERY_PACK.md
git commit -m "RTI: Phase 2 — 6 persona pages + MTTR + alert inbox"
```

`Healthcare_Launcher.ipynb` Cell 12 will redeploy on next demo run — no code changes required.
