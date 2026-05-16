# Healthcare RTI Real-Time Dashboard Guide

## Overview

The **Healthcare RTI Dashboard** is a Fabric KQL Real-Time Dashboard connected to the `Healthcare_RTI_DB` Eventhouse database. It provides real-time operational intelligence across four healthcare domains:

| Page | Domain | Who Uses It | Problem It Solves |
|------|--------|-------------|-------------------|
| **1. Overview** | Pipeline Health | Data Engineers, IT Ops | Silent pipeline failures — stale data goes unnoticed for hours/days |
| **2. Fraud Detection** | Claims Fraud | SIU Investigators, Claims Directors | Fraud caught weeks after payment — pre-pay detection stops the check before it's cut |
| **3. Care Gap Alerts** | HEDIS Quality | Care Managers, Quality/HEDIS Team | Care gaps discovered at year-end — real-time alerts enable same-visit closure |
| **4. High-Cost Trajectory** | Cost Management | Utilization Management, CFO | High-cost patients found in annual reports — early intervention saves $200K+/patient |

## KQL Database Tables

| Table | Source Notebook | Columns |
|-------|----------------|---------|
| `claims_events` | NB_RTI_Event_Simulator | event_id, event_timestamp, event_type, claim_id, patient_id, provider_id, facility_id, payer_id, diagnosis_code, procedure_code, claim_type, claim_amount, latitude, longitude, injected_fraud_flags |
| `adt_events` | NB_RTI_Event_Simulator | event_id, event_timestamp, event_type, patient_id, facility_id, facility_name, admission_type, primary_diagnosis, latitude, longitude, has_open_care_gaps, open_gap_measures |
| `rx_events` | NB_RTI_Event_Simulator | event_id, event_timestamp, event_type, patient_id, provider_id, medication_code, medication_name, drug_class, quantity, days_supply, latitude, longitude |
| `fraud_scores` | NB_RTI_Fraud_Detection | score_id, score_timestamp, claim_id, patient_id, provider_id, facility_id, claim_amount, fraud_score, fraud_flags, risk_tier, latitude, longitude |
| `care_gap_alerts` | NB_RTI_Care_Gap_Alerts | alert_id, alert_timestamp, patient_id, facility_id, facility_name, measure_id, measure_name, gap_days_overdue, alert_priority, alert_text, latitude, longitude |
| `highcost_alerts` | NB_RTI_HighCost_Trajectory | alert_id, alert_timestamp, patient_id, patient_first_name, patient_last_name, facility_id, facility_name, rolling_spend_30d, rolling_spend_90d, claims_count_30d, ed_visits_30d, readmission_count_30d, readmission_flag, risk_tier, cost_trend, latitude, longitude |

---

## Parameters (Filters)

### 1. Time Range (`_timeRange`)

| Setting | Value |
|---------|-------|
| **Label** | `_timeRange` |
| **Parameter type** | Time range |
| **Default value** | Last 24 hours |
| **Show on pages** | Select all |

Creates two internal variables: `_startTime` and `_endTime`. Used in every tile.

### 2. Facility (`_facility`)

| Setting | Value |
|---------|-------|
| **Label** | `_facility` |
| **Parameter type** | Multiple selection |
| **Variable name** | `_facility` |
| **Data type** | string |
| **Show on pages** | Select all |
| **Source** | Query |
| **Query** | `adt_events \| distinct facility_name \| order by facility_name asc` |
| **Add "Select all" value** | Checked |

### 3. Risk Tier (`_riskTier`)

| Setting | Value |
|---------|-------|
| **Label** | `_riskTier` |
| **Parameter type** | Multiple selection |
| **Variable name** | `_riskTier` |
| **Data type** | string |
| **Show on pages** | Select all |
| **Source** | Fixed values |
| **Values** | `CRITICAL`, `HIGH`, `MEDIUM` |
| **Add "Select all" value** | Checked |

### Important: Empty String Handling

When "Select all" is selected, the parameter sends an empty string. All queries use this pattern to handle it:

```kql
| where isempty(_facility) or <actual filter condition>
```

This ensures "Select all" returns all data.

---

## Page 1 — Overview

### Tile 1: Live Claims Count (Stat)

```kql
claims_events
| where event_timestamp between (_startTime .. _endTime)
| where isempty(_facility) or facility_id in (adt_events | where facility_name in~ (_facility) | distinct facility_id)
| count
```

### Tile 2: ADT Event Count (Stat)

```kql
adt_events
| where event_timestamp between (_startTime .. _endTime)
| where isempty(_facility) or facility_name in~ (_facility)
| count
```

### Tile 3: Rx Event Count (Stat)

```kql
rx_events
| where event_timestamp between (_startTime .. _endTime)
| count
```

### Tile 4: Event Volume Trend (Time chart)

```kql
let claims = claims_events | where event_timestamp between (_startTime .. _endTime) | summarize Claims=count() by bin(event_timestamp, 1h);
let adt = adt_events | where event_timestamp between (_startTime .. _endTime) | summarize ADT=count() by bin(event_timestamp, 1h);
let rx = rx_events | where event_timestamp between (_startTime .. _endTime) | summarize Rx=count() by bin(event_timestamp, 1h);
claims
| join kind=fullouter adt on event_timestamp
| join kind=fullouter rx on event_timestamp
| project Timestamp=coalesce(event_timestamp, event_timestamp1, event_timestamp2), Claims, ADT, Rx
| order by Timestamp asc
```

### Tile 5: Claims by Type (Pie/Donut)

```kql
claims_events
| where event_timestamp between (_startTime .. _endTime)
| where isempty(_facility) or facility_id in (adt_events | where facility_name in~ (_facility) | distinct facility_id)
| summarize Count=count() by claim_type
| order by Count desc
```

### Tile 6: ADT Events by Type (Bar)

```kql
adt_events
| where event_timestamp between (_startTime .. _endTime)
| where isempty(_facility) or facility_name in~ (_facility)
| summarize Count=count() by event_type
| order by Count desc
```

### Tile 7: Data Freshness (Stat/Table)

```kql
let c = claims_events | where event_timestamp between (_startTime .. _endTime) | summarize LastClaim=max(event_timestamp);
let a = adt_events | where event_timestamp between (_startTime .. _endTime) | summarize LastADT=max(event_timestamp);
let r = rx_events | where event_timestamp between (_startTime .. _endTime) | summarize LastRx=max(event_timestamp);
c | extend LastADT=toscalar(a | project LastADT), LastRx=toscalar(r | project LastRx)
```

### Claims Volume Map (Map)

```kql
claims_events
| where event_timestamp between (_startTime .. _endTime)
| where isempty(_facility) or facility_id in (adt_events | where facility_name in~ (_facility) | distinct facility_id)
| join kind=leftouter (
    adt_events | summarize arg_max(event_timestamp, facility_name) by facility_id
) on facility_id
| summarize Claims=count(), AvgAmount=round(avg(claim_amount), 2)
    by latitude, longitude, facility_id, Facility=facility_name
| where isnotnull(latitude) and isnotnull(longitude)
```

**Map settings:** Latitude=`latitude`, Longitude=`longitude`, Label=`Facility`, Size=`Claims`

---

## Page 2 — Fraud Detection

### Tile 8: Total Fraud Alerts (Stat)

```kql
fraud_scores
| where score_timestamp between (_startTime .. _endTime)
| where isempty(_facility) or facility_id in (adt_events | where facility_name in~ (_facility) | distinct facility_id)
| where isempty(_riskTier) or risk_tier in~ (_riskTier)
| where fraud_score >= 50
| count
```

### Tile 9: Fraud Score Distribution (Column chart)

```kql
fraud_scores
| where score_timestamp between (_startTime .. _endTime)
| where isempty(_facility) or facility_id in (adt_events | where facility_name in~ (_facility) | distinct facility_id)
| where isempty(_riskTier) or risk_tier in~ (_riskTier)
| summarize Count=count() by Bucket=case(
    fraud_score < 30, "Low (0-30)",
    fraud_score < 50, "Medium (30-50)",
    fraud_score < 70, "High (50-70)",
    "Critical (70+)")
| order by Bucket asc
```

### Tile 10: Top 10 Highest Fraud Scores (Table)

```kql
fraud_scores
| where score_timestamp between (_startTime .. _endTime)
| where isempty(_facility) or facility_id in (adt_events | where facility_name in~ (_facility) | distinct facility_id)
| where isempty(_riskTier) or risk_tier in~ (_riskTier)
| top 10 by fraud_score desc
| project patient_id, claim_id, fraud_score, fraud_flags, risk_tier, score_timestamp
```

### Tile 11: Fraud Trend Over Time (Time chart)

```kql
fraud_scores
| where score_timestamp between (_startTime .. _endTime)
| where isempty(_facility) or facility_id in (adt_events | where facility_name in~ (_facility) | distinct facility_id)
| where isempty(_riskTier) or risk_tier in~ (_riskTier)
| where fraud_score >= 50
| summarize Alerts=count() by bin(score_timestamp, 1h)
| order by score_timestamp asc
```

### Tile 12: Fraud by Flag (Bar chart)

```kql
fraud_scores
| where score_timestamp between (_startTime .. _endTime)
| where isempty(_facility) or facility_id in (adt_events | where facility_name in~ (_facility) | distinct facility_id)
| where isempty(_riskTier) or risk_tier in~ (_riskTier)
| where fraud_score >= 50
| mv-expand flag=parse_json(fraud_flags)
| summarize Count=count() by tostring(flag)
| top 10 by Count desc
```

### Fraud Hotspot Map (Map)

```kql
fraud_scores
| where score_timestamp between (_startTime .. _endTime)
| where isempty(_facility) or facility_id in (adt_events | where facility_name in~ (_facility) | distinct facility_id)
| where isempty(_riskTier) or risk_tier in~ (_riskTier)
| where fraud_score >= 50
| join kind=leftouter (
    adt_events | summarize arg_max(event_timestamp, facility_name) by facility_id
) on facility_id
| summarize AlertCount=count(), AvgScore=round(avg(fraud_score), 2)
    by latitude, longitude, Facility=facility_name
| where isnotnull(latitude) and isnotnull(longitude)
```

**Map settings:** Latitude=`latitude`, Longitude=`longitude`, Label=`Facility`, Size=`AlertCount`

---

## Page 3 — Care Gap Alerts

### Tile 13: Total Care Gap Alerts (Stat)

```kql
care_gap_alerts
| where alert_timestamp between (_startTime .. _endTime)
| where isempty(_facility) or facility_name in~ (_facility)
| where isempty(_riskTier) or alert_priority in~ (_riskTier)
| count
```

### Tile 14: Alerts by Measure (Bar)

```kql
care_gap_alerts
| where alert_timestamp between (_startTime .. _endTime)
| where isempty(_facility) or facility_name in~ (_facility)
| where isempty(_riskTier) or alert_priority in~ (_riskTier)
| summarize Count=count() by measure_name
| order by Count desc
```

### Tile 15: Alerts by Priority (Pie/Donut)

```kql
care_gap_alerts
| where alert_timestamp between (_startTime .. _endTime)
| where isempty(_facility) or facility_name in~ (_facility)
| where isempty(_riskTier) or alert_priority in~ (_riskTier)
| summarize Count=count() by alert_priority
| order by Count desc
```

### Tile 16: Care Gap Alert Trend (Time chart)

```kql
care_gap_alerts
| where alert_timestamp between (_startTime .. _endTime)
| where isempty(_facility) or facility_name in~ (_facility)
| where isempty(_riskTier) or alert_priority in~ (_riskTier)
| summarize Alerts=count() by bin(alert_timestamp, 1h)
| order by alert_timestamp asc
```

### Care Gap Alert Map (Map)

```kql
care_gap_alerts
| where alert_timestamp between (_startTime .. _endTime)
| where isempty(_facility) or facility_name in~ (_facility)
| where isempty(_riskTier) or alert_priority in~ (_riskTier)
| summarize Gaps=count(), Measures=make_set(measure_name)
    by latitude, longitude, Facility=facility_name
| where isnotnull(latitude) and isnotnull(longitude)
```

**Map settings:** Latitude=`latitude`, Longitude=`longitude`, Label=`Facility`, Size=`Gaps`

---

## Page 4 — High-Cost Trajectory

> **Note:** The `highcost_alerts` table includes `facility_id` and `facility_name` directly.
> The HighCost notebook resolves each patient's facility from their **most recent ADT event**
> (any admission type — not just EMERGENCY), so every patient with any hospital interaction
> gets a valid facility. No dashboard-side joins are needed.

### Tile 17: Total High-Cost Alerts (Stat)

```kql
highcost_alerts
| where alert_timestamp between (_startTime .. _endTime)
| where isempty(_riskTier) or risk_tier in~ (_riskTier)
| where isempty(_facility) or facility_name in~ (_facility)
| count
```

### Tile 18: Risk Tier Distribution (Pie/Donut)

```kql
highcost_alerts
| where alert_timestamp between (_startTime .. _endTime)
| where isempty(_riskTier) or risk_tier in~ (_riskTier)
| where isempty(_facility) or facility_name in~ (_facility)
| summarize Count=count() by risk_tier
| order by Count desc
```

### Tile 19: Top 10 Highest Spend (Table)

```kql
highcost_alerts
| where alert_timestamp between (_startTime .. _endTime)
| where isempty(_riskTier) or risk_tier in~ (_riskTier)
| where isempty(_facility) or facility_name in~ (_facility)
| top 10 by rolling_spend_30d desc
| project patient_id, facility_name, risk_tier, rolling_spend_30d, rolling_spend_90d, cost_trend, alert_timestamp
```

### Tile 20: High-Cost Alert Trend (Time chart)

```kql
highcost_alerts
| where alert_timestamp between (_startTime .. _endTime)
| where isempty(_riskTier) or risk_tier in~ (_riskTier)
| where isempty(_facility) or facility_name in~ (_facility)
| summarize Alerts=count() by bin(alert_timestamp, 1h)
| order by alert_timestamp asc
```

### High-Cost Patient Map (Map)

```kql
highcost_alerts
| where alert_timestamp between (_startTime .. _endTime)
| where isempty(_riskTier) or risk_tier in~ (_riskTier)
| where isempty(_facility) or facility_name in~ (_facility)
| summarize Patients=count(), AvgSpend=round(avg(rolling_spend_30d), 2), RiskTier=take_any(risk_tier)
    by latitude, longitude, Facility=facility_name
| where isnotnull(latitude) and isnotnull(longitude) and isnotempty(Facility)
```

**Map settings:** Latitude=`latitude`, Longitude=`longitude`, Label=`Facility`, Size=`Patients`

### High-Cost Facility Detail Table (companion to map)

```kql
highcost_alerts
| where alert_timestamp between (_startTime .. _endTime)
| where isempty(_riskTier) or risk_tier in~ (_riskTier)
| where isempty(_facility) or facility_name in~ (_facility)
| summarize Patients=count(), AvgSpend=round(avg(rolling_spend_30d), 2), RiskTier=take_any(risk_tier)
    by Facility=facility_name
| order by Patients desc
```

---

## Page 5 — Triage & Performance

Demo-hardening page that closes the loop on the four executive personas
(CMO / CFO / COO / SIU). Powered by four KQL view functions defined in
`NB_RTI_Setup_Eventhouse` and seeded by `NB_RTI_Seed_Scenarios`.

### One-Time UI Setup — OneLake Table Shortcuts for Patient/Provider 360

The `vw_alerts_enriched()` function joins live alerts to `dim_patient`,
`dim_provider`, and `dim_facility` in `lh_gold_curated`. These dims must be
exposed in the Eventhouse as **OneLake table shortcuts** (zero-copy, read-only).
The shortcut surface is not reliably scriptable via REST today, so do this once
in the portal:

1. Real-Time Intelligence → open **Healthcare_RTI_Eventhouse → Healthcare_RTI_DB**.
2. Click **+ Get data → OneLake**.
3. Source: pick workspace → **`lh_gold_curated`** (Lakehouse).
4. Select tables: check **`dim_patient`**, **`dim_provider`**, **`dim_facility`**.
5. Shortcut type: **Table shortcut** (NOT folder/file). Keep names unchanged.
6. Finish. The three tables now appear in the KQL DB tree and can be queried
   directly: `dim_patient | take 5`.

Validation:
```kql
vw_alerts_enriched(24h) | take 20
```
If the function errors with `'dim_patient' could not be resolved`, the
shortcut step above was skipped. Re-run it — no need to redeploy the function.

> **Why not external_table or REST?** Eventhouse `.create external table`
> with Delta works but is slower (no caching, no policy push-down) and
> requires lakehouse-specific abfss paths. The REST endpoint for KQL DB
> OneLake shortcuts is preview and inconsistent across tenants. Portal table
> shortcuts are the supported production path today.

### Tile 1 — Worst Patients (≥2 alert streams in 24h)

| Setting | Value |
|---------|-------|
| Visual | Table |
| Query  | `vw_cross_stream_patients(24h) \| take 50` |
| Color rule | `severity_rank` → red on `CRITICAL`, orange on `HIGH` |

Calls out patients showing up in multiple streams (fraud, care gap, high-cost,
readmission, deterioration) in the last 24h. After running
`NB_RTI_Seed_Scenarios`, `SEED:JOHN_PATIENT` should appear here as CRITICAL
(care gap + high-cost + claims = 3 streams).

### Tile 2 — Provider Scorecard (7d)

| Setting | Value |
|---------|-------|
| Visual | Table |
| Query  | `vw_provider_scorecard(7d) \| take 20` |

Composite risk per provider:
`composite_risk = 0.4*fraud_score_p95 + 2*denial_rate_pct + 5*readmit_count`.
Used by SIU for coaching prioritization and by the CMO for outlier review.
After seeding, `SEED:RAJ_SINGH` lands near the top once the fraud scorer runs.

### Tile 3 — MTTD / MTTR Summary (24h)

| Setting | Value |
|---------|-------|
| Visual | Multi-stat |
| Query  | `vw_alert_mttr(24h) \| summarize TotalAlerts=count(), Closed=countif(status=='CLOSED'), Open=countif(status=='OPEN'), AvgMTTRsec=round(avg(toreal(mttr_seconds)),0) \| extend AvgMTTRmin=round(AvgMTTRsec/60.0,1), CloseRatePct=round(100.0*Closed/TotalAlerts,1)` |

Joins `vw_all_alerts` to `alert_closure_events` on `alert_id`. The Acknowledge
button in the Power Automate Teams card (`Healthcare_RTI_NotifyCareTeam`) emits
a row to `alert_closure_events` with `resolved_by`, `action_taken`, and
`mttr_seconds = NOW - alert_timestamp`. Demonstrates the **closed-loop**
acknowledgment story: alert → notify → ack → measurable response.

### Tile 4 — Seeded Demo Scenarios (presenter view — last 4h)

| Setting | Value |
|---------|-------|
| Visual | Table |
| Query  | `vw_seeded_scenarios(4h) \| take 50` |

Filters the raw event tables for rows tagged `SEED:*` / `SEED-*` / `SEED_*`.
Use this as the presenter's "did my seed run?" sanity check before going live.

---

## Closed-Loop Acknowledgment

| Component | Role |
|-----------|------|
| `Healthcare_RTI_NotifyCareTeam` (Power Automate) | Sends adaptive card to Teams when Activator fires |
| Adaptive card **Acknowledge** action | POSTs back to PA flow with `alert_id`, `alert_source_table`, `action_taken`, `resolved_by` (User.Id from Teams context), `resolution_notes` |
| PA flow `Insert row into Eventhouse` | Writes one row to `alert_closure_events` (`mttr_seconds`, `outcome='ACKNOWLEDGED'`) via the same Eventstream Custom Endpoint with `_table='alert_closure_events'` |
| `vw_alert_mttr(7d)` | Surfaces close rate + average MTTR on the dashboard and to the Operations Agent |

This satisfies the demo requirement that every alert has a *human owner and a
measurable time-to-acknowledge*, not just a notification.

---

## Map Tile Configuration

All map tiles use the same setup pattern in the Visual formatting panel:

| Setting | Value |
|---------|-------|
| **Visual type** | Map |
| **Define location by** | Latitude and longitude |
| **Latitude column** | `latitude` |
| **Longitude column** | `longitude` |
| **Label column** | `Facility` (aliased `facility_name`) |
| **Size column** | Count column (`Claims`, `AlertCount`, `Gaps`, `Patients`) |

### What Each Map Shows

| Map | Bubble Size = | Insight |
|-----|---------------|---------|
| **Claims Volume** | Number of claims | Where care delivery is concentrated |
| **Fraud Hotspot** | Number of fraud alerts | Potential billing mills or fraud rings |
| **Care Gap Alert** | Number of gap alerts | Care deserts — where patients miss screenings |
| **High-Cost Patient** | Number of high-cost patients | Where to deploy care coordinators |

---

## Demo Script

| Step | Component | Action | Talking Point |
|------|-----------|--------|---------------|
| 1 | **Overview page** | Show live counts + data freshness | *"The system is ingesting claims, ADT, and Rx events in real time. Data freshness shows the last event was minutes ago — not hours."* |
| 2 | **Claims Volume Map** | Click a large bubble | *"Claims volume concentrates around 2-3 major facilities in the Midwest. This is our network footprint."* |
| 3 | **Fraud Detection page** | Show fraud count + hotspot map | *"185 claims flagged with fraud scores above 0.7. See this cluster near Lansing? That could be a billing mill — all flagged before payment."* |
| 4 | **Facility filter** | Select one facility | *"Let me filter to just Cancer Institute..."* — all tiles update |
| 5 | **Risk Tier filter** | Select HIGH only | *"Now just the HIGH risk tier..."* — watchlist narrows to urgent cases |
| 6 | **Care Gap Alerts page** | Show alerts by measure | *"47 patients have overdue mammograms. The map shows which facilities they're at — care managers can act during the current visit."* |
| 7 | **High-Cost Trajectory page** | Show Top 10 table + map | *"PAT000239 has $75K in 30-day spend at Cancer Institute. The system flagged this today — not next quarter."* |
| 8 | **Time filter** | Narrow to last 1 hour | *"Everything you see updates as new events stream in."* |

---

## Architecture Context

```
RTI Streaming Notebooks
        │
        ▼
   KQL Database (Healthcare_RTI_DB)
        │
        ├──► RTI Dashboard (human monitoring — this guide)
        │
        ├──► Fabric Data Agent / Graph (contextual queries)
        │
        ├──► Foundry AI Agent (natural language recommendations)
        │
        └──► Operations Agent / Activator (automated alerts)
```

> **Streaming detects the signal. The graph connects the context. The AI agent recommends the action. The dashboard gives humans the visibility to verify and intervene.**
