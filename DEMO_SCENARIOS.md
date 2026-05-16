# Demo Scenarios — Presenter Cheat Sheet

Run `NB_RTI_Seed_Scenarios` ~2 minutes before the demo (after the event
simulator is already streaming). The notebook injects four named stories
that the dashboard, Activator, Teams Adaptive Card, and Operations Agent can
all reference deterministically.

## 1. Maria Lopez — 5-day Heart Failure Readmission (CMO)

- **Trigger**: `readmission_events` row with `current_encounter_id='SEED-MARIA-LOPEZ-001'`, `days_since_discharge=5`, `readmission_risk_score=85`, `drg_code='291'`.
- **What lights up**:
  - Activator rule "Readmission within 30d, risk ≥ 70".
  - Triage & Performance → Worst Patients tile (READMISSION stream).
  - Care management adaptive card to the assigned provider in Teams.
- **Agent question**: *"Show me the 30-day readmission risk and prior discharge context for Maria Lopez."*

## 2. Dr. Raj Singh — Fraud Velocity Spike (SIU)

- **Trigger**: 8 `claims_events` rows for the same `provider_id` across 2 facilities in ~8 minutes, each $15k+, tagged `injected_fraud_flags='SEED:RAJ_SINGH|velocity|amount_outlier|geo_anomaly|upcoding'`.
- **What lights up**:
  - Fraud scorer assigns `fraud_score ≥ 80` → CRITICAL tier.
  - Activator rule "Fraud CRITICAL" → SIU Teams channel card.
  - Provider Scorecard tile places Dr. Raj near the top of `composite_risk`.
- **Agent question**: *"List providers with fraud_score ≥ 50 in the last hour and break down by anomaly flag."*

## 3. John Patient — Cross-Stream Patient (Coordinated Care)

- **Trigger**: One row each into `claims_events`, `care_gap_alerts` (HbA1c 120d overdue), `highcost_alerts` ($52k 30d spend, 4 ED visits) — all for the same `patient_id`.
- **What lights up**:
  - `vw_cross_stream_patients(24h)` flags him CRITICAL (3 streams).
  - Worst Patients tile on Triage & Performance.
- **Agent question**: *"Which patients appear in two or more alert streams in the last 24 hours?"*

## 4. Beaumont Royal Oak — Capacity & Staffing Mismatch (COO)

- **Trigger**: 2 `ops_capacity_events` (BED_OCCUPANCY=95% vs threshold 85%, ED_BOARDING=18 vs 10) + 1 `staffing_acuity_events` row (ICU nurse_count=6, patient_count=18, ratio_actual=3.0 vs target 2.0). All tagged `SEED_*`.
- **What lights up**:
  - COO alert path → COO adaptive card.
  - Seeded Demo Scenarios tile (presenter view).
- **Agent question**: *"What is the current bed occupancy and ED boarding at Beaumont Royal Oak?"*

---

## Verification Queries (Eventhouse → Healthcare_RTI_DB)

```kql
// Did seeded events land?
vw_seeded_scenarios(2h)

// Cross-stream patients (John should show up CRITICAL)
vw_cross_stream_patients(24h)

// Provider scorecard (Dr. Raj should rank high)
vw_provider_scorecard(7d)

// MTTD/MTTR — exercise after clicking Acknowledge in Teams
vw_alert_mttr(24h)
```
