# Production Gaps — Honest Inventory

This repo is a **demo built on synthetic data**. The architecture (Eventstream → KQL DB → Activator → Power Automate → Teams, plus Fabric Data Agent + Operations Agent) is production-shaped, but several deliberate shortcuts exist to keep the demo runnable on a single workspace by a single engineer. This document lists every known gap, classified by severity, plus a 3-sprint roadmap to close them.

**Read this before any production conversation.** Do not claim the demo is production-ready — claim that the *pattern* is production-ready and the *data plane* needs the work below.

---

## 🔴 Blockers — Must Fix Before Real Patients/Claims

| # | Gap | Why It Blocks Production | Effort |
|---|-----|--------------------------|--------|
| B1 | **Simulator is the data source** | `NB_RTI_Event_Simulator` generates synthetic JSON and pushes to Eventstream via `EventHubProducerClient`. In production, sources are HL7v2/FHIR feeds, X12 837 claims, NCPDP SCRIPT, and ADT from HIE. Need adapters (Mirth, Rhapsody, Azure Health Data Services) per source. | L |
| B2 | **No MPI / EMPI** | `patient_id` is deterministic in the simulator because it reads `df_patients` from `lh_gold_curated.dim_patient`. Real-world patient IDs differ per source (EHR, payer, pharmacy). Need an enterprise master patient index (Azure Health Data Services Patient resource + matching). | L |
| B3 | **No PHI handling / de-identification** | Synthetic data is free of PHI. Real data requires HIPAA-compliant storage (encryption at rest+transit, audit logs, BAA), tokenization for analytics, and Purview classifications on every column. | M |
| B4 | **No dead-letter queue (DLQ)** | If the KQL update policy fails (malformed JSON, schema drift), rows are dropped silently. Need a `rti_dlq` table + alert on non-zero ingest. | S |
| B5 | **No schema evolution policy** | Adding a column today requires `.alter-merge table` + `.alter-merge ingestion mapping` + redeploying every notebook. No contract registry, no consumer impact analysis. Need a schema registry (Confluent or Purview) and versioned event types. | M |
| B6 | **Single region / no DR** | Eventhouse, Eventstream, Activator are all in one Fabric capacity. No geo-replication, no failover runbook, no RPO/RTO defined. | M |

---

## 🟡 Operational — Will Hurt You in Week 2

| # | Gap | Symptom | Effort |
|---|-----|---------|--------|
| O1 | **Scoring notebooks use overwrite mode** | `fraud_scores`, `care_gap_alerts`, `highcost_alerts` are written with `mode("overwrite")` each run. History is lost. Production needs append + scoring_run_id. | S |
| O2 | **No model registry** | The fraud scorer is rule-based KQL. When you swap in an ML model, there's no versioning, no A/B, no rollback. Need MLflow / Fabric model registry. | M |
| O3 | **No SLA monitoring** | No alert when Eventstream backlog > N, when scoring lag > 10 min, or when Activator stops firing. Need an SRE workbook on the Eventhouse. | S |
| O4 | **No replay / backfill** | If the scorer has a bug, you cannot replay 24h of `claims_events` through it. Need an idempotent scoring job that reads from a watermark. | M |
| O5 | **Power Automate flow is fragile** | A single flow (`Healthcare_RTI_NotifyCareTeam`) handles all routing. Owner is one person. No version control. Migrate to Logic Apps Standard in source control. | M |
| O6 | **No load testing** | Simulator pushes ~10 events/sec. Real Corewell volume is 100–500/sec sustained, 2000/sec peak. Have not validated Eventstream throughput or KQL update policy lag at that rate. | M |

---

## 🟢 Phase-2 Roadmap Items

| # | Gap | Decision | Effort |
|---|-----|----------|--------|
| P1 | **Batch-on-interval scoring** | Scoring notebooks run on a 5-min trigger. Moving to true streaming detection (KQL `update policy` rules that compute risk inline) reduces latency from minutes to seconds for selected rules. Plan migration rule-by-rule. | L |
| P2 | **Agents run on Foundry, not Fabric** | The orchestrator agent lives on Microsoft Foundry (Operations Agent v28). Fabric Data Agent is a thin grounding layer. Long-term, consolidate on one runtime when Fabric agent capabilities mature. | M |
| P3 | **No FinOps** | No tagging strategy, no cost allocation per workload, no Eventhouse hot/cold tier policy beyond defaults. | S |
| P4 | **Unrouted event types from simulator** | The simulator emits 12 additional event types that currently land in `rti_all_events` only (no typed table): `quality_metric_events`, `denial_adjudication_events`, `ar_snapshot_events`, `auth_lifecycle_events`, `clinical_deterioration_events`, `mortality_hai_events`, `net_revenue_variance_events`, `ops_capacity_events`, `staffing_acuity_events`, `dq_violation_events`, `audit_access_events`, `model_drift_events`. Each needs (a) a typed table + streaming policy, (b) an `Extract<X>()` KQL function, (c) an update policy, (d) JSON ingestion mapping, (e) a Data Agent table reference, (f) optionally an Activator rule. Pattern is established in `NB_RTI_Setup_Eventhouse` — apply it 12 more times. | M |
| P5 | **fraud_score scale (0–100 vs 0–1)** | We locked the scale at **0–100** for this demo (Option A). Option B (switch to 0–1 / 0.0–1.0 probability) is deferred. If switched later, every threshold in the dashboard JSON, Activator rules, Operations Agent instructions, and README must change together — they are currently consistent at 0–100. | S |

---

## Honest Notes on the Demo Mechanics

These are not "gaps" but **facts you must disclose if asked**:

1. **Patient ID determinism is engineered.** The simulator reads `df_patients` from `lh_gold_curated.dim_patient` so that streaming `patient_id` values match lakehouse dimension keys. In production, sources are independent — patient matching is a hard problem (see B2).
2. **The 0–100 fraud_score is rule-based.** It is `velocity(0–30) + amount(0–25) + geo(0–25) + upcoding(0/20)`. It is not an ML model. Risk tiers are `CRITICAL ≥ 50, HIGH ≥ 30, MEDIUM ≥ 15`. The label "ML scoring" in marketing slides should read "rules-based scoring with ML upgrade path".
3. **Readmission detection is upstream-scored.** The simulator emits `READMISSION_DETECTED` events with `readmission_risk_score` already populated (synthetic LACE-style). In production, this score is computed by a separate model on ADT discharge + admit pairing.
4. **ADT event_type enum is exactly `ADMIT / DISCHARGE / TRANSFER / OBSERVATION`.** Earlier drafts of the agent instructions mentioned `ED_ADMIT` — that value does not exist in the data and has been removed.
5. **Operations Agent is not real-time-aware.** It answers questions over the KQL tables on demand. It does not push proactive alerts. Activator handles push notifications.

---

## 3-Sprint Production Roadmap

**Sprint 1 — Source integrity** (B1, B2, B3, B4)
- Replace simulator with one real adapter (start with HL7 ADT via Mirth → Eventstream)
- Stand up Azure Health Data Services Patient resource for MPI
- Add `rti_dlq` table + Activator alert on backlog
- Run a privacy review and tag PHI columns in Purview

**Sprint 2 — Data contracts** (B5, O1, O2, P4 partial)
- Adopt a schema registry; publish v1 contracts for `claims_events`, `adt_events`, `rx_events`, `readmission_events`
- Switch scoring tables to append + `scoring_run_id`
- Stand up MLflow on Fabric for the fraud and readmission models
- Route 3 of the 12 unrouted event types (pick highest business value: `denial_adjudication_events`, `auth_lifecycle_events`, `clinical_deterioration_events`)

**Sprint 3 — Operations + governance** (B6, O3, O4, O5, O6, P3)
- SRE workbook with backlog, ingest lag, scoring lag, Activator firings/hour
- Idempotent backfill job with watermark
- Move Power Automate flow to Logic Apps Standard + source control
- Load test to peak volume + document RPO/RTO
- FinOps tagging + Eventhouse retention/tier policy

**(Optional) Sprint 4 — Real-time inference** (P1, P2)
- Migrate 2 highest-value rules from batch scoring to KQL `update policy` inline scoring
- Decide Foundry vs Fabric agent consolidation

---

*Last updated: alongside the demo-ready hardening pass that locked `fraud_score` at 0–100, added `readmission_events` end-to-end, and rewrote the Operations Agent with hard anti-fabrication rules.*
