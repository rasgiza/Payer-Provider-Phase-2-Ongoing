"""
verify_demo_ready.py — Pre-demo health check for Healthcare RTI

Run this 30 minutes before any executive demo. It validates:
  1. Streaming freshness — every input table has events in the last 5 minutes
  2. Scoring freshness — fraud_scores has rows in the last 30 minutes
  3. Threshold coverage — at least one fraud_score >= 50 in the last 1h
  4. Readmission stream landed — readmission_events has rows in last 1h
  5. ADT event_type enum sanity — DISCHARGE and ADMIT both present
  6. Patient ID reconciliation — streaming patient_id values overlap with lakehouse dim_patient

Outputs a PASS/WARN/FAIL line per check and exits non-zero if any FAIL.

Usage (from a Fabric notebook or local with Kusto SDK):
    python verify_demo_ready.py

Set env vars or edit DEFAULTS below:
    KUSTO_QUERY_URI   e.g. https://<eventhouse>.z9.kusto.fabric.microsoft.com
    KQL_DATABASE      Healthcare_RTI_DB
    LAKEHOUSE_SQL_EP  optional — SQL endpoint for dim_patient check
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

DEFAULTS = {
    "KUSTO_QUERY_URI": os.environ.get("KUSTO_QUERY_URI", ""),
    "KQL_DATABASE": os.environ.get("KQL_DATABASE", "Healthcare_RTI_DB"),
}

try:
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
except ImportError:
    print("FAIL: azure-kusto-data not installed. Run: pip install azure-kusto-data")
    sys.exit(2)


def kusto_client() -> KustoClient:
    uri = DEFAULTS["KUSTO_QUERY_URI"]
    if not uri:
        print("FAIL: KUSTO_QUERY_URI not set")
        sys.exit(2)
    kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(uri)
    return KustoClient(kcsb)


def run(client: KustoClient, query: str):
    resp = client.execute(DEFAULTS["KQL_DATABASE"], query)
    rows = list(resp.primary_results[0])
    return rows


CHECKS = [
    {
        "id": "1a",
        "label": "claims_events freshness (last 5 min)",
        "query": "claims_events | summarize n = countif(event_timestamp >= ago(5m)), last = max(event_timestamp)",
        "ok": lambda r: r[0]["n"] > 0,
        "fail_msg": "No claims_events in last 5 minutes — simulator may be stopped",
    },
    {
        "id": "1b",
        "label": "adt_events freshness (last 5 min)",
        "query": "adt_events | summarize n = countif(event_timestamp >= ago(5m)), last = max(event_timestamp)",
        "ok": lambda r: r[0]["n"] > 0,
        "fail_msg": "No adt_events in last 5 minutes",
    },
    {
        "id": "1c",
        "label": "rx_events freshness (last 5 min)",
        "query": "rx_events | summarize n = countif(event_timestamp >= ago(5m)), last = max(event_timestamp)",
        "ok": lambda r: r[0]["n"] > 0,
        "fail_msg": "No rx_events in last 5 minutes",
    },
    {
        "id": "2",
        "label": "fraud_scores scoring freshness (last 30 min)",
        "query": "fraud_scores | summarize n = countif(score_timestamp >= ago(30m)), last = max(score_timestamp)",
        "ok": lambda r: r[0]["n"] > 0,
        "fail_msg": "No fraud_scores in last 30 min — scoring notebook may not have run; threshold queries will return zero",
    },
    {
        "id": "3",
        "label": "fraud_score >= 50 coverage (last 1h)",
        "query": "fraud_scores | where score_timestamp >= ago(1h) | summarize n = countif(fraud_score >= 50), max_score = max(fraud_score)",
        "ok": lambda r: r[0]["n"] > 0,
        "fail_msg": "Zero fraud_scores >= 50 in last hour — agent Detection #1 will return empty; expand window or wait for scorer",
    },
    {
        "id": "4",
        "label": "readmission_events landed (last 1h)",
        "query": "readmission_events | summarize n = countif(event_timestamp >= ago(1h)), last = max(event_timestamp)",
        "ok": lambda r: r[0]["n"] > 0,
        "fail_msg": "No readmission_events in last hour — table or update policy missing; run NB_RTI_Setup_Eventhouse",
    },
    {
        "id": "5",
        "label": "ADT event_type enum sanity (ADMIT + DISCHARGE present)",
        "query": "adt_events | where event_timestamp >= ago(24h) | summarize n = count() by event_type | order by n desc",
        "ok": lambda r: {x["event_type"] for x in r} >= {"ADMIT", "DISCHARGE"},
        "fail_msg": "ADT stream missing ADMIT or DISCHARGE event types — agent Detection #4 will fail",
    },
    {
        "id": "6",
        "label": "Patient ID overlap (streaming vs lakehouse dim_patient)",
        "query": "let streaming_ids = claims_events | where event_timestamp >= ago(24h) | distinct patient_id; let lh_ids = external_table('dim_patient_external') | distinct patient_id; streaming_ids | join kind=inner lh_ids on patient_id | summarize overlap = count()",
        "ok": lambda r: r[0]["overlap"] > 0,
        "fail_msg": "Streaming patient_ids do not overlap with lakehouse dim_patient — demo patient stories will break. Re-run simulator with correct df_patients source.",
        "optional": True,  # external table may not exist
    },
]


def main():
    client = kusto_client()
    print(f"\nverify_demo_ready.py — {datetime.utcnow().isoformat()}Z\n")
    print(f"  KUSTO_QUERY_URI: {DEFAULTS['KUSTO_QUERY_URI']}")
    print(f"  KQL_DATABASE   : {DEFAULTS['KQL_DATABASE']}\n")

    fails = 0
    warns = 0
    for check in CHECKS:
        try:
            rows = run(client, check["query"])
            ok = check["ok"](rows) if rows else False
            if ok:
                print(f"  PASS  [{check['id']}] {check['label']}")
            elif check.get("optional"):
                warns += 1
                print(f"  WARN  [{check['id']}] {check['label']} -- {check['fail_msg']}")
            else:
                fails += 1
                print(f"  FAIL  [{check['id']}] {check['label']} -- {check['fail_msg']}")
        except Exception as e:
            if check.get("optional"):
                warns += 1
                print(f"  WARN  [{check['id']}] {check['label']} -- query error (optional): {e}")
            else:
                fails += 1
                print(f"  FAIL  [{check['id']}] {check['label']} -- query error: {e}")

    print(f"\nSummary: {fails} FAIL, {warns} WARN, {len(CHECKS) - fails - warns} PASS")
    if fails:
        print("\nDO NOT DEMO until FAILs are resolved.\n")
        sys.exit(1)
    if warns:
        print("\nDemo OK to proceed but address WARNs when possible.\n")
        sys.exit(0)
    print("\nAll checks green. Ready to demo.\n")
    sys.exit(0)


if __name__ == "__main__":
    main()
