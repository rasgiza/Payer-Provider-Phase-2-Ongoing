"""apply_sm_descriptions.py — inject TMDL descriptions into PayerAnalytics and
ProviderAnalytics semantic models.

TMDL represents descriptions as triple-slash comments immediately preceding the
object declaration (table / measure / column). Example:

    /// Total denied claims as a percentage of all claims.
    measure 'Denial Rate' = ...

This utility scans every .tmdl file under each SemanticModel/definition/tables
folder, finds each table / measure / column declaration that is NOT already
preceded by `///` documentation lines, and inserts a business-friendly
description sourced from DESCRIPTIONS below. Items not in the dictionary fall
back to a heuristic generated from the name itself.

Idempotent: re-running the script is a no-op once descriptions exist.

Usage:
    python tools/apply_sm_descriptions.py [<root_path> ...]

If no root paths are given, defaults to the SMs under ``workspace/``.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Description dictionary
# ---------------------------------------------------------------------------
# Keys are either:
#   ('TABLE', 'table_name')                 → table-level description
#   ('MEASURE', 'table_name', "Measure Name")
#   ('COLUMN', 'table_name', 'column_name')
# Special wildcard ``'*'`` for table_name applies to any table containing the
# given column / measure (used for date keys, surrogate keys, audit columns).
# ---------------------------------------------------------------------------

DESCRIPTIONS: dict[tuple, str] = {
    # ---- Tables --------------------------------------------------------
    ("TABLE", "dim_patient"): "Patient master dimension. One row per patient, SCD2-versioned by effective_start_date / effective_end_date. Filter to is_current = 1 for the current view of a patient.",
    ("TABLE", "dim_provider"): "Provider master dimension. One row per clinician (NPI). Use display_name for search and specialty / department for grouping.",
    ("TABLE", "dim_payer"): "Payer (insurance plan) master dimension. payer_type distinguishes Commercial vs Government (Medicare, Medicaid).",
    ("TABLE", "dim_plan"): "Health plan dimension. Distinguishes products (HMO, PPO, EPO, Medicare Advantage) within a payer.",
    ("TABLE", "dim_date"): "Date dimension with day, week, month, quarter, fiscal year attributes. Use date_key (YYYYMMDD integer) to join.",
    ("TABLE", "dim_diagnosis"): "Diagnosis dimension keyed by ICD-10 code. is_chronic flags long-term conditions used for chronic-care reporting.",
    ("TABLE", "dim_medication"): "Medication dimension. drug_class and therapeutic_area are the preferred grouping fields; is_chronic flags maintenance medications.",
    ("TABLE", "dim_sdoh"): "Social Determinants of Health dimension at the ZIP-code level. Always filter by risk_tier ('High'/'Medium'/'Low'), never by raw social_vulnerability_index.",
    ("TABLE", "fact_claim"): "Claim fact. One row per submitted medical claim. Use denial_flag = 1 for denied claims; denial_risk_category is the preferred filter for risk (do not filter on the raw score).",
    ("TABLE", "fact_encounter"): "Encounter fact. One row per inpatient or outpatient visit. readmission_risk_category and encounter_type are the canonical filters.",
    ("TABLE", "fact_prescription"): "Prescription fill fact. One row per dispensed prescription. Join to dim_medication for drug_class.",
    ("TABLE", "fact_diagnosis"): "Patient diagnosis fact. One row per (patient, diagnosis, encounter). diagnosis_type = 'Primary' identifies the principal diagnosis on an encounter.",
    ("TABLE", "fact_premium"): "Member premium payments. Monthly grain by (payer, plan, member). Used in MLR and revenue analyses.",
    ("TABLE", "fact_authorization"): "Prior-authorization fact. One row per auth request with status, decision date, and turnaround time.",
    ("TABLE", "fact_capitation"): "Provider capitation payments. Monthly per-member-per-month (PMPM) payments from payer to provider under value-based contracts.",
    ("TABLE", "fact_appeal"): "Claim appeal fact. One row per appeal of a denied claim, with appeal_outcome and amount_recovered.",
    ("TABLE", "bridge_provider_contract"): "Bridge between providers and payer contracts. Resolves the many-to-many between dim_provider and dim_payer for in-network analysis.",
    ("TABLE", "agg_medication_adherence"): "Pre-aggregated medication adherence per (patient, medication, measurement period). pdc_score is the Proportion of Days Covered; adherent at >= 0.80.",
    ("TABLE", "agg_readmission_by_date"): "Pre-aggregated readmission counts by encounter date and encounter type. Use for daily / monthly readmission trend lines.",
    ("TABLE", "agg_revenue_by_drg"): "Pre-aggregated revenue and margin by DRG (Diagnosis-Related Group). Used for service-line profitability.",
    ("TABLE", "agg_days_in_ar"): "Pre-aggregated Days-in-Accounts-Receivable by (claim date, payer). Lower is better; industry benchmark ~35-45 days.",
    ("TABLE", "agg_appeal_outcomes"): "Pre-aggregated appeal outcomes by payer and denial reason. Used for appeal-success-rate dashboards.",
    ("TABLE", "agg_mlr_by_payer_month"): "Medical Loss Ratio aggregated by payer and year-month. MLR % = medical_paid / premium_collected; ACA minimums are 80% (individual/small group) and 85% (large group).",
    ("TABLE", "agg_hedis_compliance"): "Pre-aggregated HEDIS quality-measure compliance per (payer, plan, measurement year, measure).",
    ("TABLE", "agg_star_rating"): "Per-measure Medicare Star Rating components per (payer, measurement year).",
    ("TABLE", "agg_star_rating_overall"): "Overall Medicare Star Rating (1-5 stars) per (payer, measurement year). Drives plan bonus payments.",
    ("TABLE", "agg_raf_score"): "Risk Adjustment Factor (RAF) score per member-year, used for Medicare Advantage risk adjustment.",
    # ---- Measures: fact_claim ------------------------------------------
    ("MEASURE", "fact_claim", "Total Claims"): "Count of claim records in fact_claim.",
    ("MEASURE", "fact_claim", "Denial Rate"): "Percentage of claims with denial_flag = 1, computed as denied_claims / total_claims.",
    ("MEASURE", "fact_claim", "Total Billed"): "Sum of billed_amount across all claims (gross charges submitted to payers).",
    ("MEASURE", "fact_claim", "Total Paid"): "Sum of paid_amount across all claims (actual payer reimbursement received).",
    ("MEASURE", "fact_claim", "Collection Rate"): "Total Paid divided by Total Billed. Gross collection rate; lower than Net Collection Rate by the contractual write-offs.",
    ("MEASURE", "fact_claim", "At Risk Revenue"): "Sum of billed_amount for claims with denial_risk_category = 'High'. Forward-looking measure of revenue exposed to denial.",
    ("MEASURE", "fact_claim", "Avg Denial Risk"): "Average denial_risk_score across claims. Score is 0-1 from the upstream risk model.",
    ("MEASURE", "fact_claim", "Avg Days to Payment"): "Average days from claim submission to payment receipt.",
    ("MEASURE", "fact_claim", "Net Collection Rate"): "Average of the row-level net_collection_rate field (paid / allowed). Industry benchmark 95-99%.",
    ("MEASURE", "fact_claim", "Appeal Success Rate"): "Won appeals divided by total appeals (Won + Lost). Excludes claims never appealed.",
    ("MEASURE", "fact_claim", "Total Appeal Recovery"): "Sum of appeal_amount_recovered for claims that won an appeal.",
    # ---- Measures: dim_patient -----------------------------------------
    ("MEASURE", "dim_patient", "Total Patients"): "Count of current patients (is_current = 1). SCD2 dimension — only counts the latest version of each patient.",
    ("MEASURE", "dim_patient", "Chronic Patients"): "Distinct count of patients with at least one chronic diagnosis (dim_diagnosis.is_chronic = 1).",
    ("MEASURE", "dim_patient", "Avg Patient Age"): "Average age across the current patient roster.",
    # ---- Measures: dim_payer ------------------------------------------
    ("MEASURE", "dim_payer", "Total Payers"): "Count of active payers (is_active = 1).",
    ("MEASURE", "dim_payer", "Avg Reimbursement Rate"): "Average avg_reimbursement_pct across payers (% of allowed amount that the payer actually reimburses).",
    ("MEASURE", "dim_payer", "Large Network Payers"): "Count of active payers with network_size = 'Large'.",
    ("MEASURE", "dim_payer", "Commercial Payers"): "Count of active payers with payer_type = 'Commercial'.",
    ("MEASURE", "dim_payer", "Government Payers"): "Count of active payers with payer_type = 'Government' (Medicare, Medicaid, TRICARE).",
    # ---- Measures: bridge / aggs ---------------------------------------
    ("MEASURE", "bridge_provider_contract", "Contract Count"): "Total number of provider-payer contract rows in the bridge.",
    ("MEASURE", "bridge_provider_contract", "In-Network Contract Rate"): "Percentage of contract rows with is_in_network = TRUE.",
    ("MEASURE", "agg_medication_adherence", "Adherent Rate"): "Percentage of patient-medication rows where adherence_category = 'Adherent' (PDC >= 0.80).",
    ("MEASURE", "agg_medication_adherence", "Avg PDC Score"): "Average Proportion of Days Covered across patient-medication rows. Range 0-1; >= 0.80 is adherent.",
    ("MEASURE", "agg_mlr_by_payer_month", "MLR %"): "Medical Loss Ratio: medical_paid / premium_collected. ACA requires >= 80% individual/small group and >= 85% large group.",
    ("MEASURE", "agg_mlr_by_payer_month", "Premium Collected"): "Sum of premium dollars collected from members in the period.",
    ("MEASURE", "agg_mlr_by_payer_month", "Medical Paid"): "Sum of claims paid out by the payer in the period.",
    ("MEASURE", "agg_hedis_compliance", "HEDIS Compliance %"): "HEDIS compliance rate: numerator_met / denominator_eligible. Higher is better.",
    ("MEASURE", "agg_hedis_compliance", "HEDIS Eligible Members"): "Sum of denominator_eligible (members in the measure's denominator).",
    ("MEASURE", "agg_hedis_compliance", "HEDIS Numerator Met"): "Sum of numerator_met (members who satisfied the HEDIS measure).",
    ("MEASURE", "agg_star_rating", "Avg Star Score"): "Simple average of star_score across measures (1-5).",
    ("MEASURE", "agg_star_rating", "Weighted Star Score"): "Weight-adjusted average star score = SUM(weighted_score) / SUM(weight). The official CMS Star calculation.",
    ("MEASURE", "agg_star_rating_overall", "Overall Star Score"): "Overall CMS Star Rating for the payer-year (1-5 stars). Drives Medicare Advantage bonus payments.",
    ("MEASURE", "agg_raf_score", "Avg RAF Score"): "Average Risk Adjustment Factor across members. Higher RAF indicates a sicker population and more risk-adjusted revenue.",
    ("MEASURE", "agg_raf_score", "Avg HCC Count"): "Average count of Hierarchical Condition Categories per member.",
    ("MEASURE", "agg_raf_score", "Members with HCCs"): "Distinct count of members with at least one HCC.",
    # ---- Common columns (wildcard table) -------------------------------
    ("COLUMN", "*", "patient_id"): "Patient business key. Stable across SCD2 versions.",
    ("COLUMN", "*", "patient_key"): "Surrogate key for joining to dim_patient. Changes across SCD2 versions.",
    ("COLUMN", "*", "provider_id"): "Provider business key.",
    ("COLUMN", "*", "provider_key"): "Surrogate key for joining to dim_provider.",
    ("COLUMN", "*", "payer_id"): "Payer business key.",
    ("COLUMN", "*", "payer_key"): "Surrogate key for joining to dim_payer.",
    ("COLUMN", "*", "payer_name"): "Display name of the payer.",
    ("COLUMN", "*", "payer_type"): "Payer category: Commercial, Government (Medicare/Medicaid/TRICARE), or Self-Pay.",
    ("COLUMN", "*", "plan_key"): "Surrogate key for joining to dim_plan.",
    ("COLUMN", "*", "medication_key"): "Surrogate key for joining to dim_medication.",
    ("COLUMN", "*", "diagnosis_key"): "Surrogate key for joining to dim_diagnosis.",
    ("COLUMN", "*", "date_key"): "Date key in YYYYMMDD integer form (e.g. 20250515). Join to dim_date.",
    ("COLUMN", "*", "claim_date_key"): "Date the claim was submitted, as YYYYMMDD integer. Join to dim_date.",
    ("COLUMN", "*", "encounter_date_key"): "Date the encounter occurred, as YYYYMMDD integer. Join to dim_date.",
    ("COLUMN", "*", "_load_timestamp"): "ETL load audit timestamp. Internal — hide from end users.",
    ("COLUMN", "*", "is_chronic"): "1 = chronic / maintenance condition or medication; 0 = acute.",
    ("COLUMN", "*", "is_current"): "SCD2 flag — 1 = latest version of this entity; 0 = historical.",
    ("COLUMN", "*", "is_active"): "1 = entity is active and selectable; 0 = retired.",
    # ---- Columns: dim_patient ------------------------------------------
    ("COLUMN", "dim_patient", "first_name"): "Patient first name.",
    ("COLUMN", "dim_patient", "last_name"): "Patient last name.",
    ("COLUMN", "dim_patient", "date_of_birth"): "Patient date of birth.",
    ("COLUMN", "dim_patient", "gender"): "Patient gender (M / F / Other).",
    ("COLUMN", "dim_patient", "address"): "Street address. PII — hide from non-clinical users.",
    ("COLUMN", "dim_patient", "city"): "Patient city.",
    ("COLUMN", "dim_patient", "state"): "Patient state (US 2-letter code).",
    ("COLUMN", "dim_patient", "zip_code"): "Patient 5-digit ZIP. Join to dim_sdoh for community-level SDOH.",
    ("COLUMN", "dim_patient", "phone"): "Patient phone. PII.",
    ("COLUMN", "dim_patient", "email"): "Patient email. PII.",
    ("COLUMN", "dim_patient", "insurance_type"): "Self-reported insurance type (Commercial, Medicare, Medicaid, Self-Pay, Uninsured).",
    ("COLUMN", "dim_patient", "insurance_provider"): "Self-reported insurance plan name.",
    ("COLUMN", "dim_patient", "insurance_policy_number"): "Patient insurance policy / member ID. PII.",
    ("COLUMN", "dim_patient", "age"): "Patient age in years as of report date.",
    ("COLUMN", "dim_patient", "age_group"): "Banded age cohort (e.g. 0-17, 18-44, 45-64, 65+).",
    ("COLUMN", "dim_patient", "effective_start_date"): "SCD2 — date this version of the patient record became current.",
    ("COLUMN", "dim_patient", "effective_end_date"): "SCD2 — date this version was superseded.",
    # ---- Columns: dim_provider ----------------------------------------
    ("COLUMN", "dim_provider", "display_name"): "Provider display name (preferred field for name search).",
    ("COLUMN", "dim_provider", "first_name"): "Provider first name.",
    ("COLUMN", "dim_provider", "last_name"): "Provider last name.",
    ("COLUMN", "dim_provider", "specialty"): "Clinical specialty (Cardiology, Endocrinology, Internal Medicine, ...).",
    ("COLUMN", "dim_provider", "department"): "Hospital department or service line.",
    ("COLUMN", "dim_provider", "npi_number"): "10-digit National Provider Identifier.",
    # ---- Columns: dim_payer -------------------------------------------
    ("COLUMN", "dim_payer", "plan_type"): "Product type within payer: HMO, PPO, EPO, POS, Medicare Advantage, Medicaid Managed Care.",
    ("COLUMN", "dim_payer", "network_size"): "Network size band: Small, Medium, Large.",
    ("COLUMN", "dim_payer", "avg_reimbursement_pct"): "Average percent of allowed amount this payer reimburses.",
    # ---- Columns: dim_date --------------------------------------------
    ("COLUMN", "dim_date", "full_date"): "Calendar date (date type).",
    ("COLUMN", "dim_date", "day_of_week"): "ISO weekday number (1=Monday … 7=Sunday).",
    ("COLUMN", "dim_date", "day_name"): "Weekday name (Monday, Tuesday, …).",
    ("COLUMN", "dim_date", "day_of_month"): "Day of month (1-31).",
    ("COLUMN", "dim_date", "day_of_year"): "Day of year (1-366).",
    ("COLUMN", "dim_date", "week_of_year"): "ISO week number (1-53).",
    ("COLUMN", "dim_date", "month_number"): "Month number (1-12).",
    ("COLUMN", "dim_date", "month_name"): "Month name (January, February, …).",
    ("COLUMN", "dim_date", "quarter"): "Calendar quarter (Q1-Q4).",
    ("COLUMN", "dim_date", "year"): "Calendar year (4-digit).",
    ("COLUMN", "dim_date", "is_weekend"): "1 = Saturday or Sunday; 0 otherwise.",
    ("COLUMN", "dim_date", "is_holiday"): "1 = US federal holiday; 0 otherwise.",
    ("COLUMN", "dim_date", "fiscal_year"): "Fiscal year aligned to the organization's reporting calendar.",
    ("COLUMN", "dim_date", "fiscal_quarter"): "Fiscal quarter within fiscal year.",
    # ---- Columns: dim_diagnosis ---------------------------------------
    ("COLUMN", "dim_diagnosis", "icd_code"): "ICD-10 diagnosis code (e.g. E11.9 for Type 2 diabetes without complications).",
    ("COLUMN", "dim_diagnosis", "icd_description"): "Human-readable ICD-10 description.",
    ("COLUMN", "dim_diagnosis", "icd_category"): "High-level ICD-10 category (Diabetes, Hypertension, COPD, ...).",
    # ---- Columns: dim_medication --------------------------------------
    ("COLUMN", "dim_medication", "rxnorm_code"): "RxNorm clinical drug identifier.",
    ("COLUMN", "dim_medication", "medication_name"): "Brand or generic name as dispensed.",
    ("COLUMN", "dim_medication", "generic_name"): "Generic (non-proprietary) name.",
    ("COLUMN", "dim_medication", "drug_class"): "Pharmacological class (e.g. Statin, ACE Inhibitor, Biguanide).",
    ("COLUMN", "dim_medication", "therapeutic_area"): "Therapeutic area (Cardiovascular, Endocrine, Respiratory, ...).",
    ("COLUMN", "dim_medication", "route"): "Route of administration (Oral, IV, Topical, ...).",
    ("COLUMN", "dim_medication", "form"): "Dosage form (Tablet, Capsule, Solution, ...).",
    ("COLUMN", "dim_medication", "strength"): "Drug strength as labeled (e.g. '500 mg').",
    # ---- Columns: dim_sdoh --------------------------------------------
    ("COLUMN", "dim_sdoh", "zip_code"): "5-digit ZIP code (community).",
    ("COLUMN", "dim_sdoh", "risk_tier"): "SDOH risk tier: High (SVI >= 0.30), Medium (0.15-0.30), Low (< 0.15). Always filter SDOH by this string, NOT the raw SVI.",
    ("COLUMN", "dim_sdoh", "social_vulnerability_index"): "SVI 0-1 (this dataset 0.05-0.53). Do NOT filter on this directly; use risk_tier.",
    ("COLUMN", "dim_sdoh", "poverty_rate"): "Census poverty rate for the ZIP (0-1).",
    ("COLUMN", "dim_sdoh", "food_desert_flag"): "1 = ZIP designated a food desert; 0 otherwise.",
    ("COLUMN", "dim_sdoh", "transportation_score"): "Transportation access score (higher = better access).",
    ("COLUMN", "dim_sdoh", "uninsured_rate"): "Census uninsured rate for the ZIP (0-1).",
    # ---- Columns: fact_claim ------------------------------------------
    ("COLUMN", "fact_claim", "claim_id"): "Claim business key.",
    ("COLUMN", "fact_claim", "claim_status"): "Claim status: Submitted, Pending, Paid, Denied, Appealed.",
    ("COLUMN", "fact_claim", "billed_amount"): "Gross amount billed to payer (charges submitted).",
    ("COLUMN", "fact_claim", "allowed_amount"): "Amount the payer agrees is allowable per contract.",
    ("COLUMN", "fact_claim", "paid_amount"): "Amount actually paid by the payer.",
    ("COLUMN", "fact_claim", "denial_flag"): "1 = claim denied; 0 = paid or pending.",
    ("COLUMN", "fact_claim", "denial_risk_score"): "Model-predicted probability of denial (0-1). Do NOT filter directly; use denial_risk_category.",
    ("COLUMN", "fact_claim", "denial_risk_category"): "Denial risk band: High, Medium, Low. Preferred filter for risk.",
    ("COLUMN", "fact_claim", "primary_denial_reason"): "Free-text reason the claim was denied (e.g. Missing Authorization, Coding Error).",
    ("COLUMN", "fact_claim", "recommended_action"): "System-suggested remediation action for the denial.",
    ("COLUMN", "fact_claim", "days_to_payment"): "Days from claim submission to payment.",
    ("COLUMN", "fact_claim", "net_collection_rate"): "Row-level net collection rate (paid / allowed).",
    ("COLUMN", "fact_claim", "appeal_outcome"): "Appeal result: Won, Lost, In Progress, or blank if not appealed.",
    ("COLUMN", "fact_claim", "appeal_amount_recovered"): "Dollars recovered via a successful appeal.",
    # ---- Columns: fact_encounter --------------------------------------
    ("COLUMN", "fact_encounter", "encounter_id"): "Encounter business key.",
    ("COLUMN", "fact_encounter", "encounter_type"): "Encounter type: Inpatient, Outpatient, Emergency, Observation.",
    ("COLUMN", "fact_encounter", "length_of_stay"): "Length of stay in days (inpatient only).",
    ("COLUMN", "fact_encounter", "total_charges"): "Total billed charges for the encounter.",
    ("COLUMN", "fact_encounter", "total_cost"): "Total cost to deliver the encounter (provider-side).",
    ("COLUMN", "fact_encounter", "readmission_risk_score"): "Model probability of 30-day readmission (0-1). Do NOT filter directly; use readmission_risk_category.",
    ("COLUMN", "fact_encounter", "readmission_risk_category"): "Readmission risk band: High, Medium, Low. Preferred filter for risk.",
}

# Default fallback descriptions by name pattern (regex → description template)
FALLBACK_PATTERNS = [
    (re.compile(r".*_key$"),       "Surrogate join key."),
    (re.compile(r".*_id$"),        "Business key."),
    (re.compile(r"^is_.*"),         "Boolean flag (1 = true, 0 = false)."),
    (re.compile(r"^has_.*"),        "Boolean flag (1 = true, 0 = false)."),
    (re.compile(r".*_flag$"),      "Boolean flag (1 = true, 0 = false)."),
    (re.compile(r".*_date$"),      "Date column."),
    (re.compile(r".*_timestamp$"), "Timestamp column."),
    (re.compile(r".*_count$"),     "Count of records in the underlying grain."),
    (re.compile(r".*_pct$"),       "Percentage (0-1 unless otherwise noted)."),
    (re.compile(r".*_rate$"),      "Rate (0-1)."),
    (re.compile(r"^total_.*"),      "Sum of the underlying measure across the grain."),
    (re.compile(r"^avg_.*"),        "Average of the underlying measure across the grain."),
    (re.compile(r"^median_.*"),     "Median of the underlying measure."),
    (re.compile(r"^p90_.*"),        "90th percentile of the underlying measure."),
    (re.compile(r"^min_.*"),        "Minimum of the underlying measure."),
    (re.compile(r"^max_.*"),        "Maximum of the underlying measure."),
    (re.compile(r"^.*_amount$"),    "Dollar amount."),
    (re.compile(r"^.*_cost$"),      "Dollar cost."),
    (re.compile(r"^.*_score$"),     "Model-produced score."),
]


def fallback_description(name: str, kind: str) -> str:
    """Best-effort description from the name when no dictionary entry exists."""
    nl = name.lower().strip("'")
    for pat, txt in FALLBACK_PATTERNS:
        if pat.match(nl):
            return txt
    pretty = nl.replace("_", " ").strip()
    if kind == "MEASURE":
        return f"Measure: {pretty}."
    if kind == "COLUMN":
        return f"{pretty.capitalize()}."
    return f"{pretty.capitalize()}."


# ---------------------------------------------------------------------------
# TMDL transformation
# ---------------------------------------------------------------------------

DECL_RE = re.compile(
    r"^(?P<indent>\s*)(?P<kind>table|measure|column)\s+(?P<name>'[^']+'|[A-Za-z_][\w]*)"
)


def lookup_description(kind: str, table: str, name: str) -> str:
    name_clean = name.strip("'")
    if kind == "TABLE":
        return DESCRIPTIONS.get(("TABLE", name_clean)) or fallback_description(name_clean, "TABLE")
    key_kind = "MEASURE" if kind == "MEASURE" else "COLUMN"
    return (
        DESCRIPTIONS.get((key_kind, table, name_clean))
        or DESCRIPTIONS.get((key_kind, "*", name_clean))
        or fallback_description(name_clean, key_kind)
    )


def process_file(path: Path) -> int:
    """Insert /// descriptions into a single .tmdl file. Returns insertions made."""
    lines = path.read_text(encoding="utf-8").splitlines(keepends=False)
    out: list[str] = []
    current_table = ""
    insertions = 0

    i = 0
    while i < len(lines):
        line = lines[i]
        m = DECL_RE.match(line)
        if not m:
            out.append(line)
            i += 1
            continue

        indent = m.group("indent")
        kind_raw = m.group("kind")
        name = m.group("name")
        kind = kind_raw.upper()

        if kind == "TABLE":
            current_table = name.strip("'")

        # Already documented?  Check the last non-blank lines we appended.
        already = False
        j = len(out) - 1
        while j >= 0 and out[j].strip() == "":
            j -= 1
        if j >= 0 and out[j].lstrip().startswith("///"):
            already = True

        if not already:
            desc = lookup_description(kind, current_table, name)
            # Split long descriptions on sentence boundaries for readability.
            for chunk in _wrap(desc):
                out.append(f"{indent}/// {chunk}")
            insertions += 1

        out.append(line)
        i += 1

    if insertions:
        path.write_text("\n".join(out) + "\n", encoding="utf-8")
    return insertions


def _wrap(text: str, width: int = 110) -> list[str]:
    words = text.split()
    lines: list[str] = []
    cur = ""
    for w in words:
        if not cur:
            cur = w
        elif len(cur) + 1 + len(w) <= width:
            cur += " " + w
        else:
            lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines


def main(argv: list[str]) -> int:
    if argv:
        roots = [Path(p) for p in argv]
    else:
        here = Path(__file__).resolve().parent.parent
        roots = [
            here / "workspace" / "PayerAnalytics.SemanticModel" / "definition" / "tables",
            here / "workspace" / "ProviderAnalytics.SemanticModel" / "definition" / "tables",
        ]
    total_files = 0
    total_inserts = 0
    for root in roots:
        if not root.exists():
            print(f"[skip] {root} does not exist")
            continue
        for tmdl in sorted(root.glob("*.tmdl")):
            n = process_file(tmdl)
            total_files += 1
            total_inserts += n
            print(f"{'+' if n else '='} {tmdl.relative_to(root.parent.parent.parent)}: +{n} descriptions")
    print(f"\nDone. Processed {total_files} files, inserted {total_inserts} description blocks.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
