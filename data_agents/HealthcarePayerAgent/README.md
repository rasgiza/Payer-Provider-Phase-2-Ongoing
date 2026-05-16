# HealthcarePayerAgent

CFO / Payer-Executive Data Agent bound to **`PayerAnalytics.SemanticModel`**
(Direct Lake on `lh_gold_curated`).

> **Deployment**: This folder contains the agent **specification + fewshots**.
> The actual Fabric Data Agent item is created in the workspace UI by importing
> `fewshots.json` against the deployed `PayerAnalytics` semantic model. The
> launcher (`Healthcare_Launcher.ipynb`) deploys the SM; the Data Agent item
> itself is published manually after launcher run #1 (one-time).

## Bound semantic model

- **Name**: `PayerAnalytics`
- **Type**: SemanticModel (Direct Lake)
- **Source**: `lh_gold_curated`
- **Key facts**: `fact_claim`, `fact_premium`, `fact_authorization`,
  `fact_capitation`, `fact_appeal`, `fact_member_month`
- **Key dims**: `dim_patient`, `dim_payer`, `dim_plan`, `dim_provider`, `dim_date`
- **Key aggs**: `agg_mlr_by_payer_month`, `agg_hedis_compliance`,
  `agg_star_rating`, `agg_star_rating_overall`, `agg_raf_score`

## Persona

CFO / Payer Executive. Cares about: MLR, PMPM trends, PA denial leakage,
HEDIS compliance, Star Rating projection, RAF gaps, appeal recovery $.

## Files

- `fewshots.json` — example Q→DAX patterns for the agent
- `system_prompt.md` — orchestration instructions (when to use which measure)
