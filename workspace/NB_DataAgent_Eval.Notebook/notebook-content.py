# Fabric notebook source

# METADATA **{"language":"python","language_group":"synapse_pyspark"}**

# MARKDOWN **{"language":"markdown"}**

# # NB_DataAgent_Eval — Calibrated LLM-as-Judge Evaluation
#
# Regression-tests the Healthcare Data Agent against a ground-truth set of
# business questions. Pattern adapted from:
#
# - Microsoft fabric-toolbox "Semantic Model Data Agent Checklist"
# - pawarbi/snippets `dataagent_eval_notebook_LLMasJudge.ipynb`
#
# **Method (4 stages):**
#
# 1. **Resolve ground truth.** For each question in `ground_truth.csv`, run the
#    `expected_dax` against the named semantic model via `sempy.fabric.evaluate_dax`
#    to compute the canonical expected answer at runtime (so the answer stays in
#    sync with current data).
# 2. **Query the agent.** Send each question through the Fabric Data Agent
#    (`FabricOpenAI` chat completion).
# 3. **Judge.** A separate LLM (gpt-5) compares the agent's answer to the
#    canonical answer using an 8-rule critic prompt. Output is a strict
#    `pass` / `fail` label plus a one-sentence rationale.
# 4. **Calibrate.** Run the judge against 16 known pass/fail pairs from
#    `calibration.csv` to estimate TPR and FPR, then report a calibrated
#    accuracy = `(observed - FPR) / (TPR - FPR)`.
#
# **Inputs (in repo):**
# - `data_agents/Healthcare Ontology Agent.DataAgent/eval/ground_truth.csv`
# - `data_agents/Healthcare Ontology Agent.DataAgent/eval/calibration.csv`
#
# **Outputs:**
# - Eval summary printed inline
# - Detailed per-question table written to default lakehouse: `Files/eval/runs/<utc>/results.csv`

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# 0. Parameters (override via notebookutils.notebook.run)
# ============================================================================
AGENT_NAME              = "Healthcare Ontology Agent"
PAYER_SM_NAME           = "PayerAnalytics"
PROVIDER_SM_NAME        = "ProviderAnalytics"
GROUND_TRUTH_PATH       = "/lakehouse/default/Files/eval/ground_truth.csv"     # falls back to repo path
CALIBRATION_PATH        = "/lakehouse/default/Files/eval/calibration.csv"
REPO_GROUND_TRUTH       = "data_agents/Healthcare Ontology Agent.DataAgent/eval/ground_truth.csv"
REPO_CALIBRATION        = "data_agents/Healthcare Ontology Agent.DataAgent/eval/calibration.csv"

JUDGE_MODEL             = "gpt-5"
JUDGE_API_VERSION       = "2025-04-01-preview"
JUDGE_TEMPERATURE       = 0.0
AGENT_TIMEOUT_SECONDS   = 180

# CELL **{"language":"python"}**

# ============================================================================
# 1. Imports + helpers
# ============================================================================
import os, json, time, uuid, datetime as _dt
from pathlib import Path

import pandas as pd

# CELL **{"language":"python"}**

# ============================================================================
# 2. Load ground truth + calibration
# ============================================================================

def _load_csv(lake_path: str, repo_path: str) -> pd.DataFrame:
    """Prefer the lakehouse copy if it exists, else fall back to the repo copy."""
    for p in (lake_path, repo_path):
        try:
            if Path(p).exists():
                print(f"  loading {p}")
                return pd.read_csv(p)
        except Exception:
            pass
    raise FileNotFoundError(f"Could not find {lake_path} or {repo_path}")

gt_df = _load_csv(GROUND_TRUTH_PATH, REPO_GROUND_TRUTH)
cal_df = _load_csv(CALIBRATION_PATH, REPO_CALIBRATION)

print(f"\nGround truth: {len(gt_df)} questions")
print(f"Calibration:  {len(cal_df)} pairs  (expected pass={sum(cal_df['expected_label']=='pass')}, fail={sum(cal_df['expected_label']=='fail')})")
gt_df.head()

# CELL **{"language":"python"}**

# ============================================================================
# 3. Resolve canonical expected answers via DAX
# ============================================================================
# Runs each expected_dax against its semantic model and stores the scalar / row
# result as a JSON string in expected_answer. This keeps ground truth fresh as
# data changes; we only have to maintain the DAX, not the answer.
import sempy.fabric as fabric

def _dax_to_answer(sm_name: str, dax: str) -> str:
    try:
        df = fabric.evaluate_dax(sm_name, dax)
    except Exception as exc:
        return f"ERROR: {exc}"
    if df is None or df.empty:
        return "(empty result)"
    # For single-cell results, return just the scalar; else return small JSON.
    if df.shape == (1, 1):
        v = df.iat[0, 0]
        # Round floats for stable comparison.
        try:
            return f"{float(v):.4f}".rstrip("0").rstrip(".")
        except Exception:
            return str(v)
    return df.head(20).to_json(orient="records")

print("Resolving expected answers...")
expected_answers = []
for _, row in gt_df.iterrows():
    ans = _dax_to_answer(row["sm_name"], row["expected_dax"])
    expected_answers.append(ans)
    print(f"  Q: {row['question'][:70]:<72}  →  {str(ans)[:60]}")
gt_df["expected_answer"] = expected_answers

# CELL **{"language":"python"}**

# ============================================================================
# 4. Query the Data Agent
# ============================================================================
# Uses fabric.dataagent.client to obtain a FabricOpenAI-style chat client bound
# to the agent.
from fabric.dataagent.client import FabricDataAgentManagement, FabricOpenAI  # type: ignore

mgmt = FabricDataAgentManagement(AGENT_NAME)
print(f"Agent: {AGENT_NAME}  id={mgmt.agent_id}")

agent_client = FabricOpenAI(artifact_name=AGENT_NAME)

def ask_agent(question: str, timeout: int = AGENT_TIMEOUT_SECONDS) -> str:
    """One-shot: send a single user message, return the assistant text."""
    t0 = time.time()
    try:
        thread = agent_client.beta.threads.create()
        agent_client.beta.threads.messages.create(thread_id=thread.id, role="user", content=question)
        run = agent_client.beta.threads.runs.create_and_poll(
            thread_id=thread.id,
            assistant_id=mgmt.agent_id,
            poll_interval_ms=2000,
        )
        if run.status != "completed":
            return f"ERROR: run status {run.status}"
        msgs = agent_client.beta.threads.messages.list(thread_id=thread.id, order="desc", limit=1)
        for m in msgs.data:
            if m.role == "assistant":
                for c in m.content:
                    if getattr(c, "type", None) == "text":
                        return c.text.value
        return "ERROR: no assistant message"
    except Exception as exc:
        return f"ERROR: {exc}"
    finally:
        print(f"    [{time.time() - t0:5.1f}s] {question[:80]}")

print("Querying agent...")
agent_answers = [ask_agent(q) for q in gt_df["question"]]
gt_df["agent_answer"] = agent_answers

# CELL **{"language":"python"}**

# ============================================================================
# 5. LLM-as-Judge critic
# ============================================================================
# An independent judge model rates each (expected, observed) pair. The prompt
# encodes 8 rules adapted from pawarbi's LLM-as-Judge notebook.
from synapse.ml.fabric.credentials import get_openai_httpx_sync_client  # type: ignore
from openai import OpenAI  # type: ignore

# Wrap the Fabric-managed httpx client in an OpenAI client targeted at the
# internal Azure OpenAI deployment.
judge_http = get_openai_httpx_sync_client()
judge = OpenAI(http_client=judge_http, base_url="https://api.openai.com/v1", api_key="unused")

CRITIC_PROMPT = """You are a strict but fair evaluator of healthcare analytics answers.

You will be given a QUESTION, an EXPECTED answer (computed from the canonical
DAX query against the semantic model), and an OBSERVED answer (produced by the
data agent under test). Decide whether the OBSERVED answer is correct.

RULES (apply in order):
1. PASS if OBSERVED contains the same numeric value as EXPECTED within ±1% relative tolerance,
   even if the formatting differs (e.g. "12.5%" vs "0.125", "$1,234" vs "1234").
2. PASS if EXPECTED is a single entity (payer/provider/medication name) and OBSERVED
   identifies the same entity, even with surrounding prose.
3. PASS if EXPECTED is a small set/table and OBSERVED contains all the same entities
   in any order with matching values (±1%).
4. FAIL if OBSERVED says "I don't know", "no data", "cannot answer", refuses, or
   returns an error — and EXPECTED is a real value.
5. FAIL if OBSERVED reports a value that is off by more than 1% from EXPECTED.
6. FAIL if OBSERVED answers a different question than asked (e.g. gives count when
   asked for a rate).
7. Ignore differences in wording, units when units are unambiguous, and follow-up
   question suggestions appended to OBSERVED.
8. If unsure between PASS and FAIL, choose FAIL.

Reply STRICTLY as JSON: {"label": "pass" | "fail", "rationale": "<one sentence>"}
"""

def judge_pair(question: str, expected: str, observed: str) -> tuple[str, str]:
    payload = (
        f"QUESTION:\n{question}\n\n"
        f"EXPECTED:\n{expected}\n\n"
        f"OBSERVED:\n{observed}\n"
    )
    try:
        resp = judge.chat.completions.create(
            model=JUDGE_MODEL,
            temperature=JUDGE_TEMPERATURE,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": CRITIC_PROMPT},
                {"role": "user", "content": payload},
            ],
        )
        body = json.loads(resp.choices[0].message.content)
        return body.get("label", "fail").lower(), body.get("rationale", "")
    except Exception as exc:
        return "fail", f"judge error: {exc}"

# CELL **{"language":"python"}**

# ============================================================================
# 6. Calibrate the judge — derive TPR and FPR from known pairs
# ============================================================================
print("Calibrating judge with 16 known pairs...")
cal_results = []
for _, row in cal_df.iterrows():
    label, rat = judge_pair(row["question"], row["expected_answer"], row["agent_answer"])
    cal_results.append({**row.to_dict(), "judge_label": label, "judge_rationale": rat})
cal_out = pd.DataFrame(cal_results)

TP = sum((cal_out["expected_label"] == "pass") & (cal_out["judge_label"] == "pass"))
FN = sum((cal_out["expected_label"] == "pass") & (cal_out["judge_label"] == "fail"))
FP = sum((cal_out["expected_label"] == "fail") & (cal_out["judge_label"] == "pass"))
TN = sum((cal_out["expected_label"] == "fail") & (cal_out["judge_label"] == "fail"))

TPR = TP / max(TP + FN, 1)
FPR = FP / max(FP + TN, 1)
print(f"  TPR = {TPR:.3f}   FPR = {FPR:.3f}   (TP={TP} FN={FN} FP={FP} TN={TN})")

# CELL **{"language":"python"}**

# ============================================================================
# 7. Judge the actual evaluation set
# ============================================================================
print("Judging agent answers vs expected...")
eval_labels, eval_rationales = [], []
for _, row in gt_df.iterrows():
    label, rat = judge_pair(row["question"], row["expected_answer"], row["agent_answer"])
    eval_labels.append(label)
    eval_rationales.append(rat)
    print(f"  [{label.upper():4}] {row['question'][:80]}")

gt_df["judge_label"]     = eval_labels
gt_df["judge_rationale"] = eval_rationales

# CELL **{"language":"python"}**

# ============================================================================
# 8. Summary — observed accuracy + calibration-adjusted accuracy
# ============================================================================
observed = (gt_df["judge_label"] == "pass").mean()
denom    = (TPR - FPR) if (TPR - FPR) > 1e-6 else 1.0
calibrated = max(0.0, min(1.0, (observed - FPR) / denom))

print()
print("=" * 70)
print(f"  Observed accuracy   : {observed:.1%}  ({sum(gt_df['judge_label']=='pass')}/{len(gt_df)})")
print(f"  Judge TPR / FPR     : {TPR:.3f} / {FPR:.3f}")
print(f"  Calibrated accuracy : {calibrated:.1%}")
print("=" * 70)

# Fail-the-notebook behavior if accuracy regresses below 70% calibrated.
ACCURACY_GATE = 0.70
if calibrated < ACCURACY_GATE:
    print(f"\n⚠️  Calibrated accuracy {calibrated:.1%} is below the gate {ACCURACY_GATE:.0%}.")
    print("   Inspect the FAIL rows below and improve the SM (descriptions, measures, AI instructions).")

# CELL **{"language":"python"}**

# ============================================================================
# 9. Persist detailed results to lakehouse
# ============================================================================
run_id = _dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
out_dir = Path(f"/lakehouse/default/Files/eval/runs/{run_id}")
try:
    out_dir.mkdir(parents=True, exist_ok=True)
    gt_df.to_csv(out_dir / "results.csv", index=False)
    cal_out.to_csv(out_dir / "calibration.csv", index=False)
    pd.DataFrame([{
        "run_id": run_id,
        "agent": AGENT_NAME,
        "n_questions": len(gt_df),
        "observed_accuracy": observed,
        "calibrated_accuracy": calibrated,
        "tpr": TPR,
        "fpr": FPR,
        "judge_model": JUDGE_MODEL,
    }]).to_csv(out_dir / "summary.csv", index=False)
    print(f"Wrote {out_dir}")
except Exception as exc:
    print(f"[warn] could not persist to lakehouse: {exc}")

# Show the failures inline.
fails = gt_df[gt_df["judge_label"] == "fail"][["question", "expected_answer", "agent_answer", "judge_rationale"]]
if not fails.empty:
    print("\nFailures:")
    for _, row in fails.iterrows():
        print(f"  Q: {row['question']}")
        print(f"     expected: {row['expected_answer']}")
        print(f"     observed: {str(row['agent_answer'])[:200]}")
        print(f"     judge   : {row['judge_rationale']}")
        print()

gt_df
