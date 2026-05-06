"""Deep-dive: test every measure used by visuals + check is_current column."""
import requests, json
from azure.identity import InteractiveBrowserCredential

WORKSPACE_ID = "d6ed5901-0f1d-4a5c-a263-e5f857169a79"
SM_ID = "da289586-aff9-4aec-8def-7145ce008e3c"

cred = InteractiveBrowserCredential(login_hint="admin@MngEnvMCAP661056.onmicrosoft.com")
token = cred.get_token("https://analysis.windows.net/powerbi/api/.default").token
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
PBI = f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}"

def run_dax(query):
    resp = requests.post(
        f"{PBI}/datasets/{SM_ID}/executeQueries",
        headers=headers,
        json={"queries": [{"query": query}], "serializerSettings": {"includeNulls": True}}
    )
    if resp.status_code == 200:
        result = resp.json()
        err = result.get("results", [{}])[0].get("error")
        if err:
            return f"DAX ERROR: {err}"
        tables = result.get("results", [{}])[0].get("tables", [])
        if tables:
            rows = tables[0].get("rows", [])
            return rows
        return "no tables"
    return f"HTTP {resp.status_code}: {resp.text[:200]}"

print("=" * 60)
print("  MEASURE & DATA DEEP DIVE")
print("=" * 60)

# 1. Check is_current column in dim_patient
print("\n1. dim_patient[is_current] distribution:")
result = run_dax("""
EVALUATE 
SUMMARIZE(dim_patient, dim_patient[is_current], "cnt", COUNTROWS(dim_patient))
""")
print(f"   {result}")

# 2. Test each key measure individually (table-qualified)
measures = [
    ("dim_patient", "Total Patients"),
    ("fact_claim", "Total Claims"),
    ("fact_claim", "Denial Rate"),
    ("fact_claim", "Total Billed"),
    ("fact_claim", "Total Paid"),
    ("fact_encounter", "Total Encounters"),
    ("fact_encounter", "Readmission Rate"),
    ("fact_encounter", "Avg Length of Stay"),
    ("fact_encounter", "Total Charges"),
    ("fact_prescription", "Total Fills"),
    ("fact_prescription", "Total Rx Cost"),
    ("agg_medication_adherence", "Adherent Rate"),
    ("agg_medication_adherence", "Avg PDC Score"),
]

print("\n2. Testing all measures:")
for table, measure in measures:
    result = run_dax(f'EVALUATE ROW("val", CALCULATE({table}[{measure}]))')
    if isinstance(result, list) and result:
        val = result[0].get("[val]", "NULL")
        print(f"   {table}[{measure}] = {val}")
    else:
        print(f"   {table}[{measure}] = ERROR: {result}")

# 3. Check some basic column data
print("\n3. Sample data checks:")

result = run_dax("EVALUATE TOPN(3, VALUES(dim_payer[payer_name]))")
print(f"   Payer names (top 3): {result}")

result = run_dax("EVALUATE TOPN(3, VALUES(dim_provider[specialty]))")
print(f"   Specialties (top 3): {result}")

result = run_dax("EVALUATE TOPN(3, VALUES(fact_claim[claim_status]))")
print(f"   Claim statuses (top 3): {result}")

result = run_dax("EVALUATE TOPN(3, VALUES(dim_sdoh[risk_tier]))")
print(f"   SDOH risk tiers (top 3): {result}")

# 4. Check if dim_date has data
result = run_dax("EVALUATE ROW(\"cnt\", COUNTROWS(dim_date), \"min_yr\", MIN(dim_date[year]), \"max_yr\", MAX(dim_date[year]))")
print(f"\n4. dim_date: {result}")

# 5. Raw row counts for all visual-referenced tables
print("\n5. Row counts (all tables):")
result = run_dax("""
EVALUATE
ROW(
    "dim_patient", COUNTROWS(dim_patient),
    "dim_payer", COUNTROWS(dim_payer),
    "dim_provider", COUNTROWS(dim_provider),
    "dim_sdoh", COUNTROWS(dim_sdoh),
    "dim_date", COUNTROWS(dim_date),
    "dim_medication", COUNTROWS(dim_medication),
    "fact_claim", COUNTROWS(fact_claim),
    "fact_encounter", COUNTROWS(fact_encounter),
    "fact_prescription", COUNTROWS(fact_prescription),
    "agg_medication_adherence", COUNTROWS(agg_medication_adherence)
)
""")
if isinstance(result, list) and result:
    for k, v in result[0].items():
        print(f"   {k.replace('[', '').replace(']', '')}: {v}")

print("\n" + "=" * 60)
