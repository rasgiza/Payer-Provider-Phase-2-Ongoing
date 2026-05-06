"""Patch Healthcare_Launcher.ipynb: Skip Eventstream definition push if it already exists.
Only push the topology for brand-new Eventstreams."""
import json, pathlib

p = pathlib.Path('Healthcare_Launcher.ipynb')
nb = json.loads(p.read_text(encoding='utf-8'))

# Find the cell that contains the Eventstream wiring logic
target_cell = None
for i, cell in enumerate(nb['cells']):
    if cell.get('cell_type') == 'code':
        src = ''.join(cell['source'])
        if 'Wire Eventstream' in src and 'updateDefinition' in src:
            target_cell = i
            break

if target_cell is None:
    print("ERROR: Could not find the Eventstream wiring cell")
    exit(1)

print(f"Found Eventstream wiring cell at index {target_cell}")

# Get the source as a single string
src = ''.join(nb['cells'][target_cell]['source'])

# 1. Add _es_is_new flag when creating
old_create = '    if not _es_id:\n        try:'
new_create = '    _es_is_new = False\n    if not _es_id:\n        _es_is_new = True\n        try:'
if old_create in src:
    src = src.replace(old_create, new_create, 1)
    print("  [1] Added _es_is_new flag")
else:
    print("  [1] SKIP: Could not find create block")

# 2. Wrap the topology push in an if _es_is_new block
# Find "if _es_id and kql_db_id:" and add condition
old_topology_start = '''    if _es_id and kql_db_id:
        # ── Build Eventstream topology (single landing table) ───────'''
new_topology_start = '''    if _es_id and kql_db_id and _es_is_new:
        # ── Build Eventstream topology (single landing table) ───────
        # Only push definition for NEWLY CREATED Eventstreams.
        # If the Eventstream already exists, its portal-configured
        # Direct Ingestion destination is already correct — don't overwrite it.'''

if old_topology_start in src:
    src = src.replace(old_topology_start, new_topology_start, 1)
    print("  [2] Added _es_is_new guard around topology push")
else:
    print("  [2] SKIP: Could not find topology start")

# 3. Add an else branch for when Eventstream already exists
old_elif = '''    elif _es_id:
        print("  [WARN] KQL Database not found — cannot wire Eventstream topology")
        print("  Eventstream created but empty. Wire manually in the portal.")'''
new_elif = '''    elif _es_id and kql_db_id and not _es_is_new:
        # Eventstream already exists with correct configuration — don't overwrite
        _es_url = f"https://app.fabric.microsoft.com/groups/{workspace_id}/eventstreams/{_es_id}"
        print(f"\\n  [OK] Eventstream already configured — skipping topology push")
        print(f"  Eventstream URL: {_es_url}")
        print()
        print("  The Eventstream destination (Direct Ingestion → rti_all_events) is")
        print("  already configured. Run the RTI pipeline to start data flow.")
        print()
        print("  ┌──────────────────────────────────────────────────────────────┐")
        print("  │  NEXT: Copy the Eventstream connection string (if needed)    │")
        print("  │  and paste into the next cell to start the RTI pipeline.     │")
        print("  └──────────────────────────────────────────────────────────────┘")
    elif _es_id:
        print("  [WARN] KQL Database not found — cannot wire Eventstream topology")
        print("  Eventstream created but empty. Wire manually in the portal.")'''

if old_elif in src:
    src = src.replace(old_elif, new_elif, 1)
    print("  [3] Added else branch for existing Eventstream")
else:
    print("  [3] SKIP: Could not find elif block")

# Write back as individual lines (ipynb format)
nb['cells'][target_cell]['source'] = [line + '\n' for line in src.split('\n')[:-1]] + [src.split('\n')[-1]]

p.write_text(json.dumps(nb, indent=1, ensure_ascii=False), encoding='utf-8')
print("\nDONE: Healthcare_Launcher.ipynb patched.")
print("  - New Eventstream: creates + pushes full topology definition")
print("  - Existing Eventstream: skips definition push, preserves portal config")
