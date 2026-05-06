"""Fix the workspace_dir reference in the dashboard deploy code."""
import json

NB_PATH = 'Healthcare_Launcher.ipynb'

with open(NB_PATH, 'r', encoding='utf-8') as f:
    nb = json.load(f)

cell = nb['cells'][14]
src = cell['source']

# Find and fix the workspace_dir line
fixed = 0
for i, line in enumerate(src):
    if 'workspace_dir' in line:
        src[i] = line.replace('workspace_dir', 'ws_dir')
        fixed += 1
        print(f"  Fixed line {i}: {src[i].rstrip()}")

    # Also fix: if os.path.exists(_candidate) -> if _candidate and os.path.exists(_candidate)
    if 'if os.path.exists(_candidate):' in line and 'if _candidate' not in line:
        src[i] = line.replace('if os.path.exists(_candidate):', 'if _candidate and os.path.exists(_candidate):')
        fixed += 1
        print(f"  Fixed line {i}: {src[i].rstrip()}")

# Add ws_dir guard: the os.path.join(ws_dir, ...) will fail if ws_dir is None
# We already have a list comprehension that handles it. Let's make it safer:
for i, line in enumerate(src):
    if 'os.path.join(ws_dir, "..", "rti_dashboard"' in line:
        # Wrap in conditional
        src[i] = line.replace(
            'os.path.join(ws_dir, "..", "rti_dashboard", "healthcare_rti_dashboard.json")',
            'os.path.join(ws_dir, "..", "rti_dashboard", "healthcare_rti_dashboard.json") if ws_dir else ""'
        )
        fixed += 1
        print(f"  Guarded line {i}: {src[i].rstrip()}")

print(f"\nApplied {fixed} fixes")

with open(NB_PATH, 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)
    f.write('\n')

print("Saved.")
