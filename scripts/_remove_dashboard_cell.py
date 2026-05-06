"""Remove the 'Deploy Real-Time Dashboard' cell from Healthcare_Launcher.ipynb."""
import json

NB_PATH = r"C:\Users\kwamesefah\nypproject\Fabric-Payer-Provider-HealthCare-Demo\Healthcare_Launcher.ipynb"

with open(NB_PATH, "r", encoding="utf-8") as f:
    nb = json.load(f)

# Find the cell
target_idx = None
for i, cell in enumerate(nb["cells"]):
    src = "".join(cell.get("source", []))
    if "CELL 13" in src and "Deploy Real-Time Dashboard" in src:
        target_idx = i
        print(f"Found target cell at index {i} ({len(cell['source'])} lines)")
        print(f"  First line: {cell['source'][0].strip()[:80]}")
        break

if target_idx is None:
    print("ERROR: Could not find the Deploy Real-Time Dashboard cell")
    exit(1)

# Remove it
nb["cells"].pop(target_idx)
print(f"Removed cell {target_idx}. New cell count: {len(nb['cells'])}")

# Verify remaining cells
for i, cell in enumerate(nb["cells"]):
    src = "".join(cell.get("source", []))
    first = src.strip().split("\n")[0][:90]
    print(f"  Cell {i} ({len(cell['source'])} lines): {first}")

with open(NB_PATH, "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)
    f.write("\n")

print("\nDone.")
