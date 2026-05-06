"""Add mappingName to Eventhouse destination in Eventstream definition."""
import json

with open("Healthcare_Launcher.ipynb", "r", encoding="utf-8") as f:
    nb = json.load(f)

cells = nb["cells"]
code_cells = [(i, c) for i, c in enumerate(cells) if c["cell_type"] == "code"]
idx, cell = code_cells[13]  # Cell 12 (Deploy RTI)
source = cell["source"]

# Find tableName line and insert mappingName after it
for i, line in enumerate(source):
    if '"tableName": "rti_all_events"' in line:
        new_line = '                "mappingName": "rti_all_events_mapping",\n'
        source.insert(i + 1, new_line)
        print(f"Inserted mappingName at source line {i+1}")
        break
else:
    print("ERROR: tableName line not found")
    exit(1)

with open("Healthcare_Launcher.ipynb", "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=1)

print("DONE - Healthcare_Launcher.ipynb updated")
