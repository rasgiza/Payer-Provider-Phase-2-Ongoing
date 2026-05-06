import json

nb = json.load(open('Healthcare_Launcher.ipynb', 'r', encoding='utf-8'))
cells = nb['cells']
print(f'Total cells: {len(cells)}')
for i, c in enumerate(cells):
    src = c.get('source', [])
    ct = c.get('cell_type', '?')
    # Find CELL N marker in source
    marker = ''
    for line in src[:5]:
        if 'CELL' in line and '—' in line:
            marker = line.strip()[:100]
            break
    if not marker and src:
        marker = src[0].strip()[:100]
    print(f'  [{i}] ({ct}) {marker}')
