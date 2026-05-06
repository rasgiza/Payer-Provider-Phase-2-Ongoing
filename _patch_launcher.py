"""Patch Healthcare_Launcher.ipynb: ProcessedIngestion -> DirectIngestion"""
import pathlib

p = pathlib.Path('Healthcare_Launcher.ipynb')
content = p.read_text(encoding='utf-8')

count = content.count('ProcessedIngestion')
print(f'Found "ProcessedIngestion" {count} time(s)')

if count > 0:
    content = content.replace('ProcessedIngestion', 'DirectIngestion')
    p.write_text(content, encoding='utf-8')
    print('DONE: Replaced all occurrences with "DirectIngestion"')
else:
    print('Nothing to replace')
