"""Dependency-free source, local Markdown link and fixture checks."""
import ast
import json
import re
from pathlib import Path
from urllib.parse import unquote
import xml.etree.ElementTree as ET

root = Path(__file__).resolve().parents[1]
files = [p for p in root.rglob('*') if p.is_file() and not any(
    part.startswith('.') or part in {'runtime', 'dist', '__pycache__'}
    for part in p.relative_to(root).parts)]
for path in files:
    if path.suffix == '.py':
        ast.parse(path.read_text(), filename=str(path))
    elif path.suffix == '.json':
        json.loads(path.read_text())
    elif path.suffix == '.jsonl':
        for line in path.read_text().splitlines():
            json.loads(line)
    elif path.suffix == '.svg':
        ET.parse(path)
    elif path.suffix == '.md':
        for link in re.findall(r'!?\[[^\]]*\]\(([^\s)]+)(?:\s+[^)]*)?\)', path.read_text()):
            target = unquote(link.split('#')[0])
            if target and not re.match(r'[a-z]+:', target) and not (path.parent / target).exists():
                raise AssertionError(f'{path.relative_to(root)}: missing {target}')
print(f'PASS: syntax, fixtures, SVG and relative links ({len(files)} files)')
