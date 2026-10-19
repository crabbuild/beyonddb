#!/usr/bin/env python3
"""Check public documentation links, examples, SVGs and sequence-only Mermaid."""
from collections import Counter
import json
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote, urlsplit
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]


def blocks_and_prose(text):
    blocks, prose, active = [], [], None
    for line_number, line in enumerate(text.splitlines(), 1):
        if line.startswith('```'):
            if active is None:
                active = (line[3:].strip(), line_number, [])
            else:
                blocks.append((active[0], active[1], '\n'.join(active[2])))
                active = None
            prose.append('')
        elif active is not None:
            active[2].append(line)
            prose.append('')
        else:
            prose.append(line)
    if active is not None:
        raise ValueError(f'unclosed code fence at line {active[1]}')
    return blocks, '\n'.join(prose)


def anchors(text):
    _, prose = blocks_and_prose(text)
    found, counts = set(), Counter()
    for heading in re.findall(r'^#{1,6}\s+(.+?)\s*#*$', prose, re.M):
        heading = re.sub(r'\[([^]]+)\]\([^)]+\)', r'\1', heading)
        slug = re.sub(r'[^\w -]', '', heading.lower()).replace(' ', '-')
        found.add(slug if counts[slug] == 0 else f'{slug}-{counts[slug]}')
        counts[slug] += 1
    found.update(re.findall(r'\bid=["\']([^"\']+)', prose))
    return found


def main():
    paths = sorted([ROOT / 'README.md', ROOT / 'AGENTS.md', *ROOT.glob('docs/**/*.md'),
                    *ROOT.glob('crates/*/README.md'), *ROOT.glob('deploy/**/*.md')])
    errors, counts = [], Counter()
    for path in paths:
        relative = path.relative_to(ROOT)
        text = path.read_text()
        try:
            blocks, prose = blocks_and_prose(text)
        except ValueError as error:
            errors.append(f'{relative}: {error}')
            continue
        counts['documents'] += 1
        for language, line, body in blocks:
            if language == 'mermaid':
                significant = [v.strip() for v in body.splitlines() if v.strip() and not v.lstrip().startswith('%%')]
                if not significant or significant[0] != 'sequenceDiagram':
                    errors.append(f'{relative}:{line}: use SVG for non-sequence diagrams')
                counts['sequences'] += 1
            elif language in ('sh', 'bash'):
                checked = subprocess.run(['bash', '-n'], input=body, text=True, capture_output=True)
                if checked.returncode:
                    errors.append(f'{relative}:{line}: {checked.stderr.strip()}')
                counts['shell examples'] += 1
            elif language == 'json':
                try:
                    json.loads(body)
                except ValueError as error:
                    errors.append(f'{relative}:{line}: invalid JSON: {error}')
                counts['JSON examples'] += 1
        for label, destination in re.findall(r'\[([^\]]*)\]\(([^)\s]+)\)', prose):
            parsed = urlsplit(destination.strip('<>'))
            if parsed.scheme or parsed.netloc:
                continue
            target = (path.parent / unquote(parsed.path)).resolve() if parsed.path else path
            counts['local links'] += 1
            if not target.exists():
                errors.append(f'{relative}: missing link target {destination}')
            elif parsed.fragment and target.suffix == '.md':
                if unquote(parsed.fragment) not in anchors(target.read_text()):
                    errors.append(f'{relative}: missing heading {destination}')
    for path in sorted(ROOT.glob('diagram/**/*.svg')):
        try:
            svg = ET.parse(path).getroot()
            if svg.tag != '{http://www.w3.org/2000/svg}svg' or 'viewBox' not in svg.attrib:
                raise ValueError('SVG needs its namespace and a viewBox')
            if path.parent.name in ('operations', 'reference'):
                for name in ('title', 'desc'):
                    if svg.find(f'{{http://www.w3.org/2000/svg}}{name}') is None:
                        raise ValueError(f'SVG needs accessible {name}')
                for element in svg.iter():
                    if element.tag.endswith(('script', 'foreignObject')):
                        raise ValueError('SVG must be self-contained without executable content')
            counts['SVGs'] += 1
        except (ET.ParseError, ValueError) as error:
            errors.append(f'{path.relative_to(ROOT)}: {error}')
    if errors:
        print('\n'.join(errors), file=sys.stderr)
        return 1
    print('Documentation checks passed: ' + ', '.join(f'{v} {k}' for k, v in counts.items()))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
