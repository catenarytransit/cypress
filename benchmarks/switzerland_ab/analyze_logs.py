#!/usr/bin/env python3
"""Summarize phase logs without treating nested timers as disjoint CPU time."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import re

ANSI = re.compile(r'\x1b\[[0-9;]*m')
PHASE = re.compile(r'op="([^"]+)" phase="([^"]+)" elapsed_ms=(\d+)')


def analyze(path):
    phases = defaultdict(list)
    plans = fast = threshold_plans = 0
    survivors = []
    snapshots = []
    for raw in path.read_text().splitlines():
        line = ANSI.sub('', raw)
        if 'phase complete' in line:
            match = PHASE.search(line)
            if match:
                micros = re.search(r'elapsed_us=(\d+)', line)
                elapsed_ms = int(micros[1]) / 1000 if micros else int(match[3])
                phases[f'{match[1]}.{match[2]}'].append(elapsed_ms)
        if ': token retrieval plan ' in line:
            plans += 1
            match = re.search(r'token_candidates=(\d+)', line)
            fast += bool(match and int(match[1]))
        if 'blocked fuzzy threshold plan' in line:
            threshold_plans += 1
            match = re.search(r'threshold_survivors=(\d+)', line)
            if match:
                survivors.append(int(match[1]))
        if 'bigram snapshot admission summary' in line:
            match = re.search(r'cache_snapshot_bytes=(\d+)', line)
            if match:
                snapshots.append(int(match[1]))
    return {
        'scope': 'all logged requests including first pass and warmup; timers overlap',
        'token_plans': plans, 'nonempty_token_plans': fast,
        'fallback_plans': plans - fast,
        'max_snapshot_bytes': max(snapshots, default=0),
        'blocked_threshold_plans': threshold_plans,
        'max_threshold_survivors': max(survivors, default=0),
        'phases': dict(sorted({key: {'calls': len(values), 'total_ms': sum(values),
                                    'max_ms': max(values)}
                               for key, values in phases.items()}.items(),
                              key=lambda item: -item[1]['total_ms']))}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', type=Path)
    args = parser.parse_args()
    report = {label: analyze(args.results / f'{label}-server.log')
              for label in ('baseline', 'experiment')}
    (args.results / 'log-analysis.json').write_text(json.dumps(report, indent=2) + '\n')
