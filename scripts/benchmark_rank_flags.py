#!/usr/bin/env python3
"""Paired, correctness-gated A/B benchmark for Cypress /v2/search or /v1/autocomplete.

Requires only the Python standard library. Start baseline and experimental query
processes against the SAME compiled index on different ports and pass their base
URLs. Each request measures wall time and (when available) server took_ms.

Examples:
  python3 scripts/benchmark_rank_flags.py \
      --baseline http://127.0.0.1:3000 --candidate http://127.0.0.1:3001 \
      --repeats 10 --warmup 2 --out ab.json
  python3 scripts/benchmark_rank_flags.py \
      --baseline http://127.0.0.1:3000 --candidate http://127.0.0.1:3001 \
      --path /v1/autocomplete --queries queries.txt --verify ids
"""

import argparse
import json
import random
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path

DEFAULT_QUERIES = [
    "Los Angeles", "Hollywood Walk of Fame", "New York", "Central Park",
    "San Francisco", "1600 Pennsylvania Avenue", "Union Station",
    "Grand Central Terminal", "Main Street", "London", "London Bridge",
    "Paris", "Place de la République", "Champs Elysées", "Lyon",
    "Berlin Hauptbahnhof", "München", "Frankfurt am Main", "Zürich",
    "Bahnhofstrasse", "Amsterdam", "Roma Termini", "São Paulo",
    "Ciudad de México", "Tokyo", "Shinjuku Station", "Osaka", "서울역",
    "Montréal", "Vancouver", "Sydney Opera House", "Melbourne",
    "Los Angles", "San Fransisco", "Hollywod", "Pari", "Berln",
    "Notre Dame de Paris", "10 Downing Street", "Broadway",
]


def percentile(values, p):
    if not values:
        raise ValueError("cannot calculate percentile of empty input")
    data = sorted(values)
    index = (len(data) - 1) * p / 100.0
    lo = int(index)
    hi = min(lo + 1, len(data) - 1)
    return data[lo] + (data[hi] - data[lo]) * (index - lo)


def stats(values):
    return {
        "samples": len(values),
        "mean_ms": round(statistics.mean(values), 3),
        "p50_ms": round(percentile(values, 50), 3),
        "p95_ms": round(percentile(values, 95), 3),
        "p99_ms": round(percentile(values, 99), 3),
        "max_ms": round(max(values), 3),
    }


def load_queries(file):
    if not file:
        return DEFAULT_QUERIES
    contents = Path(file).read_text(encoding="utf-8")
    if file.endswith(".json"):
        queries = json.loads(contents)
        if not isinstance(queries, list) or not all(isinstance(s, str) for s in queries):
            raise ValueError("JSON query file must contain a list of strings")
    else:
        queries = [line.strip() for line in contents.splitlines()
                   if line.strip() and not line.lstrip().startswith("#")]
    return [q for q in queries if q.strip()]


def request(base_url, path, query, size, timeout):
    url = urllib.parse.urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    separator = "&" if "?" in url else "?"
    url += separator + urllib.parse.urlencode({"text": query, "size": size})
    started = time.perf_counter_ns()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            body = json.load(response)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"request failed: {url}: {exc}") from exc
    wall_ms = (time.perf_counter_ns() - started) / 1_000_000.0
    if not isinstance(body, dict) or not isinstance(body.get("features"), list):
        raise RuntimeError(f"unexpected Cypress response shape from {url}")
    server_ms = body.get("took_ms", body.get("memdb_took_ms"))
    return {
        "wall_ms": wall_ms,
        "server_ms": float(server_ms) if isinstance(server_ms, (int, float)) else None,
        "features": body["features"],
    }


def signature(features, verify):
    if verify == "full":
        # Compare every returned feature in rank order, including geometry,
        # names and confidence; excludes only the response-level timing.
        return features
    return [
        (feature.get("properties", {}).get("id"),
         feature.get("properties", {}).get("layer"))
        for feature in features
    ]


def self_test():
    assert percentile([1, 2, 3, 4], 50) == 2.5
    assert abs(percentile([1, 2, 3, 4], 95) - 3.85) < 1e-10
    data = [{"properties": {"id": "a"}}, {"properties": {"id": "b"}}]
    reverse = list(reversed(data))
    assert signature(data, "ids") != signature(reverse, "ids")
    assert signature(data, "full") == data
    assert stats([1, 2, 3])["p50_ms"] == 2
    print("Self-test passed: quantiles, rank-order comparison, and summaries")


def run(args):
    queries = load_queries(args.queries)
    if not queries:
        raise ValueError("query corpus is empty")
    if args.baseline.rstrip("/") == args.candidate.rstrip("/"):
        raise ValueError("baseline and candidate must be separate server processes")
    rng = random.Random(args.seed)
    servers = {"baseline": args.baseline, "candidate": args.candidate}
    records = []
    mismatches = []

    # Warm EACH process for EACH query, avoiding a cold-only bias. The order
    # is randomized to make cache and frequency effects less asymmetric.
    warm_jobs = [(name, query) for _ in range(args.warmup)
                 for query in queries for name in servers]
    rng.shuffle(warm_jobs)
    for name, query in warm_jobs:
        request(servers[name], args.path, query, args.size, args.timeout)

    jobs = [(query, repeat) for repeat in range(args.repeats) for query in queries]
    rng.shuffle(jobs)
    for seq, (query, repeat) in enumerate(jobs, 1):
        order = ["baseline", "candidate"]
        if rng.getrandbits(1):
            order.reverse()
        pair = {}
        for name in order:
            pair[name] = request(servers[name], args.path, query, args.size, args.timeout)
        left = signature(pair["baseline"]["features"], args.verify)
        right = signature(pair["candidate"]["features"], args.verify)
        identical = left == right
        if not identical:
            mismatches.append({
                "query": query, "repeat": repeat,
                "baseline_ids": signature(pair["baseline"]["features"], "ids"),
                "candidate_ids": signature(pair["candidate"]["features"], "ids"),
            })
        records.append({
            "query": query, "repeat": repeat, "identical": identical,
            "baseline_ms": pair["baseline"]["wall_ms"],
            "candidate_ms": pair["candidate"]["wall_ms"],
            "baseline_server_ms": pair["baseline"]["server_ms"],
            "candidate_server_ms": pair["candidate"]["server_ms"],
        })
        if args.progress and (seq % args.progress == 0 or seq == len(jobs)):
            print(f"{seq}/{len(jobs)} pairs; rank mismatches: {len(mismatches)}", flush=True)

    baseline = [r["baseline_ms"] for r in records]
    candidate = [r["candidate_ms"] for r in records]
    baseline_server = [r["baseline_server_ms"] for r in records
                       if r["baseline_server_ms"] is not None]
    candidate_server = [r["candidate_server_ms"] for r in records
                        if r["candidate_server_ms"] is not None]
    speedups = [r["baseline_ms"] / r["candidate_ms"] for r in records]
    buckets = defaultdict(lambda: [[], []])
    for r in records:
        buckets[r["query"]][0].append(r["baseline_ms"])
        buckets[r["query"]][1].append(r["candidate_ms"])
    per_query = [{
        "query": q,
        "baseline_p50_ms": round(percentile(v[0], 50), 2),
        "candidate_p50_ms": round(percentile(v[1], 50), 2),
        "speedup_of_medians": round(percentile(v[0], 50) / percentile(v[1], 50), 3),
    } for q, v in sorted(buckets.items())]
    report = {
        "baseline_url": args.baseline, "candidate_url": args.candidate,
        "path": args.path, "size": args.size, "queries": len(queries),
        "pairs": len(records), "warmup_per_query": args.warmup,
        "verify": args.verify, "all_ordered_results_identical": not mismatches,
        "mismatch_count": len(mismatches), "mismatches": mismatches[:50],
        "baseline_wall": stats(baseline), "candidate_wall": stats(candidate),
        "baseline_server": stats(baseline_server) if baseline_server else None,
        "candidate_server": stats(candidate_server) if candidate_server else None,
        "median_paired_speedup": round(percentile(speedups, 50), 3),
        "fraction_candidate_faster": round(sum(r["candidate_ms"] < r["baseline_ms"]
                                               for r in records) / len(records), 4),
        "per_query": per_query, "records": records,
    }
    print("\nRESULTS (wall clock, ms)")
    print(f"Baseline:   p50={report['baseline_wall']['p50_ms']:.2f}  "
          f"p95={report['baseline_wall']['p95_ms']:.2f}  "
          f"mean={report['baseline_wall']['mean_ms']:.2f}")
    print(f"Candidate:  p50={report['candidate_wall']['p50_ms']:.2f}  "
          f"p95={report['candidate_wall']['p95_ms']:.2f}  "
          f"mean={report['candidate_wall']['mean_ms']:.2f}")
    print(f"Median paired speedup: {report['median_paired_speedup']:.3f}x; "
          f"candidate faster in {100 * report['fraction_candidate_faster']:.1f}% of pairs")
    print(f"Ordered-result mismatches: {len(mismatches)}/{len(records)}")
    print("\nSlowest baseline queries (p50):")
    for row in sorted(per_query, key=lambda x: -x["baseline_p50_ms"])[:10]:
        print(f"  {row['query'][:35]:35} "
              f"{row['baseline_p50_ms']:8.1f} -> {row['candidate_p50_ms']:8.1f} ms "
              f"({row['speedup_of_medians']:.2f}x)")
    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n",
                                  encoding="utf-8")
        print(f"Full JSON results: {args.out}")
    if mismatches:
        print("WARNING: outputs differ. This optimization is not rank-equivalent.",
              file=sys.stderr)
    return 2 if mismatches else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--baseline", help="Unflagged server base URL, e.g. localhost:3000")
    parser.add_argument("--candidate", help="Flagged server base URL, e.g. localhost:3001")
    parser.add_argument("--path", default="/v2/search",
                        choices=["/v1/search", "/v2/search", "/v1/autocomplete"])
    parser.add_argument("--queries", help="UTF-8 .txt (one per line) or JSON list of strings")
    parser.add_argument("--size", type=int, default=10)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--seed", type=int, default=20261013)
    parser.add_argument("--verify", choices=["ids", "full"], default="full")
    parser.add_argument("--out", help="Write per-request JSON results to this path")
    parser.add_argument("--progress", type=int, default=25,
                        help="Progress line every N paired samples (0 to disable)")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    if not args.baseline or not args.candidate:
        parser.error("--baseline and --candidate are required unless --self-test is used")
    if args.repeats < 1 or args.warmup < 0 or args.size < 1:
        parser.error("repeats and size must be positive; warmup must be nonnegative")
    return run(args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, ValueError) as error:
        print(f"Benchmark failed: {error}", file=sys.stderr)
        sys.exit(1)
