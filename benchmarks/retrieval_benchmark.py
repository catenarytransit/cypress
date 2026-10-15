#!/usr/bin/env python3
"""Seeded, sequential HTTP benchmark for Cypress retrieval. Python 3.9+ only."""
import argparse
import csv
import json
import math
import random
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    return values[max(0, math.ceil(p / 100 * len(values)) - 1)]


def load_cases(path):
    seen = set()
    cases = []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        item = json.loads(line)
        key, query = item.get("id"), item.get("text")
        if not isinstance(key, str) or not key or key in seen:
            raise ValueError(f"{path}:{lineno}: invalid/duplicate case id {key!r}")
        if not isinstance(query, str) or not query.strip():
            raise ValueError(f"{path}:{lineno}: invalid query")
        seen.add(key)
        cases.append(item)
    if not cases:
        raise ValueError("workload must contain at least one case")
    return cases


def fetch_one(base_url, endpoint, case, timeout):
    params = {"text": case["text"], "size": case.get("size", 10)}
    for key in ("lang", "bbox", "focus.point.lat", "focus.point.lon",
                "focus.point.weight", "layers"):
        if key in case:
            params[key] = case[key]
    url = base_url.rstrip("/") + endpoint + "?" + urlencode(params)
    request = Request(url, headers={"Accept": "application/json",
                                    "Accept-Encoding": "identity"})
    started = time.perf_counter_ns()
    try:
        with urlopen(request, timeout=timeout) as response:
            status, payload = response.status, response.read()
    except HTTPError as error:
        status, payload = error.code, error.read()
    except (URLError, TimeoutError, OSError) as error:
        return {"status": 0, "error": str(error),
                "wall_ms": (time.perf_counter_ns() - started) / 1e6,
                "server_took_ms": None, "ids": []}
    duration = (time.perf_counter_ns() - started) / 1e6
    try:
        parsed = json.loads(payload)
        if not isinstance(parsed, dict) or not isinstance(parsed.get("features"), list):
            raise ValueError("missing features list")
        ids = [feature["properties"]["id"] for feature in parsed["features"]]
        return {"status": status, "error": "" if status == 200 else f"HTTP {status}",
                "wall_ms": duration,
                "server_took_ms": parsed.get("took_ms", parsed.get("memdb_took_ms")),
                "ids": ids}
    except (ValueError, KeyError, TypeError) as error:
        return {"status": status, "error": f"invalid JSON response: {error}",
                "wall_ms": duration, "server_took_ms": None, "ids": []}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:3000")
    parser.add_argument("--endpoint", choices=["/v1/search", "/v2/search",
                                                "/v1/autocomplete"],
                        default="/v2/search")
    parser.add_argument("--workload", type=Path,
                        default=Path(__file__).parent / "workloads" / "multilingual.jsonl")
    parser.add_argument("--output-dir", type=Path,
                        default=Path("benchmark_results/retrieval"))
    parser.add_argument("--cycles", type=int, default=11)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--seed", type=int, default=20261015)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--compare-baseline", type=Path)
    parser.add_argument("--strict-ids", action="store_true")
    args = parser.parse_args()

    if args.cycles < 1 or args.warmup < 0 or args.timeout <= 0:
        parser.error("cycles and timeout must be positive; warmup must be nonnegative")

    cases = load_cases(args.workload)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(args.seed)
    baseline = None
    if args.compare_baseline is not None:
        baseline = json.loads(args.compare_baseline.read_text(encoding="utf-8"))
        if baseline.get("endpoint") != args.endpoint:
            parser.error("baseline endpoint does not match benchmark endpoint")
        expected_queries = {case["id"]: case["text"] for case in cases}
        if baseline.get("queries") != expected_queries:
            parser.error("baseline workload differs (case IDs or texts)")
        if baseline.get("params") != {case["id"]: {k: v for k, v in case.items() if k != "id"}
                                     for case in cases}:
            parser.error("baseline request parameters differ")

    for _ in range(args.warmup):
        shuffled = cases[:]
        rng.shuffle(shuffled)
        for case in shuffled:
            answer = fetch_one(args.base_url, args.endpoint, case, args.timeout)
            if answer["status"] != 200 or answer["error"]:
                print(f"warmup {case['id']} failed: {answer['error']}", file=sys.stderr)
                return 2

    rows = []
    for cycle in range(args.cycles):
        shuffled = cases[:]
        rng.shuffle(shuffled)
        for case in shuffled:
            result = fetch_one(args.base_url, args.endpoint, case, args.timeout)
            rows.append({"cycle": cycle + 1, "case_id": case["id"],
                         "query": case["text"], **result})

    with (args.output_dir / "requests.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["cycle", "case_id", "query", "status",
                                               "error", "wall_ms", "server_took_ms", "ids"])
        writer.writeheader()
        for row in rows:
            writer.writerow({**row, "ids": json.dumps(row["ids"], ensure_ascii=False)})

    successes = [row for row in rows if row["status"] == 200 and not row["error"]]
    failure_count = len(rows) - len(successes)
    per_case = {}
    recorded = {}
    drift = []
    mismatches = []
    for case in cases:
        ident = case["id"]
        current = [row for row in successes if row["case_id"] == ident]
        samples = [row["wall_ms"] for row in current]
        reference = current[0]["ids"] if current else []
        recorded[ident] = reference
        if len(current) != args.cycles or any(row["ids"] != reference for row in current):
            drift.append(ident)
        if baseline is not None and baseline["result_ids"].get(ident) != reference:
            mismatches.append(ident)
        per_case[ident] = {"successes": len(current), "p50_ms": percentile(samples, 50),
                           "p95_ms": percentile(samples, 95), "p99_ms": percentile(samples, 99),
                           "ids": reference}

    wall = [row["wall_ms"] for row in successes]
    summary = {
        "endpoint": args.endpoint, "seed": args.seed, "cycles": args.cycles,
        "warmup": args.warmup, "cases": len(cases), "failures": failure_count,
        "cycle_drift": drift, "baseline_mismatches": mismatches,
        "p50_ms": percentile(wall, 50), "p95_ms": percentile(wall, 95),
        "p99_ms": percentile(wall, 99), "per_case": per_case,
    }
    (args.output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    (args.output_dir / "baseline.json").write_text(
        json.dumps({"endpoint": args.endpoint, "queries": {c["id"]: c["text"] for c in cases},
                    "params": {c["id"]: {k: v for k, v in c.items() if k != "id"}
                               for c in cases},
                    "result_ids": recorded}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8")
    print("cases={} cycles={} failures={} p50={}ms p95={}ms drift={} mismatches={}".format(
        len(cases), args.cycles, failure_count, summary["p50_ms"],
        summary["p95_ms"], len(drift), len(mismatches)))
    print("Results: " + str(args.output_dir.resolve()))
    return 1 if failure_count or (args.strict_ids and (drift or mismatches)) else 0


if __name__ == "__main__":
    sys.exit(main())
