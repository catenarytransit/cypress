#!/usr/bin/env python3
"""Benchmark identical Cypress APIs, main vs experimental, with deterministic query order."""
import argparse
import csv
import json
import math
import random
import statistics
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    return round(values[max(0, math.ceil(p * len(values) / 100) - 1)], 3)


def distance_m(a, b):
    lat1, lon1 = a
    lat2, lon2 = b
    latitude = math.radians(lat2 - lat1)
    longitude = math.radians(lon2 - lon1)
    h = math.sin(latitude / 2) ** 2 + (math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2)) * math.sin(longitude / 2) ** 2)
    return 12742000 * math.asin(min(1, math.sqrt(h)))


def fetch(url, case, timeout):
    path = "/v1/autocomplete" if case["mode"] == "autocomplete" else "/v2/search"
    params = {"text": case["text"], "size": 10}
    # Avoid applying a language preference to one engine only.
    params["lang"] = case["language"]
    req = Request(url.rstrip("/") + path + "?" + urlencode(params),
                  headers={"Accept": "application/json", "Accept-Encoding": "identity"})
    start = time.perf_counter_ns()
    try:
        with urlopen(req, timeout=timeout) as response:
            status = response.status
            payload = json.loads(response.read())
        elapsed = (time.perf_counter_ns() - start) / 1e6
        if status != 200 or not isinstance(payload.get("features"), list):
            raise ValueError(f"invalid response {status}")
        features = payload["features"]
        ids = [f["properties"]["id"] for f in features]
        distances = []
        for f in features[:5]:
            lon, lat = f["geometry"]["coordinates"][:2]
            distances.append(round(distance_m(
                (case["lat"], case["lon"]), (lat, lon)), 1))
        return {"status": status, "wall_ms": round(elapsed, 3),
                "took_ms": payload.get("took_ms", payload.get("memdb_took_ms")),
                "ids": ids, "distances_m": distances}
    except (URLError, HTTPError, OSError, TimeoutError, ValueError,
            KeyError, TypeError) as error:
        elapsed = (time.perf_counter_ns() - start) / 1e6
        return {"status": 0, "wall_ms": round(elapsed, 3),
                "error": str(error), "ids": [], "distances_m": []}


def measure(args):
    cases = [json.loads(s) for s in args.workload.read_text(
        encoding="utf-8").splitlines() if s.strip()]
    assert cases and len(set(c["id"] for c in cases)) == len(cases)
    randomizer = random.Random(args.seed)
    observations = []
    for cycle in range(-args.warmup, args.cycles):
        batch = cases[:]
        randomizer.shuffle(batch)
        for case in batch:
            measurement = fetch(args.url, case, args.timeout)
            if cycle >= 0:
                observations.append({"cycle": cycle, "id": case["id"], **measurement})
            elif measurement["status"] != 200:
                raise RuntimeError(f"warmup failed {case['id']}: {measurement.get('error')}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(
        {"engine": args.label, "cases": cases, "observations": observations,
         "cycles": args.cycles, "warmup": args.warmup, "seed": args.seed},
        ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    successes = sum(o["status"] == 200 for o in observations)
    print(f"{args.label} {successes}/{len(observations)} successful responses")
    return 0 if successes == len(observations) else 2


def summarize(args):
    base = json.loads(args.baseline.read_text(encoding="utf-8"))
    exp = json.loads(args.experiment.read_text(encoding="utf-8"))
    if base["cases"] != exp["cases"] or base["cycles"] != exp["cycles"]:
        raise ValueError("same cases and cycles required")
    cases = {c["id"]: c for c in base["cases"]}
    by_engine = {}
    outputs = {}
    details = {}
    for obj in (base, exp):
        label = obj["engine"]
        measurements = obj["observations"]
        ok = [m for m in measurements if m["status"] == 200]
        first = {m["id"]: m for m in reversed(measurements)}
        per_id = {}
        for case_id, case in cases.items():
            rows = [m for m in measurements if m["id"] == case_id]
            good = [m["wall_ms"] for m in rows if m["status"] == 200]
            sample = first[case_id]
            per_id[case_id] = {
                "p50_ms": percentile(good, 50), "p95_ms": percentile(good, 95),
                "ids": sample["ids"],
                "hit1": bool(sample["distances_m"]) and
                    sample["distances_m"][0] <= case["radius_m"],
                "hit5": any(d <= case["radius_m"] for d in sample["distances_m"]),
                "nonempty": bool(sample["ids"]),
                "error": sample.get("error", "")}
        by_engine[label] = {
            "p50_ms": percentile([x["wall_ms"] for x in ok], 50),
            "p95_ms": percentile([x["wall_ms"] for x in ok], 95),
            "p99_ms": percentile([x["wall_ms"] for x in ok], 99),
            "failed_requests": len(measurements) - len(ok),
            "nonempty_cases": sum(c["nonempty"] for c in per_id.values()),
            "hit1_cases": sum(c["hit1"] for c in per_id.values()),
            "hit5_cases": sum(c["hit5"] for c in per_id.values())}
        details[label] = per_id
        outputs[label] = obj
    b = by_engine[base["engine"]]
    e = by_engine[exp["engine"]]
    mismatches = [cid for cid in cases
                  if details[base["engine"]][cid]["ids"] != details[exp["engine"]][cid]["ids"]]
    overlap = []
    for cid in cases:
        a = details[base["engine"]][cid]["ids"]
        z = details[exp["engine"]][cid]["ids"]
        if a:
            overlap.append(len(set(a[:5]) & set(z[:5])) / min(5, len(a)))
    report = {"baseline": b, "experiment": e, "cases": len(cases),
              "ordered_id_mismatches": mismatches,
              "mean_top5_overlap": round(statistics.mean(overlap), 4) if overlap else None,
              "p50_speedup": round(b["p50_ms"] / e["p50_ms"], 3) if e["p50_ms"] else None,
              "p95_speedup": round(b["p95_ms"] / e["p95_ms"], 3) if e["p95_ms"] else None}
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.json").write_text(json.dumps(
        report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    with (args.output / "per_query.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "query", "category", "language", "baseline_p50_ms",
                         "experiment_p50_ms", "baseline_hit1", "experiment_hit1",
                         "baseline_hit5", "experiment_hit5", "baseline_ids",
                         "experiment_ids"])
        for cid, case in cases.items():
            x, y = details[base["engine"]][cid], details[exp["engine"]][cid]
            writer.writerow([cid, case["text"], case["category"], case["language"],
                             x["p50_ms"], y["p50_ms"], x["hit1"], y["hit1"],
                             x["hit5"], y["hit5"], json.dumps(x["ids"]),
                             json.dumps(y["ids"])])
    md = [
        "# Switzerland: Cypress main vs FST/packed-postings experiment", "",
        "Both binaries use identical production settings, except the experiment",
        "selects `CYPRESS_TOKEN_RETRIEVAL_MODE=open-geocode`.",
        "Both run against the **same Scylla database and same compiled index generation**.",
        "", "| Metric | main | experiment |", "|---|---:|---:|"]
    for title, field in [("p50 HTTP latency ms", "p50_ms"),
                         ("p95 HTTP latency ms", "p95_ms"),
                         ("p99 HTTP latency ms", "p99_ms"),
                         ("Failed HTTP requests", "failed_requests"),
                         ("Non-empty queries", "nonempty_cases"),
                         ("Geographically correct top-1", "hit1_cases"),
                         ("Geographically correct top-5", "hit5_cases")]:
        md.append(f"| {title} | {b[field]} | {e[field]} |")
    md.extend(["", f"p50 speedup: {report['p50_speedup']}x",
               f"p95 speedup: {report['p95_speedup']}x",
               f"Ordered result-ID differences: {len(mismatches)} / {len(cases)}",
               f"Mean top-5 ID overlap: {report['mean_top5_overlap']}",
               "", "## Interpretation", "",
               "The upstream open-geocode binary is NOT benchmarked. The experiment",
               "adapts its vocabulary-FST and indexed-postings candidate approach",
               "to Cypress's existing memory-mapped multilingual dictionary.",
               "The existing Cypress scoring, area fanout, and importance model remain active.",
               "Incorrect or missing top results are regressions even when latency improves.",
               "Coordinates are approximate fixtures; review misses before deciding.",
               "Warm end-to-end HTTP latency includes Scylla hydration and serialization.",
               "", "Artifacts include per-query results and full request observations.",
               ""])
    (args.output / "summary.md").write_text("\n".join(md), encoding="utf-8")
    print("\n".join(md))
    return 0 if b["failed_requests"] == 0 and e["failed_requests"] == 0 else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("measure")
    m.add_argument("--url", required=True)
    m.add_argument("--label", required=True)
    m.add_argument("--workload", type=Path, required=True)
    m.add_argument("--output", type=Path, required=True)
    m.add_argument("--cycles", type=int, default=11)
    m.add_argument("--warmup", type=int, default=2)
    m.add_argument("--timeout", type=float, default=30)
    m.add_argument("--seed", type=int, default=20261111)
    s = sub.add_parser("summarize")
    s.add_argument("--baseline", type=Path, required=True)
    s.add_argument("--experiment", type=Path, required=True)
    s.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    return measure(args) if args.cmd == "measure" else summarize(args)


if __name__ == "__main__":
    raise SystemExit(main())
