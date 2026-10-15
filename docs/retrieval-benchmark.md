# Retrieval benchmark baseline (PR 1)

This change does **not** modify candidate generation, scoring, name ingestion or index layout.
Debug tracing now records one `query retrieval baseline` event per completed `search_place_ids` request.

## Run

Start the search server with `RUST_LOG=info,cypress::query::timing=debug` (debug tracing
is expensive; compare production latency separately with normal logging).

```sh
RUST_LOG=info,cypress::query::timing=debug cargo run --release --bin query -- --memdb-dir ./data/compiled 2>&1 | tee /tmp/cypress-retrieval.log
```

Run randomized cycles with the stdlib-only Python runner:

```sh
python3 benchmarks/retrieval_benchmark.py --base-url http://127.0.0.1:3000 --cycles 11 --warmup 2 --seed 20261015 --output-dir benchmark_results/retrieval_legacy
```

After modifying the engine, compare ordered result IDs against the old run:

```sh
python3 benchmarks/retrieval_benchmark.py --cycles 11 --warmup 2 --seed 20261015 --compare-baseline benchmark_results/retrieval_legacy/baseline.json --strict-ids --output-dir benchmark_results/retrieval_new
```

The files `requests.csv`, `summary.json` and `baseline.json` are written to `--output-dir`.
Summaries use nearest-rank percentiles. Requests are sequential, shuffled each cycle,
and include exact names, addresses, typos, officially multilingual names,
native non-Latin scripts, mixed-script phrases, and ambiguous global names.
Failures return a nonzero status. `--strict-ids` additionally fails on ordered
result-ID drift across cycles or compared against the baseline.

## Instrumentation semantics

| Counter | Definition |
| --- | --- |
| `cold_bigram_posting_entries` | Logical entries visited by scanning every query gram with an empty cache |
| `posting_entries_examined` | Actual posting entries visited in uncached bigram postings |
| `posting_entries_avoided_by_cache` | Cold total minus examined; not actual CPU/memory saved |
| `cache_restored_string_ids` | Touched IDs after restoring a cached count snapshot, before uncached traversal |
| `touched_string_count` | Distinct string IDs participating in current match |
| `largest_bigram_posting_entries` | Largest individual query posting |
| `name_mapping_entries_examined` | Visited string-to-place mappings, including filtered links |
| `street_mapping_entries_examined` | Visited string-to-street-group mappings |
| `dp_candidates` | Full structured scoring candidates |
| `total_sift4_calls` | Query-local SIFT4 calls with DEBUG timing enabled |

Some existing counters are retained to allow historical comparisons.
`search_elapsed_ms` covers only the synchronous in-memory search,
whereas the HTTP benchmark includes worker queueing, database hydration, serialization, and transport.
The current postings are flat `u32` arrays: there are no block decode/seek counters yet.

Keep benchmark results out of git. The repository's `benchmark_results/` path is generated locally.

## Validation

- Run `cargo test --bin query` and `cargo fmt --all -- --check`.
- Run `python3 -m py_compile benchmarks/retrieval_benchmark.py`.
- Run the benchmark on the same compiled MemDB and same server flags across variants.
- Record p50/p95/p99, debug posting counts, result-ID differences, and CPU load.
- Do not interpret cache-avoided postings as guaranteed wall-clock speedup.
