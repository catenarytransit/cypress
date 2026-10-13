# Opt-in ranking experiments for Cypress

These experiments default to the current implementation. No MemDB recompile is
required. All flags are read once per query process; restart between variants.

| Environment variable | Default | Experiment |
| --- | --- | --- |
| `CYPRESS_NGRAM_RETRIEVAL` | `sparse` | `merge`: multiway merge of the existing sorted postings; bypasses the n-gram snapshot cache and writes counts only for strings meeting the shared-gram threshold |
| `CYPRESS_DISABLE_NGRAM_SNAPSHOTS` | `0` | `1`: skip building and admitting new sparse bigram snapshots; isolates the 134-188 ms cold snapshot cost seen in the production logs |
| `CYPRESS_PREPARED_AREA_SCORING` | `0` | `1`: tokenize each area name only once per area-cache miss, not once per phrase |
| `CYPRESS_CACHE_STREET_COMPONENTS` | `0` | `1`: cache street/phrase score options by shared string ID within a query |
| `CYPRESS_BORROW_NAME_SPANS` | `0` | `1`: directly borrow canonical space-joined name substrings instead of recreating their bytes |

## Build and launch

```bash
cargo build --release --bin query

# Terminal 1: control
./target/release/query --listen 127.0.0.1:3000 \
  --scylla-url 127.0.0.1 --memdb-dir ./data/compiled

# Terminal 2: candidate (run one variant at a time for attribution)
CYPRESS_PREPARED_AREA_SCORING=1 ./target/release/query \
  --listen 127.0.0.1:3001 --scylla-url 127.0.0.1 \
  --memdb-dir ./data/compiled
```

To test a different change, restart the second server with ONE of:

```bash
CYPRESS_NGRAM_RETRIEVAL=merge
CYPRESS_DISABLE_NGRAM_SNAPSHOTS=1
CYPRESS_CACHE_STREET_COMPONENTS=1
CYPRESS_BORROW_NAME_SPANS=1
```

Then try all the winning flags together, **only after each experiment passes
rank-equivalence testing**.

## Python A/B benchmark

```bash
python3 scripts/benchmark_rank_flags.py \
  --baseline http://127.0.0.1:3000 \
  --candidate http://127.0.0.1:3001 \
  --path /v1/autocomplete \
  --repeats 10 --warmup 2 --verify full \
  --out ranking-results.json
```

`/v1/autocomplete` isolates the memory-mapped lexical ranking more closely than
`/v2/search`. Also test `/v2/search` before promoting an experiment. The script
randomizes the order of requests within each pair, reports wall-clock p50/p95,
paired speedup, and mismatch counts, and exits with code `2` if any ordered
result differs. Query corpora can be supplied as a UTF-8 text file (one query per
line) or a JSON array of strings. Include "Hollywood Walk of Fame" and "Los
Angeles", typo and multilingual examples, very common prefixes, and numeric
street addresses.

Run `python3 scripts/benchmark_rank_flags.py --self-test` to verify the
benchmark harness locally.

## Notes / limitations

* The multiway merge produces the same per-string shared-bigram count as the
  sparse algorithm because the compiler sorts and deduplicates postings.
  However, at the 6,000-string cutoff, equal cosine scores may be retained in
  a different order because the old code uses an unstable top-K selection.
  Treat any resulting ranking differences as failures until resolved.
* Merge mode intentionally does not use the existing subset count cache. Warm
  queries might therefore be slower even if cold ones are faster.
* Snapshot-disable mode tests cold-query improvement and sacrifices reuse.
* Scores are expected to remain identical with prepared-area, street-cache,
  and borrowed-name-span flags. Run `cargo test` and full-output HTTP
  comparisons. Results might vary when the dataset or compiler changes.
* This environment cannot run Cypress's Rust build or production MemDB. No
  performance speedup is asserted until the benchmark is executed on the
  target hardware.
