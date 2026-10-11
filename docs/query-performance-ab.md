# Cypress query-performance A/B experiments

These two switches affect only the `query` binary and are read once per process.
Rebuild/restart the binary to deploy; **the compiled MemDB need not be rebuilt**.

| Switch | Default (experiment) | Rollback |
| --- | --- | --- |
| `CYPRESS_PREFIX_MODE` | `bounded` | `legacy` |
| `CYPRESS_NGRAM_CACHE_MODE` | `two-hit` | `legacy` |
| `CYPRESS_COSINE_MODE` (existing) | `adr` | `cypress` |

`bounded` preserves every exact phrase ID, probes the full query, then longest
multiword prefixes, then individual tokens. At most 2,048 additional raw FST
entries are admitted. A prefix that exceeds the remaining budget is skipped
entirely, not alphabetically truncated. This can change result recall/ranking:
compare top-10 place IDs against legacy on real queries and typed prefixes.

`two-hit` skips the costly sparse bigram-count snapshot for a novel query gram
set; the same gram set must appear a second time on the same worker before a
snapshot is stored. A bounded 512-key admission history is cleared whenever
the loaded index version changes. The original count accumulation and scoring
are unchanged, but the first repeated query does not yet benefit from a cache
hit. Admission is per worker, not process-wide; a shared cache is a separate
future experiment.

Set both to `legacy` to return to the previous behavior (requires service
restart). For separate A/B comparisons, change one switch at a time.

Run with `cypress::query::timing=debug`, and compare `prefix_candidate_count`,
`bounded_broad_skips`, `bounded_budget_used`, `strings_scored`,
`first_pass_sift4_calls`, `cache_snapshot_skipped`, `cache_snapshot_bytes`,
cache hits, HTTP p50/p95, and exact top-10 result IDs. Use a representative
mixture of exact names, typos, multilingual queries, autocomplete, and addresses.
