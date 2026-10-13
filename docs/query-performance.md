# Cypress query performance

The query server defaults to **configuration B**: bounded prefix retrieval,
ordinary sparse bigram-cache admission, and ADR cosine scoring. These are
runtime choices: restarting the query binary is sufficient; the compiled
MemDB does not need to be rebuilt.

| Setting | Default | Supported rollback |
| --- | --- | --- |
| `CYPRESS_PREFIX_MODE` | `bounded` | `legacy` (unbounded prefix search) |
| `CYPRESS_COSINE_MODE` | `adr` | `cypress` (previous formula) |

Bounded prefix retrieval retains exact phrase IDs, then probes full-query,
longer multiword, and single-token prefixes. It accepts at most 2,048
additional FST entries, skipping overbroad prefixes rather than truncating
them in arbitrary lexicographic order. All accepted candidates go through
the normal ranker.

The bigram cache stores a sparse count snapshot whenever a query's exact gram
set is not yet cached. Multiway posting-list merging and unconditional snapshot
disabling were rejected after query-latency regressions and, for merging,
result-order changes; neither is available as a runtime mode.

The query scorer now always borrows canonical space-separated name spans
instead of rebuilding those substrings. For punctuation or repeated separators,
it retains the canonical-building fallback. Existing SIFT4 exact/prefix fast
paths and ranking semantics are unchanged. The previous
`CYPRESS_BORROW_NAME_SPANS` flag is no longer read.

Prepared administrative-area names and query-local street-component caching
remain independent **opt-in** experiments via `CYPRESS_PREPARED_AREA_SCORING=1`
and `CYPRESS_CACHE_STREET_COMPONENTS=1`. The individual variants helped in
some benchmarks, but their combinations did not consistently improve median
latency, so neither is enabled by default.

The experimental `CYPRESS_LEXICAL_LOWER_BOUND`, `CYPRESS_NGRAM_CACHE_MODE`, and
`CYPRESS_COSINE_SHADOW` environment variables are no longer supported. Remove
them from deployment configuration. In an 852-pair comparison, the SIFT4
lower-bound experiment preserved rankings but regressed HTTP p50 from
365.47 ms to 368.93 ms and p95 from 864.80 ms to 868.31 ms; it is not deployed.

For production profiling, enable `cypress::query::timing=debug` and inspect
`prefix_candidate_count`, `bounded_broad_skips`, `bounded_budget_used`,
`first_pass_sift4_calls`, `cache_snapshot_bytes`, cache hits, and HTTP p50/p95.
Continue checking ordered result IDs on multilingual, autocomplete, typo,
and address queries before changing retrieval behavior.
