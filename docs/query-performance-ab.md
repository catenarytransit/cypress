# Cypress query-performance A/B experiments

These two switches affect only the `query` binary and are read once per process.
Rebuild/restart the binary to deploy; **the compiled MemDB need not be rebuilt**.

| Switch | Default (configuration B) | Alternative |
| --- | --- | --- |
| `CYPRESS_PREFIX_MODE` | `bounded` | `legacy` |
| `CYPRESS_NGRAM_CACHE_MODE` | `legacy` | `two-hit` |
| `CYPRESS_COSINE_MODE` (existing) | `adr` | `cypress` |
| `CYPRESS_COSINE_SHADOW` | off (no alternate scorer) | `1` for A/B comparison |

With the default settings, only ADR cosine runs, even with DEBUG logging enabled.
Set `CYPRESS_COSINE_SHADOW=1` to re-enable the alternate scorer for
diagnostic comparisons (requires DEBUG logging and a service restart).
The shadow scorer never selects results. The separate
`CYPRESS_COSINE_MODE=cypress` rollback still selects the legacy formula.

`bounded` preserves every exact phrase ID, probes the full query, then longest
multiword prefixes, then individual tokens. At most 2,048 additional raw FST
entries are admitted. A prefix that exceeds the remaining budget is skipped
entirely, not alphabetically truncated. This can change result recall/ranking:
compare top-10 place IDs against legacy on real queries and typed prefixes.

The production default is **configuration B**: bounded prefix retrieval, legacy
bigram snapshot admission, and ADR cosine. The environment variables must be
unset (or explicitly set to these values) for those defaults to take effect.

`two-hit` is now opt-in and skips the costly sparse bigram-count snapshot for a novel query gram
set; the same gram set must appear a second time on the same worker before a
snapshot is stored. A bounded 512-key admission history is cleared whenever
the loaded index version changes. The original count accumulation and scoring
are unchanged, but the first repeated query does not yet benefit from a cache
hit. Admission is per worker, not process-wide; a shared cache is a separate
future experiment.

To restore unrestricted prefixes, set `CYPRESS_PREFIX_MODE=legacy`. To retry
two-hit admission, set `CYPRESS_NGRAM_CACHE_MODE=two-hit`. Restart the query
process after changing either setting. For separate A/B comparisons, change
one switch at a time.

Run with `cypress::query::timing=debug`, and compare `prefix_candidate_count`,
`bounded_broad_skips`, `bounded_budget_used`, `strings_scored`,
`first_pass_sift4_calls`, `cache_snapshot_skipped`, `cache_snapshot_bytes`,
cache hits, HTTP p50/p95, and exact top-10 result IDs. Use a representative
mixture of exact names, typos, multilingual queries, autocomplete, and addresses.
