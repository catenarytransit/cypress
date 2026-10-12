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
set is not yet cached. The query scorer uses the standard SIFT4 path, with
the existing exact/prefix fast paths preserved.

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
