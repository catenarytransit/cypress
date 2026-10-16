# Experimental ranked candidate admission for Cypress query

This change addresses the observed candidate explosion after exact token retrieval:

| Query | Token candidates | Candidate strings | Scored places | Search latency |
|---|---:|---:|---:|---:|
| UC Irvine | 2 | 185 | 185 | 183 ms |
| Toronto | 1,111 | 1,122 | 1,131 | 1,196 ms |
| Vancouver | 902 | 909 | 918 | 930 ms |
| Miami Florida | 3 | 1,713 | 2,040 | 2,733 ms |

These values are **pre-change logs**; this PR has not yet been benchmarked.

## Approach and safety

Instead of reducing the global prefix enumeration indiscriminately, apply a ranked candidate admission step **after** all token, full-query prefix, multiword prefix, and exact-phrase StringIds have been gathered but **before** expensive SIFT4 phrase scoring, address expansion and structured DP.

- Preserve all exact phrase StringIds, including exact multiword names.
- Rank the rest using known full-query prefix, multiword prefix, entity importance/category, and tie-breaking by name length and StringId.
- Use no English-only address parsing or assumptions about numeric token positions. Street and POI StringIds share this admission step.
- Keep the default engine exactly unchanged.
- In `shadow` mode, compute the shortlist but **serve baseline results**. Compare coverage estimates with the original top-10 and run full-output comparisons during evaluation.
- In `on` mode, apply the shortlist. This is explicitly **approximate**. It may miss a result previously found through a weak or uncommon prefix and does not have a proven top-K bound.
- `CYPRESS_CANDIDATE_BUDGET` controls the additional approximate candidate budget (default 256, clamped to 32..4096). All exact phrases are preserved even when their count exceeds the budget.

The shadow top-K coverage metric counts whether the shortlisted strings directly map to each baseline result's place ID. A result from a street/house join may not have a direct name mapping; therefore, this metric is a **diagnostic estimate**, not an exact recall score.

## Experiment

```sh
cargo fmt --all -- --check
cargo test --bin query candidate_budget
cargo check --all-targets
```

Baseline:

```sh
CYPRESS_CANDIDATE_SHORTLIST=off CYPRESS_TOKEN_RETRIEVAL_MODE=token ./target/release/query --memdb-dir ./data/compiled
```

Observe without changing answers:

```sh
RUST_LOG=info,cypress::query::timing=debug CYPRESS_CANDIDATE_SHORTLIST=shadow CYPRESS_CANDIDATE_BUDGET=256 CYPRESS_TOKEN_RETRIEVAL_MODE=token ./target/release/query --memdb-dir ./data/compiled
```

For a separate experimental A/B instance only:

```sh
RUST_LOG=info,cypress::query::timing=debug CYPRESS_CANDIDATE_SHORTLIST=on CYPRESS_CANDIDATE_BUDGET=256 CYPRESS_TOKEN_RETRIEVAL_MODE=token ./target/release/query --memdb-dir ./data/compiled
```

Recommended test queries:

- `Miami Florida`, `Florida Miami`, `Miami Flor`
- `Toronto`, `Vancouver`, `UC Irvine`
- `Brandenburger Tor Berlin`, `Berlin Brandenburger Tor`
- `4885 Paseo de Vega Irvine`, `4885 Paseo de V`
- `Rue de la Loi 16 Bruxelles`, `16 Rue de la Loi Bruxelles`
- `Studio 54 New York`, `1000 Bruxelles`
- Every keystroke-prefix of those queries.

Capture top-10 place IDs/order, rank displacement, availability of intended address/POI, SIFT4 calls, scorer CPU time, minor/major page faults, p50/p95 warm/cold latency, and Scylla hydration time. No dataset recompilation is needed.

## Follow-on work deliberately not included

1. Exact name+administrative field joins should run **even when full-query token intersection succeeds**. That is currently a correctness/recall limitation and must be addressed separately.
2. The query's current `PreparedMatchCandidate::score` already has an exact/prefix SIFT4 bypass. Additional short-circuiting must be proven score-equivalent.
3. A compact local result-hydration index, indexed by PlaceId, is needed to remove the Scylla fetch phase from the critical path for full V2 parity. The existing dense index does not contain all result fields.
4. Top-K bound or measured recall-aware widening is needed before an approximate shortlist becomes the default.

**Do not enable the shortlist by default unless recall and ranking comparisons pass.**
