# Field-aware selective token retrieval

## Problem

`Brandenburger Tor` matches exact token conjunction; `Brandenburger Tor Berlin`
previously required `berlin` in the same name StringId and thus fell back to
25M+ bigram postings. Names, streets and administrative areas are distinct
fields in Cypress. This PR uses the existing multilingual admin alias FST and
deduplicated area-set arrays to join those fields without adding an index.

## Algorithm

1. Try the complete exact token conjunction first (same behavior as PR 4).
2. If no eligible full-string candidates, enumerate contiguous name/admin
   suffix partitions for 2..8 tokens, including multiword admin suffixes.
3. Resolve each suffix through an EXACT multilingual admin alias FST lookup.
   Ambiguous aliases are allowed up to 128 matched administrative area IDs.
4. Execute the bounded token-posting intersection on the name prefix.
5. For each StringId, inspect only its mapped PlaceIds and StreetGroupIds.
   Retain strings with actual membership in one of the suffix admin areas.
6. Merge hypotheses, sort/deduplicate StringIds and enforce the existing
   6,000-candidate budget without silent truncation.
7. At expansion, recheck the area association for join-only strings, so
   globally shared name strings cannot introduce non-matching locations.
8. Reuse the existing prefix union, multilingual phrase matches, street/house
   scoring and exact token-mask DP final ranker. No compiled format changes.

Candidate retrieval has bounded posting scans (rarest anchor <=50K), <=250K
name/street mapping entries per split, and bounded output candidate size.
If there is no successful selective plan, the legacy fuzzy bigram engine
remains the fallback. The approximate token mode remains opt-in.

## Deploy (no compiler rebuild required for this PR)

```sh
cargo fmt --all -- --check
cargo test --bin query
cargo check --all-targets
cargo build --release --bin query
```

Use the current compiled dataset including `cypress_tokens.bin`:

```sh
RUST_LOG=info,cypress::query::timing=debug CYPRESS_TOKEN_RETRIEVAL_MODE=shadow ./target/release/query --memdb-dir ./data/compiled
```

`shadow` measures field-aware token plans while serving legacy results.
For A/B testing, restart query with `CYPRESS_TOKEN_RETRIEVAL_MODE=token`.
For rollback, restart with `CYPRESS_TOKEN_RETRIEVAL_MODE=legacy`.

Expected `field-aware token retrieval plan` logging for queries with
recognized exact administrative suffixes: splits_attempted, splits_matched,
matching_strings, posting_entries_examined, mapping_links_examined, rejected.

## Regression cases

- `Brandenburger Tor Berlin`: joins place name and Berlin area.
- `Brandenburger Tor`: full-name token path remains usable.
- `Kencho Matcha München`: suffix recognized if the exact alias exists.
- `Kencho Matcha Munchen`: requires an actual `munchen` admin alias;
  otherwise falls back; this PR does not add transliteration/fuzzy admins.
- `Central Park New York`: two-token admin suffix.
- Ambiguous admin names: join candidates across all exact aliases, bounded.
- Shared street string IDs: require actual street-group admin membership.
- Unknown / fuzzy area suffix: preserves existing legacy fallback.
- Multi-script names: suffix uses exact indexed UTF-8 aliases only.

## Limitations

Only CONTIGUOUS admin suffixes are considered, not arbitrary reordered
queries or admin prefixes. Full-string conjunction and legacy prefix hits
remain independently eligible. Query ranking remains approximate in token
mode; validate relevance against the legacy multilingual benchmark before
enabling it by default. No runtime tests have been executed in the
GitHub-only edit environment.
