# Area-aware place fanout experiment

## Problem
On the October 16 traces, `Starbucks Chicago` selected 256 name strings but expanded them to 14,435 unique places and fully reranked 10,000. Its geographic/structured phase consumed 6,189 ms. The prior field-aware token path restricts only strings introduced *exclusively* by a field join; an exact name string that was also returned through ordinary prefix retrieval can still expand to thousands of unrelated worldwide locations.

## What this PR does
When enabled, the search query planner checks all contiguous two-field partitions **in both name-area and area-name ordering**, using exact lookups in the existing name FST and multilingual administrative alias FST. For each eligible exact name StringId, it:

1. Requires that the StringId was actually selected by this search request and has at least 64 mapped places.
2. Resolves the other query span to exact administrative AreaIds (including aliases mapping to multiple areas).
3. Verifies that at least one mapped place belongs to a matching area, otherwise skips gating.
4. Caches AreaSetId membership results within this query.
5. Applies the gate at the **PlaceId expansion** inside the name-evidence loop, before generating candidate evidence or scoring administrative components.
6. Leaves unmatched name StringIds, literal full-name StringIds, and all independent street/address retrieval hypotheses intact.

An exact name such as `Starbucks` is restricted to Chicago when the other component exactly resolves as Chicago. `Starbucks Chic` stays unrestricted until an exact recognized area alias exists; future work can introduce a separately measured partial-area plan.

## Switches
The new `CYPRESS_AREA_FANOUT` flag is independent of `CYPRESS_TOKEN_RETRIEVAL_MODE` and `CYPRESS_CANDIDATE_SHORTLIST`.

- `off` (default): no change to baseline behavior or scoring.
- `shadow`: build the plan, compute the potentially excluded place-link count, but return unchanged results.
- `on`: actually exclude nonmatching place links **for scoped exact-name StringIds only**. Approximate, opt-in.

Logs include `exact_name_area_interpretations`, `gated_name_strings`, `preflight_place_links`, `checked_place_links`, and `would_reject_place_links`.

## Evaluation
Use the same compiler generation, load, and query inputs for off/shadow/on. Run `cargo fmt --all -- --check`, `cargo test --bin query area_fanout`, and `cargo check --all-targets` before deploying.

Test complete and partial queries, place-focused and area-focused input order, and multilingual addresses:

- `Starbucks Chicago`, `Chicago Starbucks`, `Starbucks Chic`
- `Santa Monica Pier California`, `Paris France`
- `Studio 54 New York` (digits as POI name)
- `4885 Paseo De Vega Irvine`, `Rue de la Loi 16 Bruxelles`
- `Berlin Brandenburger Tor`, `Brandenburger Tor Berlin`
- Ambiguous aliases, no-local-match cases, and queries where the administrative term is part of a literal POI name.

Compare result PlaceIds and order, top-10 recall, place fanout, structured DP candidate count, string scoring calls, mmap page faults and p50/p95 request latency.

**Do not assume the new gate is exact-ranking-safe.** A remote place with the same name may be legitimately ranked because of focus, importance or a different interpretation. The baseline still runs in shadow mode, and on mode should not become the default until relevance testing passes.

## Deploy
Only Rust query changes. No worldwide dataset recompile required. `git pull` after merging, then `cargo build --release --bin query` and restart the actual service (often `cypress`). Keep `CYPRESS_AREA_FANOUT=shadow` during validation.
