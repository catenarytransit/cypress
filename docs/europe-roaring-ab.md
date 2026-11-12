# Europe retrieval A/B: hybrid Roaring-style postings, 2026 research iteration

## Measured predecessor and why it was removed

The previous experiment used a lexical FST plus frequency-bucketed, fixed-width `u32` posting lists. On 184 queries across CH/DE/FR and 11 measured cycles, it **regressed** compared with main:

| Metric | Pinned Cypress main | Superseded experiment |
|---|---:|---:|
| HTTP p50 | 4.446 ms | 4.678 ms |
| HTTP p95 | 66.739 ms | 78.532 ms |
| HTTP p99 | 76.464 ms | 95.641 ms |
| Correct top-1 locations | 141/184 | 141/184 |
| Ordered source-ID differences | — | 0/184 |

The previous code, its fixed-width posting file and its `--packed-token-index` switch have been **deleted** from this branch. The 2026-11-12 run #43152243101 remains available for reproducibility, but it is not the active algorithm.

A log analysis shows why raw binary searches are not automatically faster: many queries probe broad lists hundreds of times; turning every posting into 4-byte integers increases memory traffic and still performs random accesses. The new experiment replaces that entire representation, rather than tuning its old buckets.

## New algorithm: per-token hybrid bitmap/array containers

New file `cypress_roaring_tokens.bin` uses a different, generation-bound format:
- The same multilingual Unicode **FST** and original canonical Cypress StringIds.
- Fixed-width 16-byte term metadata: posting offset (`u64`), encoded size (`u32`), document frequency (`u32`).
- Each term's sorted postings are divided by **upper 16 bits** of StringId.
- A sparse container (at most 4096 IDs) stores sorted **`u16` low components**. A dense container stores a fixed **8192-byte bitmap**. Containers have 12-byte metadata and remain contiguous in their token's posting arena.
- Both the compiler and query validate the index version, dictionary fingerprint, sizes, and payload bounds. The compiler validates the entire file before publishing. No platform-dependent `usize` arrays or unsafe borrowed bitset casting.

The new query is an **adaptive rarest-first intersection**. After tokenization and deduplication, it sorts posting lists by document frequency, enumerates only the smallest list, and checks the other terms in increasing frequency order. Intersections reuse the decoded rarest-list buffer in place. For each probe list, a forward container cursor reuses progress across increasing candidate IDs, then uses a `u16` binary search or a bitmap bit test. Standalone membership still uses a container binary search. **No per-query skip-table allocations, no decoded probe posting lists, and no prefix completion expansion.**

As before, the engine falls back to existing Cypress bigram/fuzzy retrieval if a complete/selective token plan is unavailable, overbroad, or empty. The compiler and query reuse Cypress multilingual aliases, geocoder normalization, area/administrative hierarchy, importance scoring, street/address index and final ranking. This is intended to preserve administrative-place priority and cross-lingual recall. Differences in ranked source IDs or geographic accuracy are regressions even if latency improves.

This is an independent Cypress data structure *inspired* by Roaring bitmap techniques. It is not the upstream open-geocode Pack format, an LLM model, or the production default.

## Why this research direction

- **2025: A Comparative Study of Compressed, Learned, and Traditional Indexing Methods for Integer Data** (SEA 2025): sparse sorted integer indexing has substantial lookup-vs-space tradeoffs; Roaring uses small arrays and dense bitmaps, which is relevant to geocoder StringId posting sets. https://doi.org/10.4230/LIPIcs.SEA.2025.5
- **2025: Zombit: Exploiting Runs in Bitvectors** discusses Roaring container design and the efficiency of bitmap/array intersections. https://doi.org/10.1002/spe.70019
- **2026: UniTopRank** demonstrates CPU-efficient, language-independent candidate retrieval/ranking while retaining population, administrative level and multilingual name features, motivating preservation of Cypress ranking. https://doi.org/10.1080/13658816.2026.2645831
- **2026: POISS** introduces a multilingual POI retrieval evaluation suite (300k+ queries and 7 languages), emphasizing that evaluating quality and locale coverage matters as much as throughput. https://aclanthology.org/2026.emnlp-main.2497/

These papers support choosing and testing this architecture, **not a claim that it will beat Cypress main**. They address related but different datasets and workloads; the A/B benchmark decides.

## Five-country A/B isolation

The workflow downloads and filters full Geofabrik PBFs for **Switzerland, Germany, France, the United Kingdom and Italy**, sequentially. Only the first import uses `--create-index`, so all countries reside in **one disposable Scylla DB**.

A and B use two distinct source revisions, compiler executables, and compiled indexes:

| | Baseline A | Experiment B |
|---|---|---|
| Compiler | Pinned main | PR branch |
| Output directory | `compiled/baseline` | `compiled/experiment` |
| Token format | `cypress_tokens.bin` v1 delta-varints | `cypress_roaring_tokens.bin` hybrid |
| Compiler argument | `--token-index` | `--roaring-token-index` |
| Query selector | `CYPRESS_TOKEN_RETRIEVAL_MODE=token` | `CYPRESS_TOKEN_RETRIEVAL_MODE=roaring` |

The experimental compiler creates and removes a v1 token file as an intermediate, then publishes the hybrid sidecar. A/B both use:
```text
RUST_LOG=debug,cypress=debug,cypress::query::timing=debug
CYPRESS_AREA_FANOUT=on
CYPRESS_CANDIDATE_SHORTLIST=on
CYPRESS_CANDIDATE_BUDGET=256
```

The baseline index is deleted after its 11-cycle run, before the experiment is compiled, to reduce peak SSD. The runner builds binaries in **two isolated Cargo target directories**, hashes each compiler and verifies `--roaring-token-index` **before starting Scylla or downloading PBFs**, avoiding a previous hour-long failure caused by using an old compiler executable.

The 184-case workload tests **81 Swiss, 50 German, 53 French** queries across German/French/Italian/English/Romansh names, cities, stations, landmarks, restaurants, hotels, addresses and autocomplete. Report global and per-country p50/p95, p99, failures, top-1/top-5 geographic accuracy, exact ordered source-ID drift, physical index size, and query RSS. The query workload stays at 184 cases; GB/IT add corpus size and competing matches, not new query cases. Compare A/B within the new run; older three-country timings are not directly comparable.

## Running and validation

[Actions → European Cypress retrieval A/B](https://github.com/catenarytransit/cypress/actions/workflows/switzerland-retrieval-ab.yml), branch `bench/switzerland-open-geocode-ab`, 11 cycles by default. Manual dispatch is deliberate because five full country extracts take substantial import and compile time.

PR checks should pass:
```bash
cargo check --locked --all-targets
cargo test --locked --lib roaring_token
cargo test --locked --bin compiler roaring_token
cargo test --locked --bin query
bash -n benchmarks/switzerland_ab/run.sh
python3 -m py_compile benchmarks/switzerland_ab/compare.py
```

No actual speed improvement is asserted before this new benchmark has run. If the hybrid bitmaps are still slower, analyze the split between token planning, fallback bigram scans, final-place scoring, and Scylla hydration before changing more storage structures. A/B wall latency is not a precise CPU profile.

## 2026-11-12 audit and experiment adjustment

Inspected the complete artifact from [run 43209312101](https://github.com/catenarytransit/cypress/actions/runs/43209312101), commit `2d92a2d6209d230435f843c2de215d5e9a00489a`. Its **warm** results were p50 4.632 → 4.420 ms, p95 74.495 → 65.504 ms, p99 88.569 → 75.007 ms, with zero ordered-ID differences across 184 cases. End RSS was 2,422,292 → 2,469,780 KiB (+47,488 KiB). Startup RSS was 249,580 → 318,896 KiB. These are snapshots, not proof of a larger heap or an explicit cache retention policy.

The token sidecar grew from 70,928,686 to 97,258,692 bytes. Compiler logs show **3,630,618 array containers and only 55 bitmap containers** for 9,631,219 postings. With 12 bytes of header per container, sparse-container overhead is substantial. A 4096 threshold minimizes array-versus-bitmap payload size; it does not imply this layout beats delta varints. This patch preserves the file format so the five-country experiment measures this representation rather than silently introducing another encoding.

### Paper audit

- Revisited SEA 2025, especially Appendix A.2: its tested Roaring implementation uses SIMD and run containers as well as arrays and bitmaps. Our custom scalar two-container implementation cannot inherit its performance results. The paper benchmarks integer lookup, not this geocoder pipeline.
- Revisited Zombit §2.5: it explicitly attributes efficient set operations partly to iterators reusing previous computations. The old loop performed a fresh container binary search for every candidate and allocated a second result vector. Corrected by retaining the sorted anchor vector in place and advancing a per-probe container cursor. This is still scalar membership filtering, not a complete optimized Roaring container intersection implementation; run encoding and SIMD remain unimplemented.
- Revisited UniTopRank through its DLR repository record and accessible paper text: it resolves toponyms in document context using gazetteers and beam search over spatial relationships. Cypress does not implement that algorithm; the citation motivates preserving name/population/administrative ranking only. It is not evidence for the posting layout.
- Revisited the POISS paper: it evaluates geo-anchored natural-language POI queries and graded relevance. Our approximate-coordinate, top-1/top-5 fixture checks are **not POISS evaluation** and do not validate equivalent semantic recall. Ordered-ID parity remains a regression check against the baseline, not absolute quality ground truth.

Paper links above remain the sources. No paper proves an improvement for this custom encoding.

### Query-log bottlenecks

`analyze_logs.py` strips ANSI codes and reports phase totals, fast/fallback plan counts and largest count snapshot. These totals include warmup and overlap hierarchically, so do not add them together or interpret them as isolated CPU measurements.

In the inspected run, both variants emitted 2,392 token plans: 1,794 nonempty plans and **598 fallback plans (25%)**. Experimental fallback-phase totals were:

| Phase | Calls | Total logged ms |
|---|---:|---:|
| expand_missing_bigrams | 598 | 10,475 |
| cache_store_counts | 598 | 6,464 |
| compute_cosine_candidates | 598 | 6,365 |
| execute_search_v2.fetch_places | 2,197 | 466 |

The first three are sibling phases; fetch_places is a separate HTTP pipeline phase. Large sparse-count cache snapshots reach millions of entries per query. Scylla hydration is a relatively small part of these logged totals. Faster posting membership will not fix fallback scans/cache copies. No fallback cache policy or ranking change is bundled into this experiment adjustment.

Startup logs also show an immediate version-triggered reload after the initial load, including a second reverse R-tree construction. This exists in main too and is retained for A/B comparability; process high-water RSS and first-pass results may include that startup work.

### Cache and memory protocol

After **each** index compilation and before starting that variant's query server, flush and restart the same disposable Scylla container, then wait for CQL readiness. Preserve its database contents. Record a separate one-cycle first pass (`--warmup 0`), then the usual two warmup cycles and eleven measured cycles. Report first-pass and warm timings separately.

Restarting Scylla resets its process caches. It **does not evict Linux page-cache data**; compilation and mmap validation touch index pages. Adding GB/IT increases the corpus and may increase working-set pressure, but repeated 184-query passes can still warm their working set. This is not a controlled cold-disk benchmark.

At startup, after first pass, and after warm measurement, archive `/proc/PID/status`, `smaps_rollup`, `stat`, `io`, host meminfo and Scylla stats. The report separates RSS, anonymous RSS, file RSS and VmHWM. Fault counters and I/O are available in raw artifacts; snapshots can distinguish file residency from heap/cache growth, but do not attribute individual allocations. `log-analysis.json` accompanies each report.
