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

The new query is an **adaptive rarest-first intersection**. After tokenization and deduplication, it sorts posting lists by document frequency, enumerates only the smallest list, and checks the other terms in increasing frequency order. Membership uses a binary search over upper-16-bit containers, then either a `u16` binary search or a single bitmap bit test. **No per-query skip-table allocations, no decoded probe posting lists, and no prefix completion expansion.**

As before, the engine falls back to existing Cypress bigram/fuzzy retrieval if a complete/selective token plan is unavailable, overbroad, or empty. The compiler and query reuse Cypress multilingual aliases, geocoder normalization, area/administrative hierarchy, importance scoring, street/address index and final ranking. This is intended to preserve administrative-place priority and cross-lingual recall. Differences in ranked source IDs or geographic accuracy are regressions even if latency improves.

This is an independent Cypress data structure *inspired* by Roaring bitmap techniques. It is not the upstream open-geocode Pack format, an LLM model, or the production default.

## Why this research direction

- **2025: A Comparative Study of Compressed, Learned, and Traditional Indexing Methods for Integer Data** (SEA 2025): sparse sorted integer indexing has substantial lookup-vs-space tradeoffs; Roaring uses small arrays and dense bitmaps, which is relevant to geocoder StringId posting sets. https://doi.org/10.4230/LIPIcs.SEA.2025.5
- **2025: Zombit: Exploiting Runs in Bitvectors** discusses Roaring container design and the efficiency of bitmap/array intersections. https://doi.org/10.1002/spe.70019
- **2026: UniTopRank** demonstrates CPU-efficient, language-independent candidate retrieval/ranking while retaining population, administrative level and multilingual name features, motivating preservation of Cypress ranking. https://doi.org/10.1080/13658816.2026.2645831
- **2026: POISS** introduces a multilingual POI retrieval evaluation suite (300k+ queries and 7 languages), emphasizing that evaluating quality and locale coverage matters as much as throughput. https://aclanthology.org/2026.emnlp-main.2497/

These papers support choosing and testing this architecture, **not a claim that it will beat Cypress main**. They address related but different datasets and workloads; the A/B benchmark decides.

## Three-country A/B isolation

The workflow downloads and filters full Geofabrik PBFs for **Switzerland, Germany and France**, sequentially. Only the first import uses `--create-index`, so all countries reside in **one disposable Scylla DB**.

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

The 184-case workload tests **81 Swiss, 50 German, 53 French** queries across German/French/Italian/English/Romansh names, cities, stations, landmarks, restaurants, hotels, addresses and autocomplete. Report global and per-country p50/p95, p99, failures, top-1/top-5 geographic accuracy, exact ordered source-ID drift, physical index size, and query RSS. Preserve the current workload and data import conditions for valid A/B comparisons.

## Running and validation

[Actions → European Cypress retrieval A/B](https://github.com/catenarytransit/cypress/actions/workflows/switzerland-retrieval-ab.yml), branch `bench/switzerland-open-geocode-ab`, 11 cycles by default. Manual dispatch is deliberate because three full country extracts take substantial import and compile time.

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
