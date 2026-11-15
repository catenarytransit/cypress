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

The workflow downloads and filters full Geofabrik PBFs for **Switzerland, Germany, France, the United Kingdom and Italy**, sequentially. Each `ingest single` call publishes an active places_v2 generation for its distinct source filename, so all countries reside in **one disposable Scylla DB**. The database starts empty; no truncation flag is needed.

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

## 2026-11-13 experimental redesign: ABBA, country coverage and adaptive Roaring layout

**Benchmark:** each executable is benchmarked in independent A1/B1/B2/A2 processes, where A is pinned main and B is the experimental branch. The benchmark restarts the disposable Scylla instance before every phase, runs a one-cycle first pass, performs two warmups followed by eleven measured cycles, and merges the two measured blocks for each variant. Each process has distinct logs and memory snapshots. Because disk space is constrained, the baseline index is recompiled before A2; its size manifest must match the original. The kernel page cache is not flushed and these are not controlled cold-disk runs.

**Fixtures:** 234 cases: CH 81, DE 50, FR 53, GB 24, IT 26. New cases include city names, language variants, station names, landmark lookups, addresses, and autocomplete. Coordinates are approximate and are not independently labeled relevance judgments. The summary now reports nondeterministic IDs across observed measured cycles.

**Storage:** `cypress_roaring_tokens.bin` format version 2 chooses an exact representation per posting:
- Very small postings: zero-container marker and sorted flat u32 StringIds, avoiding a 12-byte header for each high-16 shard.
- Sparse shards: sorted u16 arrays.
- Dense shards: 8192-byte bitmaps.
- Consecutive shards: Roaring-style run container using [u16 start, u16 inclusive extra length] pairs.

The compiler chooses the smaller of flat posting and container encoding. For individual 16-bit shards, run encoding is used only when smaller than either the sparse array or dense bitmap. This reduces file size for highly fragmented rare tokens and compresses spatially ordered or consecutive IDs. The representation is generation-bound and deep-validated before publication.

## 2026-11-14: container intersection and runtime AVX2

The query now intersects the two rarest postings before materializing the candidate
list. It merges high-16 keys without reading skipped container payloads. Dense pairs
use 256-bit AND and skip zero vectors before decoding surviving bits; sparse pairs
consume packed u16 values directly. Only run/bitmap mixed cases need a reusable
scratch buffer. Further terms filter the sorted candidates in place, loading each
container header once per candidate group.

Array probes use V1-style broadcast equality (16 u16 or 8 u32 lanes), advancing
through block maxima with exponential search plus binary refinement for large gaps.
The cursor never returns to an earlier block. All SIMD loads are unaligned and bounded
by a complete 32-byte block; tails use scalar reads. x86-64 AVX2 is detected at runtime,
and other CPUs retain a portable scalar path. No global `target-cpu=native` requirement,
new dependency, or on-disk format change is introduced.

### Literature and scope

- [Chambi et al., Better bitmap performance with Roaring bitmaps, v4, §4](https://arxiv.org/pdf/1402.6407v4): match high keys first; bitmap/bitmap word AND; array/bitmap bit tests; merge or galloping for arrays. The paper's 4096-element threshold is a space crossover, not a recommendation to scatter IDs uniformly.
- [Lemire, Boytsov, Kurz, SIMD Compression and the Intersection of Sorted Integers](https://arxiv.org/abs/1401.6399): vector broadcast probes and galloping motivate the array kernel. This is a bounded-block adaptation, not an implementation of every algorithm in that paper.
- [Lemire et al., Roaring Bitmaps: Implementation of an Optimized Software Library](https://arxiv.org/abs/1709.07821): container-specific SIMD kernels. This implementation does not claim CRoaring's complete kernel matrix or performance.

Canonical StringIds and v2 encoding remain unchanged. Padding every small array to
32 bytes would increase sparse-token storage; loadu avoids requiring that padding.
Compiler logs now expose flat lists, run containers, and population buckets
`[1..16, 17..256, 257..4096, 4097..65536]`, alongside array/bitmap counts. These count
only containers actually serialized, after the flat-list size comparison.

Use these distributions to decide whether a separate ID permutation is worthwhile.
A token-cooccurrence ordering could cluster common terms into fewer containers, but
requires a reverse map and changes candidate locality in the name/postings tables.
Uniform redistribution would generally destroy useful runs and reduce local density.
Neither permutation nor fuzzy bigram accumulation is changed here.

### Validation and measurement

Tests compare dispatched and forced scalar array probes with a sorted-set oracle,
covering every short tail length, randomized monotone queries, unaligned storage,
large gaps, and unsigned boundaries. Compiler tests cover all flat/array/bitmap/run
pairings, missing shards, multiple shards, and high IDs. CI includes the SIMD tests.

Run:

```sh
cargo test --locked --lib --bin compiler --bin query
rustc --edition=2021 -O --test scripts/bench-roaring-kernels.rs -o /tmp/roaring-kernels
/tmp/roaring-kernels --ignored --nocapture
```

One local AVX2 kernel run (200 repetitions, 3856 sparse values) measured repeated
binary probes versus the dispatched block kernel: 79.0 vs 25.8 ms for step-3
candidates, and 2.46 vs 0.76 ms for step-127 candidates. These are synthetic warm
array-probe timings, not end-to-end or cold-cache results; the flat-list baseline
previously used a linear cursor, so these figures do not quantify that path's gain.
The existing five-country ABBA workflow remains necessary to assess latency, memory,
result parity, and fallback rate before merging.

## 2026-11-14: fix the fuzzy fallback bottleneck

### Evidence from run 43599744653

The linked run built `a05bd181a87ce93b389509e1a1490037fa6b4d41`.
It predates the AVX2 commit `c74945e`, so it cannot measure that commit.
The warm median changed from 5.336 to 5.287 ms; p95 changed from 100.471 to
101.761 ms. All 234 ordered result-ID lists matched. This single ABBA run
establishes no meaningful overall win; the small tail difference does not by
itself establish a particular kernel regression.

Both variants logged 6,552 token plans: 5,264 nonempty and 1,288 fallbacks.
For the experiment, sibling fallback phases consumed 43,393 ms expanding
bigrams, 22,222 ms making sparse cache snapshots, and 19,395 ms filtering/scoring
cosine candidates. Search itself logged 99,659 ms; hydration logged 3,317 ms.
These totals include warmup/first-pass requests and overlapping parent timers;
only the three sibling phases may be added. The largest snapshot was 21,578,140
payload bytes. Baseline had 1,286 cache misses and only two exact hits, so the
large snapshots were nearly always recreated rather than reused.

The central design error was accelerating exact-token AND while leaving fuzzy
T-overlap accumulation unchanged. The expensive path still scattered writes
across corpus-sized count/epoch arrays, materialized every touched ID, and copied
those mostly rejected IDs into cache snapshots. Improving a sparse membership
microbenchmark cannot remove those costs.

### Algorithm

`CYPRESS_TOKEN_RETRIEVAL_MODE=roaring` now uses blocked ScanCount for fuzzy
fallbacks. Existing token container intersections remain the selective path.
The sorted, unique bigram postings produced by the compiler are read directly:

1. Select the smallest next ID block across the query's posting cursors. Skip
   entirely empty blocks. Slice each posting at the block boundary.
2. Accumulate exact saturating counts in an **8 KiB counter array**, covering
   8,192 consecutive canonical StringIds. No per-ID hash lookup or epoch stamp.
3. Replay those local slices in query-bigram order. Emit strings whose final
   count reaches the existing ADR/Cypress minimum, and zero emitted counters
   to prevent duplicates. Retain full counts for the existing cosine formula.
4. Append survivors to buckets by their first posting. Flatten those buckets to
   preserve uncached legacy discovery order, including score ties at the cap.
   This needs neither a corpus-sized origin table nor a global survivor sort.

Only survivors reach the existing cosine scoring and candidate cap. The experiment
allocates no corpus-sized count/epoch arrays and creates no all-touched snapshots;
its counters are query-local. Existing scoring formulas, prefix union, languages,
StringIds, token index encoding, and source lookup remain in use. Main/token mode
retains its original cache and accumulation path as the A/B control. The new path
recomputes counts rather than reusing cached subsets; preserving uncached discovery
order does not promise parity with historical cache-dependent tie ordering. The
five-country result-ID comparison is therefore still required.

### Research basis

- [Chambi et al., Better bitmap performance with Roaring bitmaps, §4](https://arxiv.org/pdf/1402.6407): the representation and operation must fit the local density and operand cardinalities. Sparse-array intersections and bitmap AND accelerate conjunctions; a fuzzy threshold query needs different aggregation.
- [Lemire, Boytsov, Kurz, SIMD Compression and the Intersection of Sorted Integers](https://arxiv.org/pdf/1401.6399): evaluate the whole retrieval pipeline and match intersection strategy to list skew, rather than inferring full-query gains from one probe kernel.
- [Lemire et al., Roaring Bitmaps: Implementation of an Optimized Software Library, §4.2](https://arxiv.org/pdf/1709.07821): block-based array intersections differ from checking each candidate with binary search. The existing array-probe microbenchmark remains a limited kernel comparison.
- [Kaser and Lemire, Compressed bitmap indexes: beyond unions and intersections, §5.1](https://arxiv.org/pdf/1402.4466): ScanCount computes exact T-overlap; the paper also discusses alternatives such as MGOPT, DSK and bit-sliced aggregation. No single strategy wins on all distributions.
- [Lemire, How fast can scancount be?](https://lemire.me/blog/2019/08/30/how-fast-can-scancount-be/): partition the ID universe so counters fit in CPU cache, and use small counters. The local-slice replay and first-posting output buckets here preserve Cypress's ordering semantics.

This is cache-local threshold aggregation over the existing sorted bigram lists,
not a claim that token Roaring AND implements fuzzy matching, or that this is
CRoaring's bit-sliced threshold algorithm. Building another full bigram sidecar
would add storage without first proving a retrieval benefit.

### Validation

The standalone harness compares full `(StringId, count)` output **and order** with
an independent count-map oracle, including randomized lists, empty/missing blocks,
unsigned maximum IDs, thresholds above list count and 255-count saturation.
Its timing baseline mirrors the actual reusable epoch/count arrays, touched IDs,
snapshot copy and threshold filtering, rather than repeated binary searches.
Eight measured iterations alternate old/new order after correctness checks.

One local serial optimized run measured:

| Synthetic case | Posting entries | Survivors | Epoch + snapshot ms | Blocked ms | Speedup |
|---|---:|---:|---:|---:|---:|
| Broad fuzzy | 8,000,920 | 121,060 | 92.212 | 44.982 | 2.05x |
| Dense short | 2,999,322 | 339,162 | 23.618 | 14.577 | 1.62x |
| Sparse | 375,798 | 112 | 11.546 | 3.335 | 3.46x |

These synthetic timings exclude HTTP, cosine scoring and database hydration; they
are not a five-country latency claim. Run serially with
`/tmp/roaring-kernels --ignored threshold --nocapture --test-threads=1`.
The ABBA script now archives kernel timings as `retrieval-kernels.txt` before
building/importing. Logs add token retrieval timing, microsecond phase precision,
blocked-threshold work and survivor counts, so the next full run can test the
hypothesis directly. Merge still requires end-to-end latency, result parity and
resident-memory validation.
