# Europe A/B: baseline Cypress vs an alternate packed-memory token index

## Hypothesis and scope

The first FST-prefix expansion experiment appeared slower in the full Switzerland benchmark. Its query-time expansion, repeated posting intersections and unconditional admin-field joins increased CPU work while leaving the original memory layout unchanged. **The second experiment removes that prefix expansion entirely and compiles a genuinely different on-disk posting representation.** Results have not yet been measured for this revision.

This remains a **Cypress-vs-Cypress** experiment. It does not run or claim parity with the open-geocode server. The experiment borrows the idea of an mmap-addressable, FST-keyed packed index, rather than copying open-geocode's Tantivy storage, Hilbert R-tree or complete Pack format.

## What is different in the experimental memory index

The baseline compiler on the pinned `main` revision builds the existing `cypress_tokens.bin` FST with **delta-varint StringId postings** and skip metadata.

The branch compiler, invoked with `--packed-token-index`, creates `cypress_token_pack.bin` from the **same canonical multilingual StringIds**, then discards the intermediate v1 token file. The experimental file has:

- A generation-bound versioned header with string-count and dictionary fingerprint.
- The canonical lexical FST mapping a Unicode token to its ordinal.
- **16-byte fixed-width metadata per token**: absolute posting offset (`u64`), document frequency (`u32`), reserved (`u32`).
- Directly mmap-addressable **sorted `u32` StringId postings**. The posting arena is physically **grouped by eight document-frequency buckets**, with 4096-byte bucket alignment, instead of lexical posting order.
- An index reader that binary-searches the other posting arrays directly against the rarest anchor, without allocating a skip table or decoding delta-varints during each query.

This trades a larger raw posting footprint for more predictable indexed reads and fewer query-time allocations. It is a measurable tradeoff, not a presupposed speedup. The extra sidecar is experimental only and cannot be accidentally loaded as a v1 token index.

The production admin names, multilingual aliases, address anchors, scoring, place priorities, name normalization, global bigram fallback, and Scylla hydration remain in the existing Cypress pipeline. **The experiment uses the packed index for both standard retrieval and admin-area name joins**, and only falls back to the original fuzzy path when there is no complete selective plan. Unlike the first prototype, it does not perform broad prefix completion or unconditional join merging.

**Not implemented in this iteration:** reordering all place records by a geographic Hilbert key, changing the base rkyv place dictionary, Tantivy, or a full replica of open-geocode's Pack format. Those are separate experiments; this one isolates postings encoding and physical ordering.

## Input data: Switzerland, Germany, France

The benchmark downloads the **full Geofabrik country extracts**:

- `https://download.geofabrik.de/europe/switzerland-latest.osm.pbf`
- `https://download.geofabrik.de/europe/germany-latest.osm.pbf`
- `https://download.geofabrik.de/europe/france-latest.osm.pbf`

It records the SHA256 of each PBF. Each country is filtered, imported and its temporary PBF files deleted before fetching the next country. **Only Switzerland performs `ingest single --create-index`; Germany and France are appended without truncation**, producing one shared Scylla dataset.

The benchmark builds binaries from both revisions:

| Variant | Binary | Compiler | Memory index |
|---|---|---|---|
| A | pinned upstream `main` query | pinned upstream `main` compiler | `--token-index`, old FST + varint postings |
| B | experimental branch query | experimental branch compiler | `--packed-token-index`, new FST + bucketed `u32` postings |

Both indexes are compiled **independently from the same completed Scylla dataset**, in different output directories. To conserve runner disk, A is measured and its compiled files are removed before B is compiled and measured. All raw HTTP responses/measurements remain as artifacts. Each build is recorded separately, including index file sizes.

The workload contains **184 queries: Switzerland 81, Germany 50, France 53**. It includes cities, multilingual exonyms and aliases, train stations, airports, landmarks, attractions, hotels, restaurants, streets and house numbers, plus autocomplete. A/B result equality compares stable *source IDs* returned by the API, never internal StringId or PlaceId ordinals from separately compiled generations.

Outputs include cross-country p50/p95/p99 HTTP timings, per-country p50/p95 and top-1/top-5 geographic hit counts, failure counts, ordered result drift and per-query CSV. A faster experiment with worse ranking or multilingual recall is a regression.

## Run

Go to **Actions → European Cypress memory layout A/B (CH DE FR) → Run workflow**, selecting branch `bench/switzerland-open-geocode-ab`.

Current defaults: `runner=ubuntu-latest`, `cycles=11`, `scylla_memory_mib=3072`. This keeps the 16 GiB public runner's heap allocation at 3 GiB for Scylla with a 5632 MiB Docker cgroup limit (including Seastar reserve/headroom), leaving memory for the importer/compiler/query. The job clears unused Android/.NET/CodeQL tools and uses a shared Cargo build directory.

**Disk warning:** Standard Linux hosted runners have about **14 GB of SSD**; full Germany + France + Switzerland together may require more storage once Scylla and compiled indexes are included. The workflow processes one PBF at a time and deletes A's compiled index before compiling B to reduce peak usage, but this may still be insufficient. A custom/larger runner with **at least 50 GiB free disk** is advisable for reliable full-country imports. Do not interpret a storage failure as an indexing-performance result. Also note the 360-minute GitHub-hosted job limit.

Run locally (with separate scratch directories and the same provisioned Scylla host) using:
```sh
bash benchmarks/switzerland_ab/run.sh
```

## Checks

```sh
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo test --bin compiler packed_token
cargo test --bin query
python3 -m py_compile benchmarks/switzerland_ab/compare.py
bash -n benchmarks/switzerland_ab/run.sh
```

The PR's Rust compiler checks run automatically. The full three-country benchmark is manually dispatched because of its ingest and disk cost. We do not promote this mode to production until both latency and multilingual/admin ranking are validated.

## Follow-on experiments

If fixed-width frequency-bucket postings do not produce an advantage, measure compact block-packed / SIMD-searchable postings, independent large-block locality for fuzzy candidates, then consider a separate **Hilbert-ordered place-record sidecar with an explicit PlaceId permutation**. Keep ranking and the imported dataset unchanged for each A/B stage.
