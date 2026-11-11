# Switzerland experiment: production main vs FST/packed-postings retrieval

This is a **Cypress-vs-Cypress** experiment. It does **not** launch the open-geocode HTTP server. The control builds from the current `main` SHA at benchmark execution time. The experiment builds this branch and activates `CYPRESS_TOKEN_RETRIEVAL_MODE=open-geocode`.

## Production baseline (identical except retrieval mode)

```ini
RUST_LOG=debug,cypress=debug,cypress::query::timing=debug
CYPRESS_AREA_FANOUT=on
CYPRESS_CANDIDATE_SHORTLIST=on
CYPRESS_CANDIDATE_BUDGET=256
CYPRESS_TOKEN_RETRIEVAL_MODE=token
```

The experiment inherits every setting above and changes only the last one to `open-geocode`. It retains the Cypress API, multilingual search-key dictionary, administrative-place priors, area-aware name joins, candidate shortlist, SIFT4/structured scoring, and Scylla response hydration. The production mode is unchanged.

## What is borrowed from open-geocode

open-geocode uses memory-mapped, compressed indexes with FST vocabulary traversal, Tantivy-based fuzzy search, and spatial index packing. This **initial prototype** explores its FST-based term/prefix candidate-retrieval strategy with Cypress's existing generation-bound `cypress_tokens.bin` packed postings sidecar. It computes bounded prefix expansions and intersects token postings, then passes matching original Cypress StringIds through the existing place/admin ranker.

**This is not an implementation of open-geocode's proprietary Pack format or Tantivy index**; it is a deliberate first-stage adaptation. Cypress already uses mmap and FSTs. The experiment does not test whether copying open-geocode's entire file layout, page organization, Tantivy engine or Hilbert R-tree will be faster. Follow-on work should isolate storage layout and spatial indexing if this experiment indicates useful gains.

The experimental mode refuses broad prefixes, missing terms, invalid sidecars, and overlarge candidate sets and falls back to the baseline bigram path. Some token matches still cannot substitute for Cypress's multilingual cross-field semantics. In this experiment the normal ranker and administrative field joins remain active.

## Running

Open **Actions → Switzerland Cypress retrieval A/B (main vs experimental) → Run workflow**, select this PR branch. Specify a large runner label if available. Full-country ingestion plus Scylla benefits from generous RAM (16+ GiB recommended) and scratch disk (~32 GiB or more; depends on current extract). This manually dispatched job is intentionally not run on every PR. On a standard `ubuntu-latest` runner, the benchmark now starts a 1-shard Scylla instance with 1536 MiB of reserved memory and a 2048 MiB Docker limit, rather than requesting an unavailable 3 GiB; it also compiles the Rust binaries **before** starting Scylla and defaults to two compiler workers. The **Run workflow** form now exposes `scylla_memory_mib` (default `1536`); increase it only when selecting a larger runner. `BENCHMARK_COMPILER_CONCURRENCY` may be overridden in the runner environment to raise compiler parallelism. The larger runner remains recommended for full Swiss ingestion and consistent latency measurements.

The workflow:
1. Pins the baseline `main` commit SHA when it starts; builds its `ingest`, `compiler`, and `query` binaries.
2. Downloads exactly one `switzerland-latest.osm.pbf` from [Geofabrik](https://download.geofabrik.de/europe/switzerland-latest.osm.pbf), records SHA256 and metadata, then filters data using Cypress's existing filter scripts.
3. Imports the filtered source once into a **disposable** Scylla instance; compiles one shared memdb generation with `--token-index`.
4. Builds and runs the PR's query binary separately, retaining all production flags except the experimental mode.
5. Warmups, then performs 11 deterministic randomized cycles over Swiss cities, stations, tourist attractions, addresses, hotels, restaurants, and autocomplete prefixes, including German, French, Italian, English and Romansh names.
6. Exports raw per-request measurements, query-level p50/p95, total p50/p95/p99, HTTP failures, ordered result ID drift, top-five overlap, and approximate geographic hit@1/hit@5.

Both variants serve `/v2/search` or `/v1/autocomplete` with identical parameters. Results share the same content generation, so record IDs are comparable. The query-level JSON/CSV is uploaded as an Actions artifact and the Markdown report is posted to the job summary. Differences in correct results **must** be considered regressions until investigated, even if an alternative path is faster.

The test is end-to-end HTTP latency (including Scylla hydration). It is **not** a format-only microbenchmark and cannot be used to attribute all speedup to the memory layout. Query-server RSS and Scylla memory are recorded separately. Because the control runs first, CPU, page-cache and order effects can influence results; for publication, rerun on a dedicated runner, reverse A/B order, and repeat.

## Build checks

```sh
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo test --bin query
```

This experimental retrieval mode must never become the production default until parity and multilingual/admin ranking are demonstrated. In particular, inspect all changed results for Zürich / Zurich / Zurigo / Turitg, Genève / Geneva / Genf / Ginevra, Bâle / Basel / Basilea, station aliases, and queries composed of place plus municipality.

## Next steps

If significant gains hold with acceptable recall, test a second alternative storage backend using a dedicated Tantivy index and open-geocode-style packed sections, keeping the Cypress data model and ranker. A separate ranking-stage/hydration timing breakdown will help identify which bottleneck actually dominates.
