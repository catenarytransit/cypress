# Disk-backed spatial grid: bounded external sort

This build replaces the compiler's 192M-element `cell_pairs` vector and final
in-memory spatial sort with a disk-backed grid. It is a targeted RSS fix:
other major in-memory `CypressMemDb` arrays remain unchanged.

## Build

```sh
cargo fmt --all -- --check
cargo test --lib spatial_grid
cargo test --bin compiler
cargo check --all-targets
cargo build --release --bin compiler --bin query
```

Compile with the new sort-buffer flag (128 MiB by default):

```sh
./target/release/compiler --scylla-url 127.0.0.1 --out-dir ./data/compiled --grid-sort-buffer-mib 128 --token-index
```

The compiler streams exactly one 8-byte (cell_id, place_id) record per place
to scratch, sorts fixed-size chunks, merges at most 32 sorted runs at once,
and streams the CSR columns (sorted active cell IDs, offsets, place IDs)
to `cypress_grid.bin`. Place records are unmapped after the initial scan.
Temporary disk must accommodate the input pairs, sorted runs, merge passes,
and final grid during compilation. For ~192M places, each full pair stream
is ~1.54 GB decimal (~1.43 GiB), so provision several GB of scratch
space beyond the existing large compiler spill files.

## Runtime

```sh
./target/release/query --memdb-dir ./data/compiled --scylla-url 127.0.0.1 --listen 0.0.0.0:3000
```

`MemdbData` validates and maps the new grid file on load/hot reload.
Reverse geocoding consults the disk-backed grid and existing place
coordinate arrays. Older builds without `cypress_grid.bin` still use
their existing embedded `active_cells`, `cell_offsets`, `cell_places`.
New builds intentionally leave those rkyv grid vectors empty to avoid
duplicating hundreds of millions of place IDs in the compiled index.
The query loader rejects new builds without their spatial sidecar.

## Algorithm and bounds

- Input: `N` fixed-width 8-byte pairs, with global place IDs preserved.
- Run formation: up to `sort_buffer_mib * 2^20 / 8` pairs in memory.
- Merge: binary min-heap with <=32 readers, repeatedly coalescing runs.
- Output: streamed sorted cells, CSR offsets, and cell-place IDs.
- Extra RAM: O(sort buffer + 32 buffered readers + small merge heap).
- I/O: external merge sorting, proportional to full reads/writes per pass.
- Reverse search: O(log C + places in requested cells), where C is number
  of occupied cells; results and distance ranking remain unchanged.

The grid format has a 32-byte little-endian header and direct u32 arrays.
The loader validates cell order, nonempty ranges and final offsets once
per generation. It does not scan all place IDs during ordinary requests.

## Regression checks

Tests cover unsorted input, ties within cells, >32 runs (multi-pass merge),
empty grids, and malformed CSR directory metadata. Compare `/v1/reverse`
and `/v2/reverse` answers against a legacy build using the same source data.

## Limitations

This PR removes the late O(N) spatial sort and the duplicated grid vectors.
It does **not** make the complete Cypress compiler memory-bounded:
the name, address, bigram and other place arrays still grow in RAM.
Further PRs should stream those final arrays and address their RSS peaks.

Generation publication is only partially atomic in the existing compiler;
do not hot-reload a live service from a directory while its files are being
overwritten. Compile into a fresh directory and switch the serving generation
after compilation succeeds, or deploy with a stopped query service.
