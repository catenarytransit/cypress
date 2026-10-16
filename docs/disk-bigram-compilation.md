# Bounded external bigram compilation

Why: the previous compiler loaded each large bigram shard into a Vec<(u16,u32)>,
sorted it, and appended every posting to a global Vec<u32> while the
152.7M-house address arrays were still resident. This can exceed RSS limits.

## Algorithm

1. Retain the existing deterministic StringId assignment and 256 shard spills.
2. Read up to --bigram-sort-buffer-mib of pairs per run (default 64 MiB).
3. Sort and deduplicate each bounded run by (gram, StringId).
4. Merge at most 32 runs using a min-heap; reduce in passes if necessary.
5. Deduplicate across runs during the merge, emit StringIds directly to disk,
   and keep only the 65537 u32 offsets in process memory.
6. Publish cypress_bigrams.bin with header, count and name fingerprint.
7. Keep rkyv bigram_data empty in newly compiled generations. Query mmap
   reads the new sidecar using the same offsets and legacy cosine score.

Bounded extra sort RAM is O(M + kB + k), where M is the run buffer,
k <= 32 is merge fan-in and B is buffered-reader size. External merge IO
is O((N/B) log_(M/B)(N/B)) in the standard model, with additional
sequential writes per merge pass.

## Compiler

```sh
cargo build --release --bin compiler --bin query
./target/release/compiler --scylla-url 127.0.0.1 --out-dir ./data/compiled --grid-sort-buffer-mib 128 --bigram-sort-buffer-mib 64 --token-index
```

Produces cypress_bigrams.bin alongside cypress_index.bin,
cypress_places.bin and cypress_grid.bin. Use a fresh output directory
and a scratch directory with sufficient free space. Bigram shard files
are still produced by the existing compiler.

## Runtime

```sh
CYPRESS_TOKEN_RETRIEVAL_MODE=legacy ./target/release/query --memdb-dir ./data/compiled --scylla-url 127.0.0.1
```

The server validates the sidecar once per data generation and reads its
mmap-backed u32 postings with the existing bigram offsets.
Old compiled datasets with embedded bigram_data remain supported.
Do not mix sidecars and main indexes from different compilations.

## Tests

```sh
cargo fmt --all -- --check
cargo test --lib bigram_index
cargo test --bin compiler
cargo test --bin query
cargo check --all-targets
```

Test coverage includes sort/dedup, byte format, header validation,
fingerprinting and offset consistency. Compare the same legacy query
workload and reverse queries before and after rebuild.

## Remaining memory problem

This is a targeted reduction, not a complete bounded-memory compiler.
The many house, postcode, place and address reverse-map Vecs remain
resident during bigram sorting and rkyv serialization. If RSS still
exceeds the server limit, those fields must be moved into disk-backed
columns, with query-side mmap access and generation-safe publication.

Finally, compilation output is not fully atomically published in the
existing application. Build into an isolated staging directory and
restart/switch the query service once all files are complete.
