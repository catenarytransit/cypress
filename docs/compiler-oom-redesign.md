# Compiler OOM: bounded address-bucket sorting (draft, incomplete)

This branch begins the larger out-of-core compiler redesign requested after the compiler was killed immediately following Pass 2b.

## Implemented
- Replace the unbounded `Vec<AddressBucketEntry>` per hash bucket with externally sorted runs.
- Preserve the existing comparison order `(anchor, area_set, house_number, postcode, place_id)` including duplicate rows.
- Perform 32-way heap merges and stream street/house groups from the sorted file instead of materializing an entire address bucket.
- Add `--address-sort-buffer-mib` (default 64 MiB).
- Add a sequential `DiskColumn<T>` primitive for later migration of the final dense column arrays.
- Include a deterministic ordering/duplicate preservation unit test.

## Not yet implemented: the larger OOM fix
**This branch is not sufficient to guarantee a bounded-memory full compilation.** The compiler still keeps all dense house/place/reverse-index vectors in RAM, and the rkyv index construction still receives all of them. The token sidecar and FST, hierarchy, term bucket, string/street mapping sorts may also have unbounded memory requirements.

Follow-up changes within this PR should migrate those fields to versioned sidecars, update query readers for new/legacy formats, eliminate the large final resident `CypressMemDb`, and publish an immutable generation atomically.

## Validation
Not run locally: the execution environment does not contain a Rust toolchain or a checkout of this repository. GitHub connector file updates were used. Before merging:

```sh
cargo fmt --all
cargo test --bin compiler
cargo test --bin query
cargo check --all-targets
```

For the 192M place / 152M house build, run under the production cgroup limit, record `memory.peak`, `memory.events`, `RssAnon` and `RssFile`, and compare output IDs, address group order, and search results against an existing generation.
