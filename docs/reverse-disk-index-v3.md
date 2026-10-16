# Dense column migration / reverse index continuation

## Current implemented scope
- A bounded external merge sort of (place_id, house_idx) pairs, with 32-way fan-in, ensuring lowest house index is assigned for every place.
- Compiler writes reverse mappings as spill records rather than performing random updates to a 192M-entry vector during house grouping.
- Adds `--reverse-sort-buffer-mib` (64 MiB default) and a unit test with duplicate place IDs and missing records.
- Bridges into legacy rkyv serialization by materializing the sorted column as a vector **at the end**.

## Critically still required
This change does NOT finish the compiler/query dense-column migration. It removes the long-lived reverse array during address building, but still materializes that array for legacy serialization. It adds extra disk I/O and should not be merged as a complete OOM fix.

To complete the transition:
1. Replace the legacy final vector materialization with a published `cypress_dense.bin` file, keeping only a compact index metadata record in rkyv.
2. Stream each house and place column through `DiskColumn` rather than allocating `Vec`s.
3. Expose an overlay view in the query server with `Deref<Target = ArchivedCypressMemDb>` for the retained fields, typed mmap slices for moved fields, and full backward-compatible loading.
4. Change all search helper signatures to accept the overlay and migrate offset handling to `u64` where appropriate.
5. Verify index format and dictionary fingerprints, generation-safe publication, and exact ranking equivalence.
6. Revisit term/hierarchy dictionary builders, which still contain unbounded in-memory buckets.

## Validation
Rust toolchain is unavailable in the current execution environment. Run `cargo fmt --all`, `cargo test --bin compiler`, `cargo test --bin query`, and `cargo check --all-targets` before merging.
