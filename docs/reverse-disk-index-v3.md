# Out-of-core compiler/query integration (PR #12)

This patch fixes the compiler's previous `bytemuck::PodCastError` error by **removing** the late reverse-map `try_cast_slice(...)? .to_vec()` entirely. The query process now accesses a versioned sidecar.

## Compiler

- Build the place coordinate, importance, population, source-ID, and layer columns using sequential disk writers, not enormous `Vec`s.
- Retain read-only file mappings of latitude/longitude during the existing proximity-based name deduplication. Their values and StringIds are unchanged.
- Build place-to-area-set mappings with an external merge sort of `(place_id, area_set_idx)` pairs; missing places receive `u32::MAX`.
- Stream house-to-street, house-to-place, normalized house number and postcode columns directly to disk.
- Build place-to-house mappings with a bounded external merge sort, preserving the first (lowest indexed) house for each place.
- Stream completed string-to-place posting lists to disk; use a phrase-local buffer for proximity deduplication. StringIds, posting order, and duplicate-suppression logic remain the same.
- Assemble these 16 sections into `cypress_dense.bin`, using aligned, length-prefixed sections and a dictionary fingerprint; validate the staged file before publishing it.
- Keep corresponding vectors empty in the new rkyv index to avoid serializing the same gigabytes twice.
- Publish the sidecar before updating `current_version.txt`.

## Query server

- Map and validate `cypress_dense.bin` at generation load.
- Check its dictionary fingerprint against the compiled string names and gram counts, its place count against `cypress_places.bin`, and its format and section lengths.
- `QueryDbView` presents read-only borrowed slices for the moved fields and dereferences all unchanged rkyv fields.
- Use the same search scoring, address lookup, reverse search and ranking paths against the new slices.
- If no dense sidecar exists, use the old embedded rkyv columns. Reject a new index with empty embedded place columns and no dense sidecar.
- Support 64-bit offsets for the address number/postcode arenas in new indexes. Old rkyv indexes continue to use 32-bit offsets.

## Tests / validation

- Reverse-index fixture: duplicate records choose the earliest indexed house, missing IDs get `u32::MAX`.
- Dense-sidecar fixture: correct sections and typed values, rejected malformed section offsets.
- Existing compiler/query tests should continue to pass.

Run locally:

```sh
cargo fmt --all
cargo test --lib
cargo test --bin compiler
cargo test --bin query
cargo check --all-targets
cargo build --release --bin compiler --bin query
```

Then compile into a **new, isolated** output directory, on the same input dataset as the old compiler, and verify:
1. `cypress_dense.bin` is present and query starts correctly.
2. New and old `/v1/search`, `/v2/search`, `/v1/reverse`, `/v2/reverse` match for the same dataset.
3. Memory `memory.peak`, `memory.events`, `RssAnon` and `RssFile` show lower peak pressure on production input.
4. No new output directory is served before all files have finished writing and version validation has succeeded.

## Remaining limitations

This eliminates the **major global place/house/posting Vecs**, but is not yet a proof of a completely constant-memory compiler. Term bucket sorting, per-phrase deduplication buffers, area-set interning, some street/admin/name vectors, and full name FST bytes are still in memory and may depend on input size. Read-only mmap pages still count towards resident memory while cached. The current multi-file hot-reload publication remains unsafe if output files are overwritten in place; use immutable generation directories for serving.

Do not claim the change has passed tests or a production OOM benchmark until they have actually run.
