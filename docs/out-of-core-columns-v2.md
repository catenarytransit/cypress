# Out-of-core compiler migration: second PR

## Implemented in this branch
- Stream the multilingual place-prefix FST to a temporary file while constructing it instead of building it into a growing `MapBuilder::memory()` heap allocation. The canonical bytes are read back into the existing rkyv field after construction, retaining backward-compatible query behavior.
- Explicitly release hierarchy intern/dictionary tables after Pass 1.5c, before large address and bigram work. The retained compact area-set arrays are unchanged.
- Introduce a versioned, little-endian, 8-byte-aligned dense-column sidecar schema with strict section bounds, a name/dictionary fingerprint field, typed zero-copy views, and initial malformed-input tests. **This sidecar schema is not yet wired into the compiler/query process and does not currently reduce memory by itself.**

## Work still needed before removing the OOM risk
1. Write the large place/house columns directly to disk via the existing `DiskColumn<T>` writer; do not create the original `Vec` allocations in addition to the files.
2. Build `place_address_house_indices` via sorted `(place_id, house_idx)` spill records rather than a 192-million-element vector.
3. Complete the `DenseColumnsView` implementation and migrate all query read paths using an index-accessor compatibility layer; ensure support for old rkyv generations.
4. Externalize name/area/street columns and remove large allocations from the final `CypressMemDb` serializer.
5. Resolve generation-safe publication and measure peak cgroup memory with full production data.

## Verification
Checks have **not** been executed in this environment. Before merging run:

```sh
cargo fmt --all
cargo test --lib
cargo test --bin compiler
cargo test --bin query
cargo check --all-targets
```

Do not treat this branch as a complete bounded-memory compiler.
