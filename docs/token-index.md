# PR 3 — Optional compact token index

The compiler builds a separate `cypress_tokens.bin` over the existing canonical
`StringId`s. It does not change CypressMemDb or the query algorithm.

## Build and measure

```sh
cargo run --release --bin compiler -- --out-dir ./data/compiled --token-index
cargo test --bin compiler
cargo test --lib token_index
cargo fmt --all -- --check
```

The compiler reports token and posting counts, encoded postings and FST byte sizes,
and sidecar growth as a percent of `cypress_index.bin + cypress_places.bin`.
`--token-index` is opt-in. Without it, old sidecars are removed on successful
recompilation to avoid retaining a file from a mismatched string dictionary.

## Binary format

Header: magic CYPTOK01 (8), format version (u32), analyzer version (u32),
seven u64 values: string count, dictionary fingerprint, token count,
posting count, FST bytes, posting bytes, and skip count.

Sections: sorted token FST, posting offsets (T+1 u64), frequencies (T u32),
skip offsets (T+1 u64), skip entries (u32 last ID plus u64 absolute byte end),
and delta-varint encoded sorted StringIds. Skip groups hold at most 128 IDs.
Query-side consumers must validate the dictionary fingerprint, version,
array sizes and bounds before using sidecar StringIds.

## Global and multilingual behavior

Token extraction iterates the existing lowercased multilingual string dictionary.
All source languages use the same token lookup; each alias remains a separate
searchable string mapped back to its current ID. This PR does not infer a
unique primary language or attach new name semantics.

Whitespace and punctuation split tokens. Unicode letters, digits and common
combining marks are preserved. Complex word segmentation for CJK/Thai and
new transliterations are intentionally deferred; this token index is an
additive experimental access path, not a replacement for legacy search.

## Resource limits

The compiler writes sorted runs of 500,000 pairs, then merges at most 32 runs
at a time. The current implementation stores token-directory and skip-offset
metadata in memory, but does not hold all posting IDs or full name strings
again. Temporary disk space is required during compilation.

## Subsequent PR

PR 4 will implement selective intersections, corpus-aware candidate joins
and query work benchmarks; PR 3 alone cannot improve query latency.
