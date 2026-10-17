# Dense-compatible candidate shortlist crash fix

## Incident
With `CYPRESS_CANDIDATE_SHORTLIST=shadow` or `on`, a request panicked in `src/query/search.rs` near line 1760:

```text
index out of bounds: the len is 0 but the index is 110538418
```

The compiler stores `place_importances` in `cypress_dense.bin` and intentionally leaves the archived `CypressMemDb.place_importances` empty. PR #17 accessed the archived array instead of the dense-backed `QueryDbView` column.

## Fix
- Add `place_importances` to `QueryDbView`, selecting dense mmap values if present and archived values for older datasets.
- Use `best_mapped_importance` for shortlist ordering, retaining the original population/layer prior **and** the actual per-place importance (rather than replacing importance with zero, which would change ranking).
- Ignore invalid place IDs and tolerate absent columns defensively in the experimental helper.
- Check dictionary and posting offset ranges before using candidate StringIds.
- Replace the expensive `shadow` coverage linear posting scans with sorted posting binary searches; invalid IDs and ranges simply count as uncovered rather than panicking.
- Add regression tests including the specific crash ID, valid legacy/dense columns, out-of-range StringIds, and shadow diagnostics.

## Deployment
This is a **query-only** code change. The existing compiled dataset, token FST, dense sidecar, and Scylla database do **not** need to be rebuilt.

```sh
cargo fmt --all -- --check
cargo test --bin query candidate_budget
cargo check --all-targets
cargo build --release --bin query
sudo systemctl restart query
```

Check the actual systemd unit is running the rebuilt binary; the service may be named `cypress` rather than `query`.

Use `CYPRESS_CANDIDATE_SHORTLIST=shadow` to measure proposed shortlists without changing results. `on` remains approximate and opt-in. Set `off` to roll back the experiment without affecting `CYPRESS_TOKEN_RETRIEVAL_MODE`.

## Verification
The code was reviewed through GitHub but **Rust compilation and runtime tests could not be executed** in this environment. Validate both legacy rkyv and dense compiled datasets if available. Verify `shadow` and `on` with `Miami Florida`, `Toronto`, `Vancouver`, `UC Irvine`, `4885 Paseo De Vega Irvine`, and international POI/address cases.

This fixes the known bounds panic and similar unsafe accesses within the shortlist experiment, not every unrelated panic elsewhere in the query engine.
