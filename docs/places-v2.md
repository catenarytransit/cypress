# Places V2 rollout

The importer and compiler use `cypress.places_v2` exclusively. The query server can switch place-document hydration between `places_v2` (default) and the legacy `places` table (`--place-lookup-table places`). Existing legacy tables are preserved for rollback.

The compiler now emits `cypress_v2_lookup.bin`, an index-aligned source/generation provenance sidecar. It identifies the exact composite partition key for each compiled place, including deterministic handling of overlapping source extracts. Both search and reverse results use point lookups on `(source_file, generation, bucket, id)`, without `ALLOW FILTERING` or a source-wide scan. Bare `/place_details` ID lookups check active generations in deterministic source order.

**Deployment:** run the updated compiler and publish *all* compilation artifacts (including the new sidecar) before restarting the query service with V2 lookup. Query startup rejects a missing or invalid sidecar in V2 mode. The query CLI accepts `--place-lookup-table places_v2` (default) or `--place-lookup-table places` for rollback. Keep the legacy table and prior compiled dataset until parity is verified. V2 generation garbage collection must not delete a generation referenced by a still-serving compiled index; refresh/restart query services before collecting it.

## Migrating an existing database

```sh
./target/release/ingest migrate-places-v2 --scylla-url 127.0.0.1
./target/release/compiler --scylla-url 127.0.0.1
```

The migration streams existing legacy records into new per-source generations
and publishes after all writes succeed. It skips sources already active in V2.
Keep your old compiled index and a database backup until counts and search
parity have been verified. No existing place table is truncated.

New imports always use V2. The legacy `--create-index` option is rejected
because overwriting the entire dataset is no longer appropriate.

## Garbage collection

```sh
./target/release/ingest garbage-collect-places-v2 --scylla-url 127.0.0.1
```

Run this command periodically. It marks abandoned imports failed and deletes
inactive generations after a 48-hour grace period, using 256 partition deletes
per generation. It can be rerun following an interruption. Do not GC a
generation still needed by an unusually long-running compiler.

## Validation

```sh
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo test --locked --lib
CYPRESS_TEST_SCYLLA_URI=127.0.0.1:9042 cargo test --test places_v2 -- --ignored
```

Perform a full legacy-to-V2 migration on a disposable database, then compare
compiled counts and geocoder search results before using V2 in production.
