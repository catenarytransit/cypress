# Places V2 rollout

The importer and compiler use `cypress.places_v2` exclusively; there is no feature flag.
Existing legacy tables are preserved for rollback.

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
