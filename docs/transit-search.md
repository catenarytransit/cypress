# Transit imports and search preferences

Cypress imports named station-level OSM destinations as `venue` results:
railway stations and halts, metro/light rail/monorail stations, tram stops,
bus stops and bus stations, and ferry terminals. Nodes, ways, and station
multipolygons use the existing geometry and administrative lookup pipeline.
Unclassified `public_transport=station` features remain searchable without a
train ranking bonus. This does not import GTFS or synthesize stations from
`stop_area` relations that contain only platforms.

Mode classification follows the explicit `station=*` and mode-tag approach in
[the backend station importer](https://github.com/catenaryTransit/catenary-backend/blob/main/src/osmstationimport/main.rs).
Explicit modes take precedence over generic `railway=station`; a facility with
`train=yes` uses the highest rail mode even when it also serves metro trains.
Light rail and monorail use the metro tier. Platforms, stop positions, station
entrances, proposed/disused/abandoned facilities and Disney Parks stations are
excluded. Platforms are rejected even if they carry names, addresses or other
POI tags. Individual opposite-direction bus stops retain their OSM identities.

## HTTP options

Both Boolean parameters default to `false` and are supported on `/v1/search`,
`/v2/search` and `/v1/autocomplete`:

| Parameter | Behavior |
| --- | --- |
| `prefer_train_stations=true` | Strengthens rail → metro/light rail → tram → bus ranking for comparable matches. |
| `suppress_cities_near_stations=true` | Removes a city dominated by a nearby matching rail station, or a neighbourhood dominated by a nearby matching metro/tram station. |

For example:

```text
/v2/search?text=Cambridge&prefer_train_stations=true&suppress_cities_near_stations=true
```

The transit score bonus is applied during candidate admission, preliminary
ranking, and final structured ranking, before top-K truncation:

| Mode | Default bonus | With preference |
| --- | ---: | ---: |
| Rail | 1.00 | 8.00 |
| Metro/light rail/monorail | 0.75 | 6.00 |
| Tram | 0.50 | 4.00 |
| Bus | 0.25 | 2.00 |

Text, administrative context, population and geographic focus continue to
contribute to the overall score. The hierarchy is a relevance prior, not an
unconditional sort of unrelated stations above exact address matches. Existing
importance and population values are not rewritten.

Suppression requires all of the following:

- The station was successfully hydrated, passes the request's layer and
  geographic filters, ranks strictly above the area, and will be returned.
- Rail station versus `locality`: at most 3 km between result coordinates.
  Metro/tram versus `neighbourhood`: at most 1 km.
- Matching known country and compatible administrative context. The station
  references the area's boundary ID, or both share a locality/local-admin ID.
- Similar multilingual names or aliases after case/punctuation normalization
  and removal of station designators such as “Station”, “Bahnhof”, or “Gare de”.
  Qualifiers such as North, Central and Airport remain significant. Fuzzy
  matching permits at most one edit for names of at least eight characters,
  with equal token counts. The station must match the query at least as well
  as the area under the existing lexical scorer.

Bus stops never suppress areas. Metro/tram stations do not suppress cities.
Suppression and layer filtering retain a bounded pool of `max(40, 4 * size)`
candidates, capped at 160, then refill the requested list from that pool.
Requests can still return fewer results when the candidate pool is exhausted.
Missing or filtered stations leave the corresponding area eligible.

Ordinary autocomplete, including preference-only requests, remains mmap-only.
Autocomplete with suppression or layer filtering uses the same Scylla hydration
and finalization as search; its existing `memdb_took_ms` response field includes
that additional work. Place hydration remains limited to eight concurrent
fetches, and administrative records are fetched only for surviving results.

## Reimport and rollout

Previously filtered PBFs do not contain the newly enabled transit features.
Recreate them from raw PBFs using `scripts/filter_osm.sh`. The global and
Switzerland wrapper scripts also invalidate their filtered cache when the
filter configuration or filter script changes.

Reimport with `--refresh` so unchanged PBF hashes cannot skip the new import
and obsolete platform documents from earlier imports are removed. For batch
imports:

```bash
cargo run --release --bin ingest -- batch --config regions.toml --refresh
cargo run --release --bin compiler -- --out-dir ./data/compiled
```

For a single region, run the existing `ingest single --file ... --refresh`
command on the newly filtered or raw PBF before compiling.

The compiler streams one additional byte per place into dense sidecar format
v2. Its transit spill uses the same worker order as the place records, with
per-worker count validation and a fixed 64 KiB copy buffer. The existing rkyv
archive layout and fixed-width place record file are unchanged.

Deploy the updated query binary before publishing a v2 generation. It reads
both dense v1 and v2; v1 generations lack transit ranking metadata and need a
recompile to enable the new ranking. Older query binaries reject v2, so a
rollback must restore the older compiled generation as well.

Tests cover import/normalization, platform exclusion, dense v1/v2 compatibility
and malformed column lengths, actual mmap search ranking before top-1
truncation, warmed-query option isolation, parameter parsing, multilingual
suppression, neighbourhood rules, distance/context/qualifier guards, missing
or filtered stations, and result refill. These fixtures do not measure global
import volume or production latency; compare those on the transit-expanded
corpus before enabling the optional policies in clients.
