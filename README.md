# Cypress

A Rust-based geocoding system with Elasticsearch, inspired by [Pelias](https://pelias.io/) and [Nominatim](https://nominatim.org/).

![1200x680](https://github.com/user-attachments/assets/496f0dba-7e6d-4b50-90cc-744f21909ece)


## Features

- **OSM PBF Ingestion** - Parses OpenStreetMap data with multilingual name support
- **Road Way Merging** - Automatically merges adjacent road segments with the same name to reduce disk space usage
- **Point-in-Polygon Admin Lookup** - Assigns administrative hierarchy to each place using R-tree spatial indexing
- **Elasticsearch Backend** - Full-text search with edge n-gram autocomplete
- **Wikidata Integration** - Enriches place names with multilingual labels from Wikidata
- **Location & Bounding Box Bias** - Boost results near user's location or viewport
- **Data Refresh** - Re-import files with automatic stale document cleanup

## Requirements

- Rust 1.70+
- Elasticsearch 8.x
- ScyllaDB 5.x+
- 8GB+ RAM for Switzerland import

## Quick Start

### 1. Prerequisites

Ensure you have the following services installed and running:

- **Elasticsearch 8.x** (Default: http://localhost:9200)
- **ScyllaDB 5.x+** (Default: 127.0.0.1:9042)

### 2. Build

```bash
cargo build --release
```

### 3. Import Data

```bash
# Configure regions.toml and run:
cargo run --release --bin ingest -- batch --config regions.toml

# Or run directly (Scylla defaults to 127.0.0.1):
cargo run --release --bin ingest -- single \
  --file switzerland-latest.osm.pbf \
  --create-index \
  --refresh \
  --wikidata \
  --scylla-url 127.0.0.1

# With custom Scylla and Elasticsearch URLs:
cargo run --release --bin ingest -- single \
  --file switzerland-latest.osm.pbf \
  --es-url http://elasticsearch:9200 \
  --scylla-url 10.0.0.5 \
  --create-index \
  --refresh
```

### 4. Start Query Server

```bash
cargo run --release --bin query -- --listen 0.0.0.0:3000
```

## Data Management

### Road Way Merging

By default, Cypress automatically merges adjacent road segments (ways) that share the same name and highway type. This provides significant benefits:

**Benefits:**
- **Reduced disk space**: Fewer documents stored in Elasticsearch
- **Faster search**: Less data to scan during queries
- **Cleaner results**: One result per street instead of dozens of segments
- **Lower costs**: Reduced storage and compute requirements

**How it works:**
1. During ingestion, all road ways with names are collected
2. Ways are grouped by name and highway type (e.g., "Main Street|residential")
3. Adjacent ways (sharing endpoint nodes) are merged into single road segments
4. The merged road is indexed with its full geometry and bounding box
5. A category tag indicates how many ways were merged (e.g., `merged_ways:5`)

**Which roads are merged:**
- Residential streets, primary/secondary/tertiary roads
- Service roads, living streets, pedestrian ways
- Tracks, footways, cycleways, and paths
- **NOT merged**: Motorways, motorway links, and other link roads

You can disable this feature with `--merge-roads false`, but this is not recommended for production use.

### Wiping a Region

If you need to remove data for a specific region (e.g., to re-import it or free up space), you can use the `wipe_region.sh` script:

```bash
# Wipe data for Albania
./scripts/wipe_region.sh Albania

# Wipe data using a custom Elasticsearch URL
./scripts/wipe_region.sh Germany --url http://10.0.0.5:9200
```

The script identifies the correct records using the `source_file` field based on the regions defined in `scripts/import_global.sh`.

### index management

Deleting places index
```bash
curl -X DELETE "http://localhost:9200/places"
```

Deleting versions index
```bash
curl -X DELETE "http://localhost:9200/cypress_versions"
```

or use the wipe versions script:
```bash
cargo run --bin ingest -- reset-versions
```

## API Endpoints

### Forward Geocoding

```bash
# Basic search
curl "http://localhost:3000/v1/search?text=Zurich"

# With language preference
curl "http://localhost:3000/v1/search?text=Genève&lang=fr"

# With bounding box filter
curl "http://localhost:3000/v1/search?text=bahnhof&bbox=8.5,47.3,8.6,47.4"

# With location bias
curl "http://localhost:3000/v1/search?text=restaurant&focus.point.lat=47.37&focus.point.lon=8.54"
```

### Reverse Geocoding

```bash
curl "http://localhost:3000/v1/reverse?point.lat=47.37&point.lon=8.54"
```

### Autocomplete

```bash
curl "http://localhost:3000/v1/autocomplete?text=zur"
```

## Response Format

Results are returned in GeoJSON-like format with all available language variants:

```json
{
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [8.54, 47.37]
      },
      "properties": {
        "id": "node/123456",
        "layer": "locality",
        "name": "Zürich",
        "names": {
          "default": "Zürich",
          "de": "Zürich",
          "fr": "Zurich",
          "it": "Zurigo",
          "en": "Zurich"
        },
        "country": "Switzerland",
        "region": "Zürich",
        "confidence": 42.5
      }
    }
  ]
}
```

## Architecture

```
cypress/
├── src/
│   ├── lib.rs              # Shared library
│   ├── models/             # Place, AdminHierarchy, etc.
│   ├── elasticsearch/      # ES client, schema, bulk indexer
│   ├── pip/                # Point-in-Polygon admin lookup
│   ├── wikidata/           # SPARQL label fetcher
│   ├── ingest/             # OSM PBF import binary
│   └── query/              # HTTP query server
└── schema/
    └── places_mapping.json # Elasticsearch index mapping
```

## License

MIT
