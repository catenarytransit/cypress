# Cypress

A Rust-based geocoding system with Elasticsearch, inspired by [Pelias](https://pelias.io/) and [Nominatim](https://nominatim.org/).

![1200x680](https://github.com/user-attachments/assets/496f0dba-7e6d-4b50-90cc-744f21909ece)


## Features

- **OSM PBF Ingestion** - Parses OpenStreetMap data with multilingual name support
- **Point-in-Polygon Admin Lookup** - Assigns administrative hierarchy to each place using R-tree spatial indexing
- **Elasticsearch Backend** - Full-text search with edge n-gram autocomplete
- **Wikidata Integration** - Enriches place names with multilingual labels from Wikidata
- **Location & Bounding Box Bias** - Boost results near user's location or viewport
- **Data Refresh** - Re-import files with automatic stale document cleanup

## Requirements

- Rust 1.70+
- Elasticsearch 8.x
- 8GB+ RAM for Switzerland import

## Quick Start

### 1. Start Elasticsearch

```bash
docker run -d --name cypress-es -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  docker.elastic.co/elasticsearch/elasticsearch:8.11.0
```

### 2. Build

```bash
cargo build --release
```

### 3. Import Data

```bash
# Configure regions.toml and run:
cargo run --release --bin ingest -- batch --config regions.toml

# Or run directly:
cargo run --release --bin ingest -- \
  --file switzerland-latest.osm.pbf \
  --create-index \
  --refresh \
  --wikidata
```

### 4. Start Query Server

```bash
cargo run --release --bin query -- --listen 0.0.0.0:3000
```

## Data Management

### Wiping a Region

If you need to remove data for a specific region (e.g., to re-import it or free up space), you can use the `wipe_region.sh` script:

```bash
# Wipe data for Albania
./scripts/wipe_region.sh Albania

# Wipe data using a custom Elasticsearch URL
./scripts/wipe_region.sh Germany --url http://10.0.0.5:9200
```

The script identifies the correct records using the `source_file` field based on the regions defined in `scripts/import_global.sh`.

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
