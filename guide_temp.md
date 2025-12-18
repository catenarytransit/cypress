# Cypress Geocoding System - Walkthrough
## Overview
Built a complete Rust-based geocoding system with Elasticsearch, inspired by Pelias architecture but implemented cleanly in Rust.
## Components Created
### Core Library ([src/lib.rs](file:///home/kyler/cypress/src/lib.rs))
- **Models** - [Place](file:///home/kyler/cypress/src/models/place.rs#91-139), `AdminLevel`, [AdminHierarchy](file:///home/kyler/cypress/src/models/admin.rs#172-200), [AdminEntry](file:///home/kyler/cypress/src/models/admin.rs#134-151) for document structure
- **Elasticsearch** - Client wrapper, schema creation, bulk indexer with batching
- **PIP Service** - Point-in-Polygon admin lookup with R-tree spatial indexing
- **Wikidata** - SPARQL fetcher for multilingual label enrichment
### Ingest Pipeline ([src/ingest/main.rs](file:///home/kyler/cypress/src/ingest/main.rs))
CLI binary that:
1. Parses OSM PBF files using `osmpbfreader`
2. Extracts admin boundaries with multilingual names
3. Builds R-tree spatial index for PIP lookups
4. Processes places (nodes with names/POI tags)
5. Assigns denormalized parent hierarchy via PIP
6. Optionally enriches with Wikidata labels
7. Bulk indexes into Elasticsearch
8. Supports refresh mode (deletes stale documents)
### Query Server ([src/query/main.rs](file:///home/kyler/cypress/src/query/main.rs))
Axum HTTP server with endpoints:
- `GET /v1/search` - Forward geocoding with full-text search
- `GET /v1/autocomplete` - Edge n-gram autocomplete
- `GET /v1/reverse` - Reverse geocoding by coordinates
- `GET /health` - Health check
Features:
- Multi-match across `name.*` fields (all languages)
- Gaussian decay scoring for location bias
- Bounding box filtering
- Language preference for result ordering
- Returns all language variants in response
### Elasticsearch Schema ([schema/places_mapping.json](file:///home/kyler/cypress/schema/places_mapping.json))
Custom analyzers:
- `peliasIndex` - Tokenizer with ASCII folding
- `peliasQuery` - Adds synonym expansion
- `peliasAutocomplete` - Edge n-grams for partial matching
Dynamic templates for multilingual `name.*` and `parent.*.name_*` fields.
## Verification
### Build Status
cargo build --release ✓ Finished in 1m 14s

### Files
| Path | Description |
|------|-------------|
| `src/models/` | Place, AdminLevel, AdminHierarchy types |
| `src/elasticsearch/` | Client, schema, bulk indexer |
| `src/pip/` | Admin boundary extraction and PIP service |
| `src/wikidata/` | SPARQL label fetcher |
| `src/ingest/` | OSM import binary |
| `src/query/` | HTTP query server |
| `schema/places_mapping.json` | ES index mapping |
| `scripts/import_switzerland.sh` | Import automation |
## Usage
### Import Switzerland
```bash
./scripts/import_switzerland.sh --fresh
Run Query Server
cargo run --release --bin query
Example Queries
# Search
curl "http://localhost:3000/v1/search?text=Zurich"
# Autocomplete
curl "http://localhost:3000/v1/autocomplete?text=zur"
# Reverse geocode
curl "http://localhost:3000/v1/reverse?point.lat=47.37&point.lon=8.54"
Multilingual Support
Extracts all name:* tags from OSM (de, fr, it, en, etc.)
Stores in ES as name.de, name.fr, etc.
Searchable across all languages via multi_match
Optional Wikidata enrichment with --wikidata flag
Response includes all available languages in names object
