#!/bin/bash
# Import Switzerland OSM data into Cypress/Elasticsearch
#
# Usage: ./import_switzerland.sh [options]
#
# Options:
#   --download    Download the latest Switzerland PBF even if file exists
#   --wikidata    Also fetch Wikidata labels for multilingual enrichment
#   --fresh       Delete and recreate the index before importing
#   --no-filter   Skip the osmium pre-filter step
#   --discord-webhook <url> Discord webhook URL for notifications
#
# Prerequisites:
#   - Elasticsearch running on localhost:9200
#   - Rust toolchain installed
#   - osmium-tool (for filtering): apt install osmium-tool

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="${PROJECT_DIR}/data"
PBF_URL="https://download.geofabrik.de/europe/switzerland-latest.osm.pbf"
RAW_PBF="${DATA_DIR}/switzerland-latest.osm.pbf"
FILTERED_PBF="${DATA_DIR}/switzerland-filtered.osm.pbf"

# Check if file exists in project root (already downloaded)
if [ -f "${PROJECT_DIR}/switzerland-latest.osm.pbf" ]; then
    RAW_PBF="${PROJECT_DIR}/switzerland-latest.osm.pbf"
fi

# Parse arguments
DOWNLOAD=false
WIKIDATA=""
FRESH=""
NO_FILTER=false
DISCORD_WEBHOOK=""

for arg in "$@"; do
    case $arg in
        --download)
            DOWNLOAD=true
            ;;
        --wikidata)
            WIKIDATA="--wikidata"
            ;;
        --fresh)
            FRESH="--create-index"
            ;;
        --no-filter)
            NO_FILTER=true
            ;;
        --discord-webhook)
            shift
            DISCORD_WEBHOOK="--discord-webhook $1"
            ;;
    esac
done

echo "=== Cypress Switzerland Import ==="
echo

mkdir -p "$DATA_DIR"

# Download if needed
if [ ! -f "$RAW_PBF" ] || [ "$DOWNLOAD" = true ]; then
    echo "Downloading Switzerland OSM data..."
    RAW_PBF="${DATA_DIR}/switzerland-latest.osm.pbf"
    curl -L -o "$RAW_PBF" "$PBF_URL"
    echo "Downloaded to $RAW_PBF"
    # Force re-filter if we downloaded fresh data
    rm -f "$FILTERED_PBF"
fi

echo "Raw PBF: $RAW_PBF ($(du -h "$RAW_PBF" | cut -f1))"

# Filter the PBF to reduce memory usage
if [ "$NO_FILTER" = false ]; then
    if command -v osmium &> /dev/null; then
        # General filter
        if [ ! -f "$FILTERED_PBF" ] || [ "$RAW_PBF" -nt "$FILTERED_PBF" ]; then
            echo
            echo "Pre-filtering OSM data (reduces memory usage)..."
            "$SCRIPT_DIR/filter_osm.sh" "$RAW_PBF" "$FILTERED_PBF"
        else
            echo "Using cached filtered file: $FILTERED_PBF ($(du -h "$FILTERED_PBF" | cut -f1))"
        fi
        PBF_FILE="$FILTERED_PBF"

        # Admin filter
        ADMIN_PBF="${DATA_DIR}/switzerland-admins.osm.pbf"
        if [ ! -f "$ADMIN_PBF" ] || [ "$RAW_PBF" -nt "$ADMIN_PBF" ]; then
            echo "Filtering admin boundaries..."
            "$SCRIPT_DIR/filter_admins.sh" "$RAW_PBF" "$ADMIN_PBF"
        else
            echo "Using cached admin file: $ADMIN_PBF"
        fi
    else
        echo "Warning: osmium-tool not found, skipping pre-filter step"
        echo "  Install with: apt install osmium-tool"
        PBF_FILE="$RAW_PBF"
        ADMIN_PBF=""
    fi
else
    PBF_FILE="$RAW_PBF"
    ADMIN_PBF=""
fi

echo
echo "Importing: $PBF_FILE ($(du -h "$PBF_FILE" | cut -f1))"

# Check Elasticsearch is running
echo
echo "Checking Elasticsearch..."
if ! curl -s "http://localhost:9200" > /dev/null; then
    echo "ERROR: Elasticsearch is not running on localhost:9200"
    echo
    echo "Start it with Docker:"
    echo "  docker run -d --name cypress-es -p 9200:9200 \\"
    echo "    -e \"discovery.type=single-node\" \\"
    echo "    -e \"xpack.security.enabled=false\" \\"
    echo "    docker.elastic.co/elasticsearch/elasticsearch:8.11.0"
    exit 1
fi
echo "Elasticsearch is running"

# Build in release mode
echo
echo "Building Cypress (release mode)..."
cd "$PROJECT_DIR"
cargo build --release

# Run ingest
echo
echo "Starting import..."
cargo run --release --bin ingest -- \
    --file "$PBF_FILE" \
    --admin-file "$ADMIN_PBF" \
    --refresh \
    $WIKIDATA \
    $FRESH \
    $DISCORD_WEBHOOK

# Show stats
echo
echo "=== Import Complete ==="
echo
echo "Document count:"
curl -s "http://localhost:9200/places/_count" | jq .

echo
echo "Sample search for 'Zurich':"
curl -s "http://localhost:9200/places/_search?q=name.default:Zurich&size=3" | jq '.hits.hits[]._source | {name: .name.default, layer: .layer, locality: .parent.locality.name}'

echo
echo "Done! Start the query server with:"
echo "  cargo run --release --bin query"
