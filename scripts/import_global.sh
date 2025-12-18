#!/bin/bash
# Global Import Script for Cypress
#
# Usage: ./import_global.sh [options]
#
# Options:
#   --download        Download PBFs even if they exist
#   --wikidata        Also fetch Wikidata labels
#   --fresh           Delete and recreate the index before import
#   --no-filter       Skip osmium pre-filtering
#   --url <url>       Custom Elasticsearch URL (default: http://localhost:9200)
#
# This script downloads, filters, and imports data for multiple global regions.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="${PROJECT_DIR}/data"

# Region Definitions
# Format: "Name|URL"
REGIONS=(
    "Europe|https://download.geofabrik.de/europe-latest.osm.pbf"
    "China|https://download.geofabrik.de/asia/china-latest.osm.pbf"
    "Malaysia_Singapore_Brunei|https://download.geofabrik.de/asia/malaysia-singapore-brunei-latest.osm.pbf"
    "South_Korea|https://download.geofabrik.de/asia/south-korea-latest.osm.pbf"
    "Japan|https://download.geofabrik.de/asia/japan-latest.osm.pbf"
    "Thailand|https://download.geofabrik.de/asia/thailand-latest.osm.pbf"
    "North_America|https://download.geofabrik.de/north-america-latest.osm.pbf"
    "South_America|https://download.geofabrik.de/south-america-latest.osm.pbf"
    "Australia_Oceania|https://download.geofabrik.de/australia-oceania-latest.osm.pbf"
)

# Parse arguments
DOWNLOAD=false
WIKIDATA=""
FRESH_FLAG=""
NO_FILTER=false
IS_FIRST_IMPORT=true
ES_URL="http://localhost:9200"

# Use ELASTICSEARCH_URL env var if set
if [ -n "$ELASTICSEARCH_URL" ]; then
    ES_URL="$ELASTICSEARCH_URL"
fi

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --download)
            DOWNLOAD=true
            shift
            ;;
        --wikidata)
            WIKIDATA="--wikidata"
            shift
            ;;
        --fresh)
            FRESH_FLAG="true"
            shift
            ;;
        --no-filter)
            NO_FILTER=true
            shift
            ;;
        --url)
            ES_URL="$2"
            shift 2
            ;;
        *)
            # Ignore unknown args or warn? 
            # Previous script just ignored known flags if loop was simple `for arg in "$@"` but `case` matches.
            # If we see unknown, we should probably warn or ignore. 
            # But the original script had a simple loop that just matched known flags.
            echo "Unknown option: $1"
            shift # Just skip it
            ;;
    esac
done

# Ensure data directory exists
mkdir -p "$DATA_DIR"

echo "=== Cypress Global Import ==="
echo "Regions: ${#REGIONS[@]}"
echo "Elasticsearch URL: $ES_URL"
if [ -n "$FRESH_FLAG" ]; then
    echo "Mode: FRESH IMPORT (Index will be recreated)"
else
    echo "Mode: Append/Update"
fi
echo

# Iterate over regions
for region in "${REGIONS[@]}"; do
    IFS="|" read -r NAME URL <<< "$region"
    
    echo "----------------------------------------------------------------"
    echo "Processing Region: $NAME"
    echo "----------------------------------------------------------------"
    
    FILENAME=$(basename "$URL")
    RAW_PBF="${DATA_DIR}/${FILENAME}"
    FILTERED_PBF="${DATA_DIR}/${FILENAME%.osm.pbf}-filtered.osm.pbf"
    
    # 1. Download
    if [ ! -f "$RAW_PBF" ] || [ "$DOWNLOAD" = true ]; then
        echo "Downloading $NAME..."
        curl -L -o "$RAW_PBF" "$URL"
        # Force re-filter if new download
        rm -f "$FILTERED_PBF"
    else
        echo "Using existing file: $RAW_PBF"
    fi
    
    # 2. Filter
    PBF_TO_IMPORT="$RAW_PBF"
    if [ "$NO_FILTER" = false ]; then
        if command -v osmium &> /dev/null; then
            if [ ! -f "$FILTERED_PBF" ] || [ "$RAW_PBF" -nt "$FILTERED_PBF" ]; then
                echo "Filtering $NAME..."
                "$SCRIPT_DIR/filter_osm.sh" "$RAW_PBF" "$FILTERED_PBF"
            else
                echo "Using cached filtered file: $FILTERED_PBF"
            fi
            PBF_TO_IMPORT="$FILTERED_PBF"
        else
             echo "Warning: osmium-tool not found, skipping filter."
        fi
    fi
    
    # 3. Import
    # Only pass --create-index (FRESH) for the very first region if requested
    CURRENT_FRESH_ARG=""
    if [ "$IS_FIRST_IMPORT" = true ] && [ -n "$FRESH_FLAG" ]; then
        CURRENT_FRESH_ARG="--create-index"
        IS_FIRST_IMPORT=false
    fi
    
    echo "Importing $NAME into Elasticsearch..."
    cd "$PROJECT_DIR"
    
    cargo run --release --bin ingest -- \
        --file "$PBF_TO_IMPORT" \
        --es-url "$ES_URL" \
        --refresh \
        $WIKIDATA \
        $CURRENT_FRESH_ARG
        
    echo "Finished $NAME"
    echo
done

echo "=== Global Import Complete ==="
