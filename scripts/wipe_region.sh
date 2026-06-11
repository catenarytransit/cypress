#!/bin/bash
# Wipe data for a specific region from Elasticsearch
#
# Usage: ./wipe_region.sh <region_name> [options]
#
# Options:
#   --url <url>       Custom Elasticsearch URL (default: http://localhost:9200)
#   --index <name>    Index name (default: places)
#
# Example: ./wipe_region.sh Albania

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
IMPORT_SCRIPT="${SCRIPT_DIR}/import_global.sh"

REGION_NAME="$1"
SCYLLA_HOST="127.0.0.1"
SCYLLA_PORT="9042"

if [ -z "$REGION_NAME" ]; then
    echo "Usage: $0 <region_name> [--host <host>] [--port <port>]"
    echo "Example: $0 Albania"
    exit 1
fi

# Parse remaining arguments
shift
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --host)
            SCYLLA_HOST="$2"
            shift 2
            ;;
        --port)
            SCYLLA_PORT="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            shift
            ;;
    esac
done

echo "=== Cypress Wipe Region ==="
echo "Region: $REGION_NAME"
echo "ScyllaDB Host: $SCYLLA_HOST"
echo "ScyllaDB Port: $SCYLLA_PORT"
echo

# Find region in import_global.sh to get filename
if [ ! -f "$IMPORT_SCRIPT" ]; then
    echo "Error: Could not find import_global.sh to resolve region names."
    exit 1
fi

# Search for the region line in REGIONS array
# Format: "Name|URL"
# Use -E for extended regex to handle | properly
REGION_LINE=$(grep -E "\"${REGION_NAME}\|" "$IMPORT_SCRIPT" || true)

if [ -z "$REGION_LINE" ]; then
    # Try case-insensitive
    REGION_LINE=$(grep -Ei "\"${REGION_NAME}\|" "$IMPORT_SCRIPT" || true)
fi

if [ -z "$REGION_LINE" ]; then
    echo "Warning: Region '$REGION_NAME' not found in import_global.sh."
    echo "Falling back to using '$REGION_NAME' as base name."
    BASE_NAME="$REGION_NAME"
else
    # Extract URL and filename
    # Line looks like: "Albania|https://.../albania-latest.osm.pbf"
    URL=$(echo "$REGION_LINE" | cut -d'|' -f2 | tr -d '", ')
    FILENAME=$(basename "$URL")
    # Base source file name (e.g. albania-latest)
    BASE_NAME="${FILENAME%.osm.pbf}"
fi

# Files that could represent this region
FILES=(
    "${BASE_NAME}-filtered.osm.pbf"
    "${BASE_NAME}-admins.osm.pbf"
    "${BASE_NAME}.osm.pbf"
)

TOTAL_DELETED=0

for SF in "${FILES[@]}"; do
    echo "Querying IDs for source file: '$SF'..."
    # Query IDs from ScyllaDB
    IDS=$(cqlsh $SCYLLA_HOST $SCYLLA_PORT -e "SELECT id FROM cypress.places_by_source WHERE source_file = '$SF';" | grep -v -E "(^id|^---|^\s*$|\([0-9]+ rows\))" | tr -d '\r ' || true)
    
    if [ -n "$IDS" ]; then
        echo "Deleting documents from cypress.places..."
        for id in $IDS; do
            if [ -n "$id" ]; then
                cqlsh $SCYLLA_HOST $SCYLLA_PORT -e "DELETE FROM cypress.places WHERE id = '$id';" > /dev/null
                TOTAL_DELETED=$((TOTAL_DELETED + 1))
            fi
        done
        
        echo "Clearing partition from cypress.places_by_source..."
        cqlsh $SCYLLA_HOST $SCYLLA_PORT -e "DELETE FROM cypress.places_by_source WHERE source_file = '$SF';" > /dev/null
    fi
done

echo "Successfully deleted $TOTAL_DELETED documents."
echo
echo "Wipe complete."
