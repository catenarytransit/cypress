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
ES_URL="http://localhost:9200"
INDEX_NAME="places"

# Use ELASTICSEARCH_URL env var if set
if [ -n "$ELASTICSEARCH_URL" ]; then
    ES_URL="$ELASTICSEARCH_URL"
fi

if [ -z "$REGION_NAME" ]; then
    echo "Usage: $0 <region_name> [--url <url>] [--index <name>]"
    echo "Example: $0 Albania"
    exit 1
fi

# Parse remaining arguments
shift
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --url)
            ES_URL="$2"
            shift 2
            ;;
        --index)
            INDEX_NAME="$2"
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
echo "Elasticsearch URL: $ES_URL"
echo "Index: $INDEX_NAME"
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
    echo "Falling back to using '$REGION_NAME' as a wildcard match for source_file."
    # Sanitize REGION_NAME for wildcard
    SOURCE_PATTERN="*${REGION_NAME}*"
else
    # Extract URL and filename
    # Line looks like: "Albania|https://.../albania-latest.osm.pbf"
    URL=$(echo "$REGION_LINE" | cut -d'|' -f2 | tr -d '", ')
    FILENAME=$(basename "$URL")
    # Base source file name (e.g. albania-latest)
    BASE_NAME="${FILENAME%.osm.pbf}"
    # Use BASE_NAME with wildcard to catch -filtered, -admins, etc.
    SOURCE_PATTERN="${BASE_NAME}*"
fi

echo "Deleting documents with source_file matching: '$SOURCE_PATTERN'"

# Perform delete by query
QUERY=$(cat <<EOF
{
  "query": {
    "wildcard": {
      "source_file": {
        "value": "$SOURCE_PATTERN"
      }
    }
  }
}
EOF
)

# Use -s for silent, -S for show error
RESPONSE=$(curl -s -S -X POST "$ES_URL/$INDEX_NAME/_delete_by_query?refresh&wait_for_completion=true" \
    -H 'Content-Type: application/json' \
    -d "$QUERY")

if echo "$RESPONSE" | grep -q '"deleted"'; then
    # Extract deleted count using grep/sed
    DELETED=$(echo "$RESPONSE" | sed -n 's/.*"deleted":\([0-9]*\).*/\1/p')
    echo "Successfully deleted $DELETED documents."
else
    echo "Error or no documents found:"
    echo "$RESPONSE"
fi

echo
echo "Wipe complete."
