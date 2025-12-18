#!/bin/bash
# Filter OSM PBF to reduce memory during import
#
# This script filters an OSM PBF file to only include:
# - Admin boundary relations (for PIP lookups)
# - Named places and POIs (searchable features)
# - Addresses with housenumbers
#
# Prerequisites: osmium-tool (install: apt install osmium-tool)
#
# Usage: ./filter_osm.sh input.osm.pbf [output.osm.pbf]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CONFIG_DIR="${PROJECT_DIR}/config"
FILTER_EXPRESSIONS="${CONFIG_DIR}/filter_expressions.txt"

# Check for osmium
if ! command -v osmium &> /dev/null; then
    echo "ERROR: osmium-tool is not installed"
    echo
    echo "Install with:"
    echo "  Ubuntu/Debian: sudo apt install osmium-tool"
    echo "  macOS: brew install osmium-tool"
    echo "  Arch: sudo pacman -S osmium-tool"
    exit 1
fi

# Check arguments
if [ -z "$1" ]; then
    echo "Usage: $0 input.osm.pbf [output.osm.pbf]"
    echo
    echo "Example:"
    echo "  $0 switzerland-latest.osm.pbf switzerland-filtered.osm.pbf"
    exit 1
fi

INPUT_FILE="$1"
OUTPUT_FILE="${2:-${INPUT_FILE%.osm.pbf}-filtered.osm.pbf}"

if [ ! -f "$INPUT_FILE" ]; then
    echo "ERROR: Input file not found: $INPUT_FILE"
    exit 1
fi

if [ ! -f "$FILTER_EXPRESSIONS" ]; then
    echo "ERROR: Filter expressions file not found: $FILTER_EXPRESSIONS"
    exit 1
fi

# Get file sizes for comparison
INPUT_SIZE=$(du -h "$INPUT_FILE" | cut -f1)
echo "=== Cypress OSM Filter ==="
echo
echo "Input:  $INPUT_FILE ($INPUT_SIZE)"
echo "Output: $OUTPUT_FILE"
echo "Filter: $FILTER_EXPRESSIONS"
echo

# Count expressions
EXPR_COUNT=$(grep -v '^#' "$FILTER_EXPRESSIONS" | grep -v '^$' | wc -l)
echo "Applying $EXPR_COUNT filter expressions..."
echo

# Run osmium tags-filter
# Note: We DON'T use --omit-referenced because:
# - Admin boundary relations need their way members to build polygons
# - We need the nodes that make up those ways
time osmium tags-filter "$INPUT_FILE" \
    --expressions="$FILTER_EXPRESSIONS" \
    --output="$OUTPUT_FILE" \
    --overwrite \
    --progress

echo

# Get output size
OUTPUT_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)

echo "=== Filtering Complete ==="
echo
echo "Input:  $INPUT_SIZE"
echo "Output: $OUTPUT_SIZE"
echo

# Calculate reduction
INPUT_BYTES=$(stat --printf="%s" "$INPUT_FILE" 2>/dev/null || stat -f%z "$INPUT_FILE" 2>/dev/null)
OUTPUT_BYTES=$(stat --printf="%s" "$OUTPUT_FILE" 2>/dev/null || stat -f%z "$OUTPUT_FILE" 2>/dev/null)
if [ -n "$INPUT_BYTES" ] && [ -n "$OUTPUT_BYTES" ] && [ "$INPUT_BYTES" -gt 0 ]; then
    REDUCTION=$((100 - (OUTPUT_BYTES * 100 / INPUT_BYTES)))
    echo "Reduction: ${REDUCTION}%"
fi

echo
echo "Use the filtered file with the ingest pipeline:"
echo "  cargo run --release --bin ingest -- --file $OUTPUT_FILE --create-index"
