#!/bin/bash
set -e

# Usage: ./filter_admins.sh input.osm.pbf output.osm.pbf
INPUT_FILE=$1
OUTPUT_FILE=$2

if [ -z "$INPUT_FILE" ] || [ -z "$OUTPUT_FILE" ]; then
    echo "Usage: $0 <input.osm.pbf> <output.osm.pbf>"
    exit 1
fi

echo "Filtering admin boundaries from $INPUT_FILE to $OUTPUT_FILE..."

# config/admin_boundary_expressions.txt must exist relative to where this script is run usually, 
# or we find it relative to script dir.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONFIG_FILE="$SCRIPT_DIR/../config/admin_boundary_expressions.txt"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file not found at $CONFIG_FILE"
    exit 1
fi

# Run osmium tags-filter
# -o: output file
# --overwrite: allow overwriting
# expressions file comes from config
osmium tags-filter "$INPUT_FILE" --expressions="$CONFIG_FILE" -o "$OUTPUT_FILE" --overwrite

echo "Done. Created $OUTPUT_FILE"
