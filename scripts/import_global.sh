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
#   --tmp-dir <dir>   Directory for temporary data files (PBFs) (default: ./data)
#
# This script downloads, filters, and imports data for multiple global regions.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="${PROJECT_DIR}/data"

# Region Definitions
# Format: "Name|URL"
REGIONS=(
    # Europe
    "Albania|https://download.geofabrik.de/europe/albania-latest.osm.pbf"
    "Andorra|https://download.geofabrik.de/europe/andorra-latest.osm.pbf"
    "Austria|https://download.geofabrik.de/europe/austria-latest.osm.pbf"
    "Azores|https://download.geofabrik.de/europe/azores-latest.osm.pbf"
    "Belarus|https://download.geofabrik.de/europe/belarus-latest.osm.pbf"
    "Belgium|https://download.geofabrik.de/europe/belgium-latest.osm.pbf"
    "Bosnia-Herzegovina|https://download.geofabrik.de/europe/bosnia-herzegovina-latest.osm.pbf"
    "Bulgaria|https://download.geofabrik.de/europe/bulgaria-latest.osm.pbf"
    "Croatia|https://download.geofabrik.de/europe/croatia-latest.osm.pbf"
    "Cyprus|https://download.geofabrik.de/europe/cyprus-latest.osm.pbf"
    "Czech_Republic|https://download.geofabrik.de/europe/czech-republic-latest.osm.pbf"
    "Denmark|https://download.geofabrik.de/europe/denmark-latest.osm.pbf"
    "Estonia|https://download.geofabrik.de/europe/estonia-latest.osm.pbf"
    "Faroe_Islands|https://download.geofabrik.de/europe/faroe-islands-latest.osm.pbf"
    "Finland|https://download.geofabrik.de/europe/finland-latest.osm.pbf"
    "France|https://download.geofabrik.de/europe/france-latest.osm.pbf"
    "Georgia|https://download.geofabrik.de/europe/georgia-latest.osm.pbf"
    "Germany|https://download.geofabrik.de/europe/germany-latest.osm.pbf"
    "Greece|https://download.geofabrik.de/europe/greece-latest.osm.pbf"
    "Guernsey_and_Jersey|https://download.geofabrik.de/europe/guernsey-jersey-latest.osm.pbf"
    "Hungary|https://download.geofabrik.de/europe/hungary-latest.osm.pbf"
    "Iceland|https://download.geofabrik.de/europe/iceland-latest.osm.pbf"
    "Ireland_and_Northern_Ireland|https://download.geofabrik.de/europe/ireland-and-northern-ireland-latest.osm.pbf"
    "Isle_of_Man|https://download.geofabrik.de/europe/isle-of-man-latest.osm.pbf"
    "Italy|https://download.geofabrik.de/europe/italy-latest.osm.pbf"
    "Kosovo|https://download.geofabrik.de/europe/kosovo-latest.osm.pbf"
    "Latvia|https://download.geofabrik.de/europe/latvia-latest.osm.pbf"
    "Liechtenstein|https://download.geofabrik.de/europe/liechtenstein-latest.osm.pbf"
    "Lithuania|https://download.geofabrik.de/europe/lithuania-latest.osm.pbf"
    "Luxembourg|https://download.geofabrik.de/europe/luxembourg-latest.osm.pbf"
    "Macedonia|https://download.geofabrik.de/europe/macedonia-latest.osm.pbf"
    "Malta|https://download.geofabrik.de/europe/malta-latest.osm.pbf"
    "Moldova|https://download.geofabrik.de/europe/moldova-latest.osm.pbf"
    "Monaco|https://download.geofabrik.de/europe/monaco-latest.osm.pbf"
    "Montenegro|https://download.geofabrik.de/europe/montenegro-latest.osm.pbf"
    "Netherlands|https://download.geofabrik.de/europe/netherlands-latest.osm.pbf"
    "Norway|https://download.geofabrik.de/europe/norway-latest.osm.pbf"
    "Poland|https://download.geofabrik.de/europe/poland-latest.osm.pbf"
    "Portugal|https://download.geofabrik.de/europe/portugal-latest.osm.pbf"
    "Romania|https://download.geofabrik.de/europe/romania-latest.osm.pbf"
    "Russian_Federation|https://download.geofabrik.de/russia-latest.osm.pbf"
    "Serbia|https://download.geofabrik.de/europe/serbia-latest.osm.pbf"
    "Slovakia|https://download.geofabrik.de/europe/slovakia-latest.osm.pbf"
    "Slovenia|https://download.geofabrik.de/europe/slovenia-latest.osm.pbf"
    "Spain|https://download.geofabrik.de/europe/spain-latest.osm.pbf"
    "Sweden|https://download.geofabrik.de/europe/sweden-latest.osm.pbf"
    "Switzerland|https://download.geofabrik.de/europe/switzerland-latest.osm.pbf"
    "Turkey|https://download.geofabrik.de/europe/turkey-latest.osm.pbf"
    "Ukraine|https://download.geofabrik.de/europe/ukraine-latest.osm.pbf"
    "United_Kingdom|https://download.geofabrik.de/europe/united-kingdom-latest.osm.pbf"

    # Asia
    "China|https://download.geofabrik.de/asia/china-latest.osm.pbf"
    "Malaysia_Singapore_Brunei|https://download.geofabrik.de/asia/malaysia-singapore-brunei-latest.osm.pbf"
    "South_Korea|https://download.geofabrik.de/asia/south-korea-latest.osm.pbf"
    "Japan|https://download.geofabrik.de/asia/japan-latest.osm.pbf"
    "Thailand|https://download.geofabrik.de/asia/thailand-latest.osm.pbf"
    "Taiwan|https://download.geofabrik.de/asia/taiwan-latest.osm.pbf"
    "Vietnam|https://download.geofabrik.de/asia/vietnam-latest.osm.pbf"

    # North America
    "United_States|https://download.geofabrik.de/north-america/us-latest.osm.pbf"
    "Canada|https://download.geofabrik.de/north-america/canada-latest.osm.pbf"

    # Others
    #"South_America|https://download.geofabrik.de/south-america-latest.osm.pbf"
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
        --tmp-dir)
            DATA_DIR="$2"
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
echo "Data Directory: $DATA_DIR"
if [ -n "$FRESH_FLAG" ]; then
    echo "Mode: FRESH IMPORT (Index will be recreated)"
else
    echo "Mode: Append/Update"
fi
echo

# Download and extract Importance Data if needed
IMPORTANCE_URL="https://nominatim.org/data/wikimedia-importance.csv.gz"
IMPORTANCE_FILE="${DATA_DIR}/wikimedia-importance.csv.gz"
IMPORTANCE_CSV="${DATA_DIR}/wikimedia-importance.csv"

echo "Checking Importance Data..."
if [ ! -f "$IMPORTANCE_CSV" ]; then
    if [ ! -f "$IMPORTANCE_FILE" ]; then
        echo "Downloading Importance Data..."
        curl -A "Mozilla/5.0 (CypressImport/1.0)" -L -o "$IMPORTANCE_FILE" "$IMPORTANCE_URL"
    fi
    echo "Extracting Importance Data..."
    gunzip -k "$IMPORTANCE_FILE"
else
    echo "Importance Data exists."
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
        curl -A "Mozilla/5.0 (CypressImport/1.0)" -L -o "$RAW_PBF" "$URL"
        # Force re-filter if new download
        rm -f "$FILTERED_PBF"
    else
        echo "Using existing file: $RAW_PBF"
    fi
    
    # 2. Filter
    PBF_TO_IMPORT="$RAW_PBF"
    ADMIN_PBF=""
    
    if [ "$NO_FILTER" = false ]; then
        if command -v osmium &> /dev/null; then
            # General filter (places)
            if [ ! -f "$FILTERED_PBF" ] || [ "$RAW_PBF" -nt "$FILTERED_PBF" ]; then
                echo "Filtering places in $NAME..."
                "$SCRIPT_DIR/filter_osm.sh" "$RAW_PBF" "$FILTERED_PBF"
            else
                echo "Using cached filtered file: $FILTERED_PBF"
            fi
            PBF_TO_IMPORT="$FILTERED_PBF"
            
            # Admin filter
            ADMIN_PBF="${DATA_DIR}/${FILENAME%.osm.pbf}-admins.osm.pbf"
            if [ ! -f "$ADMIN_PBF" ] || [ "$RAW_PBF" -nt "$ADMIN_PBF" ]; then
                echo "Filtering admin boundaries in $NAME..."
                "$SCRIPT_DIR/filter_admins.sh" "$RAW_PBF" "$ADMIN_PBF"
            else
                echo "Using cached admin file: $ADMIN_PBF"
            fi
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
    
    if [ -n "$ADMIN_PBF" ]; then
        cargo run --release --bin ingest -- \
            --file "$PBF_TO_IMPORT" \
            --admin-file "$ADMIN_PBF" \
            --es-url "$ES_URL" \
            --refresh \
            $WIKIDATA \
            --importance-file "$IMPORTANCE_CSV" \
            $CURRENT_FRESH_ARG
    else
        cargo run --release --bin ingest -- \
            --file "$PBF_TO_IMPORT" \
            --es-url "$ES_URL" \
            --refresh \
            $WIKIDATA \
            --importance-file "$IMPORTANCE_CSV" \
            $CURRENT_FRESH_ARG
    fi
        
    echo "Finished $NAME"
    echo
done

echo "=== Global Import Complete ==="
